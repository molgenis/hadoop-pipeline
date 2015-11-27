package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.exceptions.SinkException;
import org.molgenis.hadoop.pipeline.application.exceptions.SinkIOException;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.processes.PipeRunner;
import org.molgenis.hadoop.pipeline.application.processes.SamRecordSink;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;

/**
 * Hadoop MapReduce Job mapper.
 */
public class HadoopPipelineMapper extends Mapper<NullWritable, BytesWritable, Text, SAMRecordWritable>
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPipelineMapper.class);

	/**
	 * BwaTool executable location.
	 */
	private String bwaTool;

	/**
	 * Alignment reference fasta file location (with index file having the same prefix and being in the same directory.
	 */
	private String alignmentReferenceFastaFile;

	/**
	 * Stores the read group line that should be added to the bwa alignment, if present.
	 */
	private String readGroupLine;

	/**
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		digestCache(context);
	}

	/**
	 * Function run on individual chunks of the data.
	 */
	@Override
	public void map(final NullWritable key, BytesWritable value, final Context context)
			throws IOException, InterruptedException
	{
		logger.info("running mapper");

		SamRecordSink sink = new SamRecordSink()
		{
			@Override
			public void digestStreamItem(SAMRecord item)
			{
				try
				{
					SAMRecordWritable samWritable = new SAMRecordWritable();
					samWritable.set(item);
					context.write(new Text("key"), samWritable);
				}
				catch (IOException e)
				{
					throw new SinkIOException(e);
				}
				catch (InterruptedException e)
				{
					throw new SinkException(e);
				}
			}
		};

		// Executes bwa alignment. If a read group line is available, it is used as well.
		if (readGroupLine != null)
		{
			logger.debug("Executing pipeline with readGroupLine: " + readGroupLine);
			PipeRunner.startPipeline(value.getBytes(), sink, new ProcessBuilder(bwaTool, "mem", "-p", "-M", "-R",
					readGroupLine, alignmentReferenceFastaFile, "-").start());
		}
		else
		{
			logger.debug("Executing pipeline without readGroupLine.");
			PipeRunner.startPipeline(value.getBytes(), sink,
					new ProcessBuilder(bwaTool, "mem", "-p", "-M", alignmentReferenceFastaFile, "-").start());
		}
	}

	/**
	 * Digests the cache files that are needed into the required formats.
	 * 
	 * IMPORTANT: Be sure the exact same array order is used as defined in {@link HadoopPipelineApplication}!
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void digestCache(Context context) throws IllegalArgumentException, IOException
	{
		bwaTool = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheArchives()[0])) + "/tools/bwa";
		alignmentReferenceFastaFile = HdfsFileMetaDataHandler.retrieveFileName(context.getCacheFiles()[0]);

		// Reads the readgroupline and adds an extra backslash for correct interpretation by the ProcessBuilder. When
		// formatted wrongly, can result in an empty Job output while still saying the Job finished successfully. This
		// should however not be possible as the readgroupline format is checked during the input digestion.
		readGroupLine = context.getConfiguration().get("input_readgroupline");
		if (readGroupLine != null) readGroupLine.replace("\\t", "\\\\t");
	}

}
