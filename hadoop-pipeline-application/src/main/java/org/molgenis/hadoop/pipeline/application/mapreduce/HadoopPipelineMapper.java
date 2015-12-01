package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.SamRecordSink;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.MapReduceBedFormatFileReader;
import org.molgenis.hadoop.pipeline.application.processes.PipeRunner;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

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
	 * Allows retrieval of the groups to which a specific {@link SAMRecord} belongs to (based upon the area the
	 * {@link SAMRecord} was aligned to on the reference data compared to a BED file stored start/end regions).
	 */
	private SamRecordGroupsRetriever groupsRetriever;

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
			public void digestStreamItem(SAMRecord item) throws IOException
			{
				try
				{
					// Creates value.
					SAMRecordWritable samWritable = new SAMRecordWritable();
					samWritable.set(item);

					// Retrieves all keys for the value.
					ArrayList<BEDFeature> groups = groupsRetriever.retrieveGroupsWithinRange(item);

					// Writes a key-value pair for each key found that matched with the SAMRecord alignment position.
					for (BEDFeature key : groups)
					{
						String keyName = new String(key.getContig() + "-" + key.getStart() + "-" + key.getEnd());
						context.write(new Text(keyName), samWritable);
					}
				}
				catch (InterruptedException e)
				{
					throw new RuntimeException(e);
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

		// Retrieves the groups stored in the bed-file which can be used for SAMRecord grouping.
		String bedFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[8]));
		ArrayList<BEDFeature> possibleGroups = new MapReduceBedFormatFileReader(
				FileSystem.get(context.getConfiguration())).read(bedFile);
		groupsRetriever = new SamRecordGroupsRetriever(possibleGroups);

		// Reads the readgroupline and adds an extra backslash for correct interpretation by the ProcessBuilder. When
		// formatted wrongly, can result in an empty Job output while still saying the Job finished successfully. This
		// should however not be possible as the readgroupline format is checked during the input digestion.
		readGroupLine = context.getConfiguration().get("input_readgroupline");
		if (readGroupLine != null) readGroupLine.replace("\\t", "\\\\t");
	}

}
