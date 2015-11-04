package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.exceptions.ProcessPipeException;
import org.molgenis.hadoop.pipeline.application.processes.PipeRunner;
import org.molgenis.hadoop.pipeline.application.processes.SamRecordSink;

import htsjdk.samtools.SAMRecord;

/**
 * Hadoop MapReduce Job mapper.
 */
public class HadoopPipelineMapper extends Mapper<NullWritable, BytesWritable, NullWritable, Text>
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
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		retrieveCache(context);
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
					context.write(key, new Text(item.getSAMString().trim()));
				}
				catch (IOException | InterruptedException e)
				{
					throw new ProcessPipeException(e);
				}
			}
		};

		PipeRunner.startPipeline(value.getBytes(), sink,
				new ProcessBuilder(bwaTool, "mem", "-p", "-M", alignmentReferenceFastaFile, "-").start());
	}

	/**
	 * Retrieves the cache files.
	 * 
	 * IMPORTANT: Be sure the exact same array order is used as defined in {@link HadoopPipelineApplication}!
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void retrieveCache(Context context) throws IllegalArgumentException, IOException
	{
		URI[] cacheArchives = context.getCacheArchives();
		URI[] cacheFiles = context.getCacheFiles();

		bwaTool = new String("./" + retrieveFileName(cacheArchives[0]) + "/tools/bwa");
		alignmentReferenceFastaFile = new String("./" + retrieveFileName(cacheFiles[0]));
	}

	/**
	 * Retrieves the file name without the path before it.
	 * 
	 * @param filePath
	 * @return {@link String}
	 */
	private String retrieveFileName(URI filePath)
	{
		String[] pathFragments = filePath.toString().split("/");
		return pathFragments[pathFragments.length - 1];
	}
}
