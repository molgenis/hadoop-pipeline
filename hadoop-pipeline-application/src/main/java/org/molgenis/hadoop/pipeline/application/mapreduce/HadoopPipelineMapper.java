package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.processes.BwaAligner;

/**
 * Hadoop MapReduce Job mapper.
 */
public class HadoopPipelineMapper extends Mapper<NullWritable, BytesWritable, NullWritable, Text> // SAMRecordWritable
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPipelineMapper.class);

	/**
	 * Job context.
	 */
	Context context;

	/**
	 * BwaTool executable location.
	 */
	String bwaTool;
	/**
	 * Alignment reference fasta file location (with index file having the same prefix and being in the same directory.
	 */
	String alignmentReferenceFastaFile;

	/**
	 * BwaAligner instance to execute BWA alignment with.
	 */
	BwaAligner bwaAligner;

	/**
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		this.context = context;
		retrieveCache();
		bwaAligner = new BwaAligner(bwaTool, alignmentReferenceFastaFile);
	}

	/**
	 * Function run on individual chunks of the data.
	 */
	@Override
	public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException
	{
		logger.info("running mapper");
		bwaAligner.setProcessInputData(value.getBytes());
		String results = bwaAligner.call();

		context.write(key, new Text(results));
	}

	/**
	 * Retrieves the cache files.
	 * 
	 * IMPORTANT: Be sure the exact same array order is used as defined in {@link HadoopPipelineApplication}!
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void retrieveCache() throws IllegalArgumentException, IOException
	{
		// URI[] cacheArchives = context.getCacheArchives();
		URI[] cacheFiles = context.getCacheFiles();

		bwaTool = new String("./tools.tar.gz/tools/bwa");

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
