package org.molgenis.hadoop.pipeline.application;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.formats.BamOutputFormat;
import org.molgenis.hadoop.pipeline.application.inputdigestion.CommandLineInputParser;
import org.molgenis.hadoop.pipeline.application.mapreduce.HadoopPipelineMapper;
import org.molgenis.hadoop.pipeline.application.mapreduce.HadoopPipelineReducer;
import org.molgenis.hadoop.pipeline.application.partitioners.BedFeatureSamRecordGroupingComparator;
import org.molgenis.hadoop.pipeline.application.partitioners.BedFeatureSamRecordPartitioner;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import mr.wholeFile.WholeFileInputFormat;

/**
 * Main application class.
 */
public class HadoopPipelineApplication extends Configured implements Tool
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPipelineApplication.class);

	/**
	 * Default main class that calls {@link ToolRunner} to execute Hadoop MapReduce.
	 * 
	 * @param args
	 *            {@link String}{@code []} user input.
	 */
	public static void main(String[] args)
	{
		try
		{
			// Digests Hadoop's Generic Options.
			GenericOptionsParser genericOptionsParser = new GenericOptionsParser(args);

			// Run the Hadoop application.
			try
			{
				ToolRunner.run(genericOptionsParser.getConfiguration(), new HadoopPipelineApplication(),
						genericOptionsParser.getRemainingArgs());
			}
			catch (Exception e) // Catches all internal errors.
			{
				logger.error(e.getMessage());
				logger.debug(ExceptionUtils.getFullStackTrace(e));
			}
		}
		catch (IOException e) // Catches errors caused by Hadoop's GenericOptionsParser.
		{
			logger.error(e.getMessage());
			logger.debug(ExceptionUtils.getFullStackTrace(e));
		}

	}

	/**
	 * Configures and executes Hadoop MapReduce job. IMPORTANT: Be sure that when retrieving items from the cache arrays
	 * in the mapper/reducer, to use the positions as defined within this function!
	 * 
	 * @param args
	 *            {@link String}{@code []} (part of the) user input.
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	@Override
	public int run(String[] args) throws IOException, ParseException, ClassNotFoundException, InterruptedException
	{
		// Writes Configuration properties to logger.debug that can (and have) cause(d) out of memory/timeout errors or
		// other problems.
		logger.debug("mapreduce.map.memory.mb: " + getConf().get("mapreduce.map.memory.mb"));
		logger.debug("mapreduce.map.java.opts: " + getConf().get("mapreduce.map.java.opts"));
		logger.debug("mapreduce.task.timeout: " + getConf().get("mapreduce.task.timeout"));
		logger.debug("mapreduce.input.fileinputformat.input.dir.recursive: "
				+ getConf().get("mapreduce.input.fileinputformat.input.dir.recursive"));

		FileSystem fileSys = FileSystem.get(getConf());

		// Digests application-specific command line arguments. Throws an Exception if the input arguments are invalid.
		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		// Generates the job and basic settings.
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HadoopPipelineApplication.class);
		job.setJobName("HadoopPipelineApplication");

		// Adds needed files to the distributed cache.
		DistributedCacheHandler cacheHandler = new DistributedCacheHandler(job);
		cacheHandler.addCacheToJob(parser);

		// Sets input/output paths.
		for (Path inputPath : parser.getInputDirs())
		{
			FileInputFormat.addInputPath(job, inputPath);
		}
		FileOutputFormat.setOutputPath(job, parser.getOutputDir());

		// Sets custom partitioner & grouping comparator so it only uses the natural key.
		// Sort comparator uses default behavior, so uses compareTo of Writable (composite key).
		job.setPartitionerClass(BedFeatureSamRecordPartitioner.class);
		job.setGroupingComparatorClass(BedFeatureSamRecordGroupingComparator.class);

		// Sets Mapper/Reducer.
		job.setMapperClass(HadoopPipelineMapper.class);
		job.setReducerClass(HadoopPipelineReducer.class);

		// Sets input/output formats.
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(BamOutputFormat.class);

		// Sets Mapper/Reducer output key/value.
		job.setMapOutputKeyClass(BedFeatureSamRecordStartWritable.class);
		job.setMapOutputValueClass(SAMRecordWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(SAMRecordWritable.class);

		// Returns 0 if job completed successfully. If not, returns 1.
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
