package org.molgenis.hadoop.pipeline.application;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.molgenis.hadoop.pipeline.application.inputdigestion.CommandLineInputParser;
import org.molgenis.hadoop.pipeline.application.mapreduce.HadoopPipelineMapper;

import mr.wholeFile.WholeFileInputFormat;

/**
 * Main application class.
 */
public class HadoopPipelineApplication extends Configured implements Tool
{
	/**
	 * Default main class that calls {@link ToolRunner} to execute Hadoop MapReduce.
	 * 
	 * @param args
	 */
	public static void main(String[] args)
	{
		try
		{
			ToolRunner.run(new Configuration(), new HadoopPipelineApplication(), args);
		}
		catch (Exception ex)
		{
			System.err.println("An error occured!");
			System.err.println(ex.getMessage());
			System.err.println(""); // Empty line before stack trace.
			ex.printStackTrace();
		}
	}

	/**
	 * Configures and executes Hadoop MapReduce job. IMPORTANT: Be sure that when retrieving items from the cache arrays
	 * in the mapper/reducer, to use the positions as defined within this function!
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public int run(String[] args) throws IOException, ParseException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = getConf();

		// YARN fix for: java.lang.OutOfMemoryError: Java heap space
		conf.set("mapreduce.map.memory.mb", "2048");
		conf.set("mapreduce.map.java.opts", "-Xmx1024m");

		FileSystem fileSys = FileSystem.get(conf);

		// Digests command line arguments.
		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		// Only execute MapReduce job if all required paremeters are present and correct.
		if (parser.isContinueApplication())
		{
			Job job = Job.getInstance(conf);
			job.setJarByClass(HadoopPipelineApplication.class);
			job.setJobName("HadoopPipelineApplication");

			// IMPORTANT: input order defines position in array for retrieval in mapper/reducer!!!
			job.addCacheArchive(parser.getToolsArchiveLocation().toUri()); // archive [0]

			// IMPORTANT: input order defines position in array for retrieval in mapper/reducer!!!
			job.addCacheFile(parser.getAlignmentReferenceFastaFile().toUri()); // file [0]
			job.addCacheFile(parser.getAlignmentReferenceFastaAmbFile().toUri());
			job.addCacheFile(parser.getAlignmentReferenceFastaAnnFile().toUri());
			job.addCacheFile(parser.getAlignmentReferenceFastaBwtFile().toUri());
			job.addCacheFile(parser.getAlignmentReferenceFastaFaiFile().toUri());
			job.addCacheFile(parser.getAlignmentReferenceFastaPacFile().toUri());
			job.addCacheFile(parser.getAlignmentReferenceFastaSaFile().toUri());

			// Sets input/output paths.
			FileInputFormat.addInputPath(job, parser.getInputDir());
			FileOutputFormat.setOutputPath(job, parser.getOutputDir());

			// Sets and configures Mapper/Reducer.
			job.setMapperClass(HadoopPipelineMapper.class);
			job.setNumReduceTasks(0);

			// Sets input/output formats.
			job.setInputFormatClass(WholeFileInputFormat.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);

		}
		return 0;
	}

}
