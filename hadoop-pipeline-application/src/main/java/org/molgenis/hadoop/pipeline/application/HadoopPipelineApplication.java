package org.molgenis.hadoop.pipeline.application;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.molgenis.hadoop.pipeline.application.inputdigestion.CommandLineInputParser;

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
	 * Configures and executes Hadoop MapReduce job.
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
		FileSystem fileSys = FileSystem.get(conf);

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);
		if (parser.isContinueApplication())
		{
			// TODO Hadoop MapReduce job
		}
		return 0;
	}

}
