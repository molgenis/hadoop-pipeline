package org.molgenis.hadoop.pipeline.application.processes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * Generic command line process using an executable, in stream, out stream and error stream.
 */
public abstract class PipelineProcess implements Callable<String>
{
	/**
	 * Contains the command line to be executed.
	 */
	protected ArrayList<String> commandLineArguments;
	/**
	 * Contains the data to be written to the input stream of the process.
	 */
	protected byte[] inputData;

	public byte[] getInputData()
	{
		return inputData;
	}

	public void setInputData(byte[] inputData)
	{
		this.inputData = inputData;
	}

	/**
	 * Executes the process.
	 */
	public String call() throws IOException, InterruptedException
	{
		ProcessBuilder builder = new ProcessBuilder(commandLineArguments);

		final Process process = builder.start();

		// Create and start the writing to the process input stream.
		new OutStreamHandler(process.getOutputStream(), inputData).start();

		// Create containers to store the error/output stream results into.
		StringContainer errContainer = new StringContainer();
		StringContainer outContainer = new StringContainer();

		// Create and start the reading of the error/output stream.
		new InStreamHandler(process.getErrorStream(), errContainer).start();
		new InStreamHandler(process.getInputStream(), outContainer).start();

		// Makes the application wait for the process to finish before continuing.
		process.waitFor();

		return outContainer.toString();
	}
}
