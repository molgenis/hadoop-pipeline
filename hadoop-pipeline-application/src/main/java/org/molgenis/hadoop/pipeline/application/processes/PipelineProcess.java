package org.molgenis.hadoop.pipeline.application.processes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * Generic command line process using an executable, in stream, out stream and error stream.
 */
public abstract class PipelineProcess implements Callable<Object>
{
	/**
	 * Contains the command line to be executed.
	 */
	protected ArrayList<String> commandLineArguments;
	/**
	 * Contains the data to be written to the input stream of the process.
	 */
	protected byte[] processInputData;

	/**
	 * The container type which will be used to store the process output stream in.
	 */
	protected InContainerFactory inContainerFactory;

	public byte[] getProcessInputData()
	{
		return processInputData;
	}

	public void setProcessInputData(byte[] inputData)
	{
		this.processInputData = inputData;
	}

	public InContainerFactory getInContainerType()
	{
		return inContainerFactory;
	}

	/**
	 * Executes the process.
	 */
	public Object call() throws IOException, InterruptedException
	{
		ProcessBuilder builder = new ProcessBuilder(commandLineArguments);

		final Process process = builder.start();

		// Create and start the writing to the process input stream.
		new OutStreamHandler(process.getOutputStream(), processInputData).start();

		// Create containers to store the error/output stream results into.
		StringInContainer errContainer = new StringInContainer();
		InContainer inContainer = inContainerFactory.getContainer();

		// Create and start the reading of the error/output stream.
		new InStreamHandler(process.getErrorStream(), errContainer).start();
		new InStreamHandler(process.getInputStream(), inContainer).start();

		// Makes the application wait for the process to finish before continuing.
		process.waitFor();

		return inContainer.get();
	}
}
