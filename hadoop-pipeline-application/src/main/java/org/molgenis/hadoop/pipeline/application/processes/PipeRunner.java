package org.molgenis.hadoop.pipeline.application.processes;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.molgenis.hadoop.pipeline.application.exceptions.ProcessPipeException;

/**
 * Class for running a pipe of one or more {@link Process}{@code es}.
 */
public class PipeRunner implements Runnable
{
	/**
	 * Stream containing data from a process.
	 */
	private final InputStream inputStream;

	/**
	 * Stream to write to a process.
	 */
	private final OutputStream outputStream;

	/**
	 * Initiate a new pipe between two processes.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @param outputStream
	 *            {@link OutputStream}
	 */
	public PipeRunner(InputStream inputStream, OutputStream outputStream)
	{
		this.inputStream = requireNonNull(inputStream);
		this.outputStream = requireNonNull(outputStream);
	}

	/**
	 * Runs a sequence of multiple {@link Process}{@code es}, piping the {@link InputStream} of one {@link Process} to
	 * the {@link OutputStream} of the next {@link Process}. The {@code inputData} gives the input in {@code byte[]} for
	 * the first {@link Process}{@code '} {@link OutputStream}. The {@link Sink} is used to digest the
	 * {@link InputStream} of the last {@link Process}.
	 * 
	 * @param inputData
	 *            {@code byte[]}
	 * @param sink
	 *            {@link Sink}
	 * @param processes
	 *            1 or more {@link Process}
	 */
	public static <T> void startPipeline(byte[] inputData, Sink<T> sink, Process... processes)
	{
		// Defines the first process.
		Process process1 = processes[0];

		// Initiates a stream to write the inputData to the first process.
		new Thread(new PipeInHandler(process1.getOutputStream(), inputData)).start();

		// If there are multiple processes in the pipeline, goes through these as well.
		if (processes.length > 1)
		{
			Process process2;

			// Pipes the output of one process as input to the following process.
			for (int i = 1; i < processes.length; i++)
			{
				process2 = processes[i];
				new Thread(new PipeRunner(process1.getInputStream(), process2.getOutputStream())).start();
				process1 = process2;
			}
		}

		// Digests the input of the last process in the pipeline.
		new PipeOutHandler<T>(process1.getInputStream(), sink).run();

		// Waits for the last process to finish before continuing.
		try
		{
			process1.waitFor();
		}
		catch (InterruptedException e)
		{
			throw new ProcessPipeException(e);
		}
	}

	/**
	 * Transfers the {@link InputStream} of one {@link Process} to the {@link OutputStream} of the next {@link Process}.
	 */
	@Override
	public void run()
	{
		try
		{
			IOUtils.copy(inputStream, outputStream);
		}
		catch (IOException e)
		{
			throw new ProcessPipeException(e);
		}
		finally
		{
			IOUtils.closeQuietly(inputStream);
			IOUtils.closeQuietly(outputStream);
		}
	}

}
