package org.molgenis.hadoop.pipeline.application.processes;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.molgenis.hadoop.pipeline.application.exceptions.ProcessPipeException;

/**
 * Writes the data stored in a {@code byte[]} to the {@link OutputStream}.
 */
public class PipeInHandler implements Runnable
{
	/**
	 * Stores the stream to write to.
	 */
	private OutputStream outputStream;

	/**
	 * Stores the data to be written to the stream.
	 */
	private byte[] inputData;

	/**
	 * Initiates a new {@link PipeInHandler}.
	 * 
	 * @param outStream
	 * @param inputData
	 */
	PipeInHandler(OutputStream outputStream, byte[] inputData)
	{
		this.outputStream = requireNonNull(outputStream);
		this.inputData = requireNonNull(inputData);
	}

	/**
	 * Writes the {@code inputData} to the {@code outputStream}.
	 */
	@Override
	public void run()
	{
		try
		{
			IOUtils.write(inputData, outputStream);
		}
		catch (IOException e)
		{
			throw new ProcessPipeException(e);
		}
		finally
		{
			IOUtils.closeQuietly(outputStream);
		}
	}
}
