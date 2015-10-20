package org.molgenis.hadoop.pipeline.application.processes;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

/**
 * Handles the stream writing to the process.
 */
public class OutStreamHandler extends StreamHandler
{
	/**
	 * Stores the stream to write to.
	 */
	private OutputStream outStream;
	/**
	 * Stores the data to be written to the stream.
	 */
	private byte[] inputData;

	/**
	 * Initiates a new {@link OutStreamHandler} instance that stores the required data.
	 * 
	 * @param outStream
	 * @param inputData
	 */
	OutStreamHandler(OutputStream outStream, byte[] inputData)
	{
		this.outStream = outStream;
		this.inputData = inputData;
	}

	/**
	 * Writes the data to the process input stream.
	 */
	@Override
	public void run()
	{
		try
		{
			IOUtils.write(inputData, outStream);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			IOUtils.closeQuietly(outStream);
		}
	}
}
