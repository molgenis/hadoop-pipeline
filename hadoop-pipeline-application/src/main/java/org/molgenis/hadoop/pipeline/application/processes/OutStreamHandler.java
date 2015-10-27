package org.molgenis.hadoop.pipeline.application.processes;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Handles the stream writing to the process.
 */
public class OutStreamHandler extends StreamHandler
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(OutStreamHandler.class);

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
			logger.error("Error occured when trying to write to the OutputStream.");
		}
		finally
		{
			IOUtils.closeQuietly(outStream);
		}
	}
}
