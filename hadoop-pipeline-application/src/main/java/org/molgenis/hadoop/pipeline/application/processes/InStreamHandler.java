package org.molgenis.hadoop.pipeline.application.processes;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Handles the stream written to the process.
 */
public class InStreamHandler extends StreamHandler
{
	/**
	 * Stores the write stream to be used.
	 */
	InputStream processStream;

	/**
	 * Object to which the output stream will be written to.
	 */
	StringContainer stringContainer;

	/**
	 * Initiates a new {@link InStreamHandler} instance that stores the required data.
	 * 
	 * @param outStream
	 * @param inputData
	 */
	InStreamHandler(InputStream processStream, StringContainer stringContainer)
	{
		this.processStream = processStream;
		this.stringContainer = stringContainer;
	}

	/**
	 * Fills the {@link StringContainer} with the process output stream.
	 */
	@Override
	public void run()
	{
		Scanner scanner = new Scanner(processStream, StandardCharsets.UTF_8.name());

		while (scanner.hasNext())
		{
			stringContainer.appendWithNewLine(scanner.nextLine());
		}
		scanner.close();
	}

}
