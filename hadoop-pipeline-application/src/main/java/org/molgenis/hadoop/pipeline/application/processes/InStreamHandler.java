package org.molgenis.hadoop.pipeline.application.processes;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Handles the streams retrieved from the process.
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
	InContainer inContainer;

	/**
	 * Initiates a new {@link InStreamHandler} instance that stores the required data.
	 * 
	 * @param outStream
	 * @param inContainer
	 */
	InStreamHandler(InputStream processStream, InContainer inContainer)
	{
		this.processStream = processStream;
		this.inContainer = inContainer;
	}

	/**
	 * Fills the {@link StringInContainer} with the process output stream.
	 */
	@Override
	public void run()
	{
		Scanner scanner = new Scanner(processStream, StandardCharsets.UTF_8.name());

		while (scanner.hasNext())
		{
			inContainer.add(scanner.nextLine());
		}
		scanner.close();
	}

}
