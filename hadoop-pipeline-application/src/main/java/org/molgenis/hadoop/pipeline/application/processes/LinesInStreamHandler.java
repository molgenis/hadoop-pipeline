package org.molgenis.hadoop.pipeline.application.processes;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Handles a stream retrieved from the process as generic lines.
 */
public class LinesInStreamHandler extends InStreamHandler
{
	/**
	 * Replaces the superclass' default {@link InContainer} to be a {@link LinesInContainer} instead.
	 */
	private LinesInContainer inContainer;

	/**
	 * Initiates a new {@link LinesInStreamHandler} instance.
	 * 
	 * @param processStream
	 * @param inContainer
	 */
	LinesInStreamHandler(InputStream processStream, LinesInContainer inContainer)
	{
		super(processStream);
		this.inContainer = requireNonNull(inContainer);
	}

	/**
	 * Digest the lines from a {@link InputStream}.
	 */
	@Override
	public void run()
	{
		try (Scanner scanner = new Scanner(getProcessStream(), StandardCharsets.UTF_8.name()))
		{
			while (scanner.hasNext())
			{
				inContainer.add(scanner.nextLine());
			}
		}
	}
}
