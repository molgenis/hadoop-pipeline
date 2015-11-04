package org.molgenis.hadoop.pipeline.application.processes.legacy;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;

/**
 * Handles the streams retrieved from the process.
 */
public abstract class InStreamHandler extends StreamHandler
{
	/**
	 * Stores the write stream to be used.
	 */
	private InputStream processStream;

	protected InputStream getProcessStream()
	{
		return processStream;
	}

	/**
	 * Initiates a new {@link InStreamHandler} instance (subclass still needs to assign an {@link InContainer}).
	 * 
	 * @param processStream
	 */
	InStreamHandler(InputStream processStream)
	{
		this.processStream = requireNonNull(processStream);
	}
}
