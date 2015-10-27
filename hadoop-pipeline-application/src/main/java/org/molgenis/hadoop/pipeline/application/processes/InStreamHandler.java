package org.molgenis.hadoop.pipeline.application.processes;

import java.io.InputStream;

/**
 * Handles the streams retrieved from the process.
 */
public abstract class InStreamHandler extends StreamHandler
{
	/**
	 * Stores the write stream to be used.
	 */
	protected InputStream processStream;

	/**
	 * Object to which the output stream will be written to.
	 */
	protected InContainer inContainer;

	/**
	 * Initiates a new {@link InStreamHandler} instance.
	 * 
	 * @param processStream
	 * @param inContainer
	 */
	InStreamHandler(InputStream processStream, InContainer inContainer)
	{
		this.processStream = processStream;
		this.inContainer = inContainer;
	}

	/**
	 * Initiates a new {@link InStreamHandler} instance (subclass still needs to assign an {@link InContainer}).
	 * 
	 * @param processStream
	 */
	InStreamHandler(InputStream processStream)
	{
		this.processStream = processStream;
	}
}
