package org.molgenis.hadoop.pipeline.application.processes;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;

/**
 * Digests a {@link InputStream} using a {@link Sink}{@code <T>}.
 */
public class PipeOutHandler<T> implements Runnable
{
	/**
	 * The stream that needs to be digested.
	 */
	private final InputStream inputStream;
	/**
	 * The {@code sink<T>} that will digest the {@code inputStream}.
	 */
	private final Sink<T> sink;

	/**
	 * Initiates a new {@code PipeOutHandler}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @param sink
	 *            {@link Sink}{@code <T>}
	 */
	public PipeOutHandler(InputStream inputStream, Sink<T> sink)
	{
		this.inputStream = requireNonNull(inputStream);
		this.sink = requireNonNull(sink);
	}

	/**
	 * Digest the {@link InputStream} using a {@link Sink}{@code <T>}.
	 */
	@Override
	public void run()
	{
		sink.handleInputStream(inputStream);
	}

}
