package org.molgenis.hadoop.pipeline.application.processes;

/**
 * Class containing generic code for streaming input/output using {@link Runnable}.
 */
public abstract class StreamHandler implements Runnable
{
	/**
	 * A new thread to execute the stream with.
	 */
	private final Thread thread = new Thread(this);

	/**
	 * Initiates the stream writing (by calling {@code run()}).
	 */
	public void start()
	{
		thread.start();
	}
}
