package org.molgenis.hadoop.pipeline.application.processes;

import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract class describing required methods for a {@link PipeRunner}{@code 's} last {@link Process}{@code '}
 * {@link InputStream} digestion.
 * 
 * @param <T>
 */
public abstract class Sink<T>
{
	/**
	 * Digests an {@link InputStream}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 */
	public abstract void handleInputStream(InputStream inputStream);

	/**
	 * Digests a single {@code <T>item} from the {@link InputStream} digested by {@link #handleInputStream(InputStream)}
	 * . This is done by calling {@link #digestStreamItem(Object)} from within {@link #handleInputStream(InputStream)}
	 * for each {@code <T>item} present in the {@link InputStream}.
	 * 
	 * @param item
	 *            {@code <T>}
	 */
	protected abstract void digestStreamItem(T item) throws IOException;
}
