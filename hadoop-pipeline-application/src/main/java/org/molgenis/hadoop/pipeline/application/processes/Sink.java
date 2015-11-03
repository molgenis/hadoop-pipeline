package org.molgenis.hadoop.pipeline.application.processes;

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
	 * Digests a single {@code <T>item} from the {@link InputStream} digested by {@code handleInputStream(inputStream)}.
	 * This is done by calling {@code digestStreamItem(item)} from within {@code handleInputStream(inputStream)} for
	 * each {@code <T>item} present in the {@link InputStream}.
	 * 
	 * @param item
	 *            {@code <T>}
	 */
	protected abstract void digestStreamItem(T item);
}
