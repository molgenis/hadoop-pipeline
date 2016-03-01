package org.molgenis.hadoop.pipeline.application.localtmpfiles;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * Interface to be used with {@link HadoopLocalTmpFileWriter}{@code s} subclasses to support writing of the
 * {@link Writable} implementations of the data the {@code writer} can write.
 * 
 * @param <T>
 *            - The {@link Writable} instances that the {@code writer} should be able to add.
 */
interface WritableWriter<T extends Writable>
{
	/**
	 * Add a single {@link Writable} {@code item} to the {@code writer}.
	 * 
	 * @param item
	 *            {@link T}
	 */
	public void addWritable(T item);

	/**
	 * Add multiple {@link Writable} {@code items} to the {@code writer}.
	 * 
	 * @param items
	 *            {@link Iterator}{@code <}{@link T}{@code >}
	 */
	public void addWritables(Iterator<T> items);
}
