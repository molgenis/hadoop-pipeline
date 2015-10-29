package org.molgenis.hadoop.pipeline.application.processes;

/**
 * Describes the storage objects used by {@link InStreamHandler}{@code s}.
 */
public interface InContainer
{
	/**
	 * Adds an item to the container.
	 * 
	 * @param line
	 */
	public void add(Object item);

	/**
	 * Returns an {@link Iterable} containing the individual pieces of data that were added.
	 * 
	 * @return {@link Iterable}
	 */
	public Iterable<?> get();

	/**
	 * Returns whether if during the addition of items a {@link ClassCastException} occurred.
	 * 
	 * @return {@code boolean}
	 */
	public boolean isClassCastExceptionOccured();

	/**
	 * Empties the container and sets all preset variables back to their defaults.
	 */
	public void clear();
}
