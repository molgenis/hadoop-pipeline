package org.molgenis.hadoop.pipeline.application.processes.legacy;

/**
 * Describes the storage objects used by {@link InStreamHandler}{@code s}.
 */
public interface InContainer
{
	/**
	 * Adds an item to the container. Returns {@code true} if add was successful, otherwise returns {@code false}.
	 * 
	 * @param line
	 * @return {@code boolean}
	 */
	public boolean add(Object item);

	/**
	 * Returns an {@link Iterable} containing the individual pieces of data that were added.
	 * 
	 * @return {@link Iterable}
	 */
	public Iterable<?> get();

	/**
	 * Empties the container and sets all preset variables back to their defaults.
	 */
	public void clear();
}
