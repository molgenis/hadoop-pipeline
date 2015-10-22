package org.molgenis.hadoop.pipeline.application.processes;

/**
 * Describes objects used by the {@link InStreamHandler}.
 */
public interface InContainer
{
	/**
	 * Adds the line to the container.
	 * 
	 * @param line
	 */
	public void add(String line);

	/**
	 * Returns an {@link Object} containing the added data.
	 * 
	 * @return {@link Object}
	 */
	public Object get();

}
