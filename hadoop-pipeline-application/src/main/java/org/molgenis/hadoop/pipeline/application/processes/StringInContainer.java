package org.molgenis.hadoop.pipeline.application.processes;

/**
 * Object to store/write/read a {@String} to/from.
 */
public class StringInContainer implements InContainer
{
	/**
	 * Stores the data that is written to the {@link StringInContainer}.
	 */
	private StringBuilder sb = new StringBuilder();

	@Override
	public void add(String line)
	{
		sb.append(line);
	}

	@Override
	public String get()
	{
		return toString();
	}

	@Override
	public String toString()
	{
		return sb.toString();
	}
}
