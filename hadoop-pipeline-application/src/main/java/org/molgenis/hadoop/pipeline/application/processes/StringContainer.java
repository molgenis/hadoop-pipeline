package org.molgenis.hadoop.pipeline.application.processes;

/**
 * Object to store/write/read a {@String} to/from.
 */
public class StringContainer
{
	/**
	 * Stores the data that is written to the {@link StringContainer}.
	 */
	private StringBuilder sb = new StringBuilder();

	/**
	 * Appends the line.
	 * 
	 * @param line
	 */
	public void append(String line)
	{
		sb.append(line);
	}

	/**
	 * Appends the line with a line seperator at the end.
	 * 
	 * @param line
	 */
	public void appendWithNewLine(String line)
	{
		sb.append(line + System.lineSeparator());
	}

	@Override
	public String toString()
	{
		return sb.toString();
	}
}
