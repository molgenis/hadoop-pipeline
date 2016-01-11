package org.molgenis.hadoop.pipeline.application.cachedigestion;

/**
 * Enum containing the fields that are expected within a tools xml file.
 */
enum ToolChildNode
{
	ID, NAME, VERSION;

	/**
	 * Retrieve an enum that belongs to the given {@link String}. Returns {@code null} if no matching
	 * {@link ToolChildNode} enum value was found. Ignores any capitalization within the given {@code fieldString}.
	 * 
	 * @param fieldString
	 *            {@link String}
	 * @return {@link ToolChildNode} if match was found, otherwise {@code null}.
	 */
	static ToolChildNode getEnum(String fieldString)
	{
		for (ToolChildNode value : ToolChildNode.values())
		{
			if (value.toString().equals(fieldString.toUpperCase()))
			{
				return value;
			}
		}
		return null;
	}
}
