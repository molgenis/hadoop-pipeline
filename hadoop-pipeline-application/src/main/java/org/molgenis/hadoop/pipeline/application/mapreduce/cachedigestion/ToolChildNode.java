package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

/**
 * Enum containing the fields that are expected within a tools xml file and that can be stored in an instance of
 * {@link Tool}.
 */
enum ToolChildNode
{
	ID, NAME, VERSION;

	/**
	 * Retrieve an enum that belongs to the given {@link String}. Returns {@code null} if no matching {@link ToolFiend}
	 * enum value was found. Ignores any capitalization within the given {@code fieldString}.
	 * 
	 * @param fieldString
	 *            {@link String}
	 * @return {@link ToolField} if match was found, otherwise {@code null}
	 */
	static ToolChildNode getEnum(String fieldString)
	{

		for (ToolChildNode value : ToolChildNode.values())
		{
			if (value.toString().toLowerCase().equals(fieldString.toLowerCase()))
			{
				return value;
			}
		}
		return null;
	}
}
