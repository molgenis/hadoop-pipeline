package org.molgenis.hadoop.pipeline.application.cachedigestion;

/**
 * Represents a required field of a {@link Sample}.
 */
public enum SamplesInfoFileField
{
	EXTERNALSAMPLEID("externalSampleID"), SEQUENCER("sequencer"), SEQUENCINGSTARTDATE("sequencingStartDate"), RUN(
			"run"), FLOWCELL("flowcell"), LANE("lane");

	/**
	 * The name of a samples info field using capitalization.
	 */
	private String name;

	public String getName()
	{
		return name;
	}

	private SamplesInfoFileField(String name)
	{
		this.name = name;
	}

	/**
	 * Retrieve an enum that belongs to the given {@link String}. Returns {@code null} if no matching
	 * {@link SamplesInfoFileField} enum value was found. Ignores any capitalization within the given
	 * {@code fieldString}.
	 * 
	 * @param fieldString
	 *            {@link String}
	 * @return {@link SamplesInfoFileField} if match was found, otherwise {@code null}.
	 */
	static SamplesInfoFileField getEnum(String fieldString)
	{
		for (SamplesInfoFileField value : SamplesInfoFileField.values())
		{
			if (value.toString().equals(fieldString.toUpperCase()))
			{
				return value;
			}
		}
		return null;
	}

	/**
	 * Compares the name of this {@link Enum} (excluding the {@link Enum} class name) to a {@link String}. Ignores
	 * capitalization during the comparison!
	 * 
	 * @param fieldName
	 *            {@link String}
	 * @return {@code true} if the {@link String} equals the name of the {@link Enum} (ignoring the {@link Enum} class
	 *         name), otherwise false.
	 */
	public boolean nameEquals(String fieldName)
	{
		return this.toString().equals(fieldName.toUpperCase());
	}
}
