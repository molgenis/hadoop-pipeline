package org.molgenis.hadoop.pipeline.application.cachedigestion;

/**
 * Represents a required field of a {@link Sample}.
 */
public enum SamplesInfoFileField
{
	EXTERNALSAMPLEID
	{
		@Override
		public String getName()
		{
			return "externalSampleID";
		}
	},
	SEQUENCER, SEQUENCINGSTARTDATE
	{
		@Override
		public String getName()
		{
			return "sequencingStartDate";
		}
	},
	RUN, FLOWCELL, LANE;

	/**
	 * Returns the name of the enum using the capitalization as expected to be used in the sample sheet files.
	 * 
	 * @return {@link String}
	 */
	public String getName()
	{
		return this.toString().toLowerCase();
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
