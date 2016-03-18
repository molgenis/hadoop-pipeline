package org.molgenis.hadoop.pipeline.application.sequences;

import java.util.List;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import htsjdk.samtools.SAMRecord;

/**
 * The type of an {@link AlignedRead}.
 */
public enum AlignedReadType
{
	/**
	 * The read did not map.
	 */
	UNMAPPED,

	/**
	 * The read mapped once.
	 */
	MAPPED,

	/**
	 * The read mapped and secondary/supplementary read(s) as well.
	 */
	MULTIMAPPED,

	/**
	 * Only a secondary/supplementary read(s) aligned, but not the primary read.
	 */
	MULTIMAPPED_SUPPLEMENTARY_ONLY,

	/**
	 * The data is deemed invalid.
	 */
	INVALID;

	/**
	 * Determines the {@link AlignedReadType} of a {@link List}{@code <}{@link SAMRecord}{@code >}. If the
	 * {@link SAMRecord} {@link List} does not adhere to the requirements, returns {@link #INVALID} as type. Otherwise,
	 * returns one of the other possible types.
	 * 
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecord}{@code >}
	 * @return {@link AlignedReadType}
	 */
	static AlignedReadType determineType(List<SAMRecord> records)
	{
		// When list is empty, type is INVALID.
		if (records.size() == 0) return INVALID;

		// Retrieves primary record and also checks whether there is more than 1 primary record.
		SAMRecord primaryRecord = null;
		for (SAMRecord record : records)
		{
			if (!record.isSecondaryOrSupplementary())
			{
				// If there is more than 1 primary record, returns INVALID.
				if (primaryRecord != null) return INVALID;

				primaryRecord = record;
			}
		}

		// If no primary record is found, returns invalid.
		if (primaryRecord == null) return INVALID;

		// If there is only 1 record, it can either be mapped or unmapped.
		if (records.size() == 1)
		{
			return primaryRecord.getReadUnmappedFlag() ? UNMAPPED : MAPPED;
		}
		// If there are multiple records, primary and supplementary reads can be mapped or only the latter.
		else
		{
			return primaryRecord.getReadUnmappedFlag() ? MULTIMAPPED_SUPPLEMENTARY_ONLY : MULTIMAPPED;
		}
	}

	/**
	 * Add +1 to the {@link TaskAttemptContext} counter of this {@link AlignedReadType} type and returns itself as well.
	 * 
	 * @param context
	 *            {@link TaskAttemptContext}
	 * @return {@link AlignedReadType} Of this instance.
	 */
	public AlignedReadType increment(TaskAttemptContext context)
	{
		context.getCounter(this).increment(1);
		return this;
	}
}
