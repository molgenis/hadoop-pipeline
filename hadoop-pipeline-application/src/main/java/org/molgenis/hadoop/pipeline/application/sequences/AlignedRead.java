package org.molgenis.hadoop.pipeline.application.sequences;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import htsjdk.samtools.SAMRecord;

/**
 * An aligned read. Stores the {@link SAMRecord}{@code s} belonging to a single read that was aligned.
 */
public class AlignedRead
{
	/**
	 * The {@link SAMRecord}{@code s} belonging to the aligned read.
	 */
	private List<SAMRecord> records;

	/**
	 * The alignment type of the read.
	 */
	private Type type;

	/**
	 * Returns whether the {@code type} is {@link AlignedReadType#INVALID}. If so, returns {@code false}. If any other
	 * {@link AlignedReadType}, returns {@code true}.
	 * 
	 * @return {@code boolean} - If valid {@code true}, otherwise {@code false}.
	 */
	public boolean isValid()
	{
		return type != Type.INVALID;
	}

	/**
	 * Returns the primary read only ({@link SAMRecord#isSecondaryOrSupplementary()}{@code == false}). If
	 * {@link #isValid()}{@code == false}, returns {@code null} instead.
	 * 
	 * @return {@link SAMRecord}
	 */
	public SAMRecord getPrimaryRecord()
	{
		if (!isValid()) return null;
		return records.get(0);
	}

	/**
	 * Returns all supplementary/secondary records ({@link SAMRecord#isSecondaryOrSupplementary()}{@code == true}). If
	 * {@link #isValid()}{@code == false}, returns {@code null} instead.
	 * 
	 * @return {@link List}{@code <}{@link SAMRecord}{@code >}
	 */
	public List<SAMRecord> getSupplementaryRecords()
	{
		if (!isValid()) return null;
		return records.subList(1, records.size());
	}

	/**
	 * Returns all records.
	 * 
	 * @return {@link List}{@code <}{@link SAMRecord}{@code >}
	 */
	public List<SAMRecord> getRecords()
	{
		return records;
	}

	/**
	 * Sets a new {@link SAMRecord} {@link List} and validates it.
	 * 
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecord}{@code >}
	 * @see {@link #update()}
	 */
	public void setRecords(List<SAMRecord> records)
	{
		this.records = records;
		update();
	}

	/**
	 * Returns the {@link AlignedReadType}.
	 * 
	 * @return {@link AlignedReadType}.
	 */
	public Type getType()
	{
		return type;
	}

	/**
	 * Create a new {@link AlignedRead}.
	 * 
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecord}{@code >}
	 */
	public AlignedRead(List<SAMRecord> records)
	{
		this.records = requireNonNull(records);
		update();
	}

	/**
	 * Orders and evaluates whether the stored {@link SAMRecord}{@code s} are correct. Note that if the stored
	 * {@link List} or the {@link SAMRecord}{@code s} in it are manipulated outside of this class, this method NEEDS TO
	 * BE CALLED BEFORE DOING ANYTHING ELSE!
	 */
	public void update()
	{
		orderRecords();
		type = Type.determineType(records);
	}

	/**
	 * Orders the {@link SAMRecord}{@code s} so that the primary record(s) are at the start.
	 */
	private void orderRecords()
	{
		for (int i = 0; i < records.size(); i++)
		{
			if (!records.get(i).isSecondaryOrSupplementary())
			{
				// Moves primary record to the first position.
				records.add(0, records.get(i));
				records.remove(i + 1);
			}
		}
	}

	/**
	 * The type of an {@link AlignedRead}.
	 */
	public enum Type
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
		 * Determines the {@link AlignedRead.Type} of a {@link List}{@code <}{@link SAMRecord}{@code >}. If the
		 * {@link List}{@code <}{@link SAMRecord}{@code >} does not adhere to the requirements, returns
		 * {@link AlignedRead.Type#INVALID}. Otherwise, returns one of the other possible types. Assumes the given
		 * {@link List}{@code <}{@link SAMRecord}{@code >} is ordered using {@link AlignedRead#orderRecords()}.
		 * 
		 * @param records
		 *            {@link List}{@code <}{@link SAMRecord}{@code >}
		 * @return {@link AlignedRead.Type}
		 */
		private static Type determineType(List<SAMRecord> records)
		{
			// When list is empty, type is INVALID.
			if (records.size() == 0) return INVALID;

			// Retrieves first record and validates whether it is a primary record.
			// As the input parameter 'records' is sorted using AlignedRead's orderRecords(),
			// it can be assumed the primary record(s) are it the first position(s).
			SAMRecord firstRecord = records.get(0);
			if (firstRecord.isSecondaryOrSupplementary()) return INVALID;

			// If there is only 1 record, it can either be mapped or unmapped.
			if (records.size() == 1)
			{
				return firstRecord.getReadUnmappedFlag() ? UNMAPPED : MAPPED;
			}
			else // If there are multiple records:
			{
				// Validates that the second record is not a primary record.
				if (!records.get(1).isSecondaryOrSupplementary()) return INVALID;

				// If the records belong to a read pair read and includes records that are from the other read compared
				// to the first record, returns INVALID.
				if (firstRecord.getReadPairedFlag())
				{
					for (SAMRecord record : records)
					{
						if (record.getFirstOfPairFlag() != firstRecord.getFirstOfPairFlag()) return INVALID;
					}
				}

				// Primary and supplementary reads can be mapped or only the latter.
				return firstRecord.getReadUnmappedFlag() ? MULTIMAPPED_SUPPLEMENTARY_ONLY : MULTIMAPPED;
			}
		}

		/**
		 * Add +1 to the {@link TaskAttemptContext} counter of this {@link AlignedRead.Type} and returns itself as well.
		 * 
		 * @param context
		 *            {@link TaskAttemptContext}
		 * @return {@link AlignedRead.Type} Of this instance.
		 */
		public Type increment(TaskAttemptContext context)
		{
			context.getCounter(this).increment(1);
			return this;
		}
	}

}
