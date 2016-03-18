package org.molgenis.hadoop.pipeline.application.sequences;

import static java.util.Objects.requireNonNull;

import java.util.List;

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
	private AlignedReadType type;

	/**
	 * Returns whether the {@code type} is {@link AlignedReadType#INVALID}. If so, returns {@code false}. If any other
	 * {@link AlignedReadType}, returns {@code true}.
	 * 
	 * @return {@code boolean} - If valid {@code true}, otherwise {@code false}.
	 */
	public boolean isValid()
	{
		return type != AlignedReadType.INVALID;
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
	public AlignedReadType getType()
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
		type = AlignedReadType.determineType(records);
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
}
