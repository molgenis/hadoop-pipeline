package org.molgenis.hadoop.pipeline.application.sequences;

import java.util.ArrayList;
import java.util.List;

import htsjdk.samtools.SAMRecord;

/**
 * An aligned read pair. Stores the {@link SAMRecord}{@code s} belonging both aligned reads.
 */
public class AlignedReadPair
{
	/**
	 * The first read from a read pair.
	 */
	private AlignedRead first;

	/**
	 * The second read from a read pair.
	 */
	private AlignedRead second;

	/**
	 * The read pair type.
	 */
	private AlignedReadPairType type;

	public AlignedRead getFirst()
	{
		return first;
	}

	public AlignedRead getSecond()
	{
		return second;
	}

	public AlignedReadPairType getType()
	{
		return type;
	}

	/**
	 * Generate a new {@link AlignedReadPair} based on a single {@link SAMRecord} {@link List} containing records from
	 * both reads.
	 * 
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecord}{@code >}
	 */
	public AlignedReadPair(List<SAMRecord> records)
	{
		divideRecords(records);
		type = AlignedReadPairType.determineType(first.getType(), second.getType());
	}

	/**
	 * Orders and evaluates whether the stored {@link SAMRecord}{@code s} are correct. Note that if the stored
	 * {@link SAMRecord} {@link List}{@code s} of the {@link AlignedRead}{@code s} or the {@link SAMRecord}{@code s} in
	 * it are manipulated outside of this class, this method NEEDS TO BE CALLED BEFORE DOING ANYTHING ELSE!
	 */
	public void update()
	{
		first.update();
		second.update();
		type = AlignedReadPairType.determineType(first.getType(), second.getType());
	}

	/**
	 * Splits a single {@link List} with {@link SAMRecord}{@code s} into two {@link AlignedRead}{@code s}. Uses
	 * {@link SAMRecord#getFirstOfPairFlag()} to determine to which {@link AlignedRead} each {@link SAMRecord} belongs
	 * to.
	 * 
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecord}{@code >}
	 */
	private void divideRecords(List<SAMRecord> records)
	{
		ArrayList<SAMRecord> firstReads = new ArrayList<>();
		ArrayList<SAMRecord> secondReads = new ArrayList<>();
		for (SAMRecord record : records)
		{
			if (record.getFirstOfPairFlag())
			{
				firstReads.add(record);
			}
			else
			{
				secondReads.add(record);
			}
		}

		first = new AlignedRead(firstReads);
		second = new AlignedRead(secondReads);
	}
}
