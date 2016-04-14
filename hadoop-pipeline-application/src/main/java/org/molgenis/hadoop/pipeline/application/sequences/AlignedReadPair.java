package org.molgenis.hadoop.pipeline.application.sequences;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
	private Type type;

	public AlignedRead getFirst()
	{
		return first;
	}

	public AlignedRead getSecond()
	{
		return second;
	}

	public Type getType()
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
		divideRecords(records); // AlignedRead updates already called in their own constructor.
		updateType();
	}

	/**
	 * Updates both {@link AlignedRead}{@code s} and retrieves the {@link AlignedReadPair.Type} of this
	 * {@link AlignedReadPair}. Use this method if after initialization the {@link AlignedRead}{@code s} have been
	 * adjusted. If an {@link AlignedRead#setRecords(List)} was used, {@link #updateType()} can be used instead.
	 */
	public void update()
	{
		first.update();
		second.update();
		updateType();
	}

	/**
	 * Retrieves the {@link AlignedReadPair.Type} of this {@link AlignedReadPair}. This method does not call the
	 * {@link AlignedRead#update()} method for the stored {@link AlignedRead}{@code s}. This method can for example be
	 * used if {@link AlignedRead#setRecords(List)} is called from one of the stored {@link AlignedRead}{@code s}, as
	 * these already call their own {@link AlignedRead#update()} already. If the actual stored {@link List}{@code <}
	 * {@link SAMRecord}{@code >} is manipulated directly however, either the {@link #update()} from this class should
	 * be called or the {@link AlignedRead#update()} from which the {@link List}{@code <}{@link SAMRecord}{@code >} was
	 * adjusted followed by {@link #updateType()}.
	 */
	public void updateType()
	{
		type = Type.determineType(first.getType(), second.getType());
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

	/**
	 * The type of an {@link AlignedReadPair}.
	 */
	public enum Type
	{
		/**
		 * Both {@link AlignedRead}{@code s} are unmapped.
		 */
		BOTH_UNMAPPED(AlignedRead.Type.UNMAPPED, AlignedRead.Type.UNMAPPED),

		/**
		 * Both {@link AlignedRead}{@code s} are mapped.
		 */
		BOTH_MAPPED(AlignedRead.Type.MAPPED, AlignedRead.Type.MAPPED),

		/**
		 * Both {@link AlignedRead}{@code s} mapped multiple times.
		 */
		BOTH_MULTIMAPPED(AlignedRead.Type.MULTIMAPPED, AlignedRead.Type.MULTIMAPPED),

		/**
		 * Both {@link AlignedRead}{@code s} only had supplementary or secondary alignments.
		 */
		BOTH_MULTIMAPPED_SUPPLEMENTARY_ONLY(AlignedRead.Type.MULTIMAPPED_SUPPLEMENTARY_ONLY,
				AlignedRead.Type.MULTIMAPPED_SUPPLEMENTARY_ONLY),

		/**
		 * One {@link AlignedRead} is unmapped and the other {@link AlignedRead} is mapped.
		 */
		ONE_UNMAPPED_ONE_MAPPED(AlignedRead.Type.UNMAPPED, AlignedRead.Type.MAPPED),

		/**
		 * One {@link AlignedRead} is unmapped and the other mapped multiple times.
		 */
		ONE_UNMAPPED_ONE_MULTIMAPPED(AlignedRead.Type.UNMAPPED, AlignedRead.Type.MULTIMAPPED),

		/**
		 * One {@link AlignedRead} is unmapped and the other only had supplementary or secondary alignments.
		 */
		ONE_UNMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY(AlignedRead.Type.UNMAPPED,
				AlignedRead.Type.MULTIMAPPED_SUPPLEMENTARY_ONLY),

		/**
		 * One {@link AlignedRead} is mapped and the other mapped multiple times.
		 */
		ONE_MAPPED_ONE_MULTIMAPPED(AlignedRead.Type.MAPPED, AlignedRead.Type.MULTIMAPPED),

		/**
		 * One {@link AlignedRead} is mapped and the other mapped multiple times.
		 */
		ONE_MAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY(AlignedRead.Type.MAPPED,
				AlignedRead.Type.MULTIMAPPED_SUPPLEMENTARY_ONLY),

		/**
		 * One {@link AlignedRead} mapped multiple times and the other only had supplementary or secondary alignments.
		 */
		ONE_MULTIMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY(AlignedRead.Type.MULTIMAPPED,
				AlignedRead.Type.MULTIMAPPED_SUPPLEMENTARY_ONLY),

		/**
		 * The data is deemed invalid. Note that if even one {@link AlignedRead.Type} is invalid, the
		 * {@link AlignedReadPair.Type} should be deemed as {@link #INVALID}.
		 */
		INVALID(AlignedRead.Type.INVALID, AlignedRead.Type.INVALID);

		/**
		 * Stores an {@link AlignedRead.Type}{@code s} that together with {@code secondAlignedReadType} represents a
		 * specific {@link AlignedReadPair.Type}.
		 */
		private AlignedRead.Type firstAlignedReadType;

		/**
		 * Stores a {@link AlignedRead.Type}{@code s} that together with {@code firstAlignedReadType} represents a
		 * specific {@link AlignedReadPair.Type}.
		 */
		private AlignedRead.Type secondAlignedReadType;

		private AlignedRead.Type getFirstAlignedReadType()
		{
			return firstAlignedReadType;
		}

		private AlignedRead.Type getSecondAlignedReadType()
		{
			return secondAlignedReadType;
		}

		/**
		 * Constructor for AlignedReadPair.Type.
		 * 
		 * @param firstAlignedReadType
		 *            {@link AlignedRead.Type} One of the two {@link AlignedRead.Type}{@code s} that define a specific
		 *            {@link AlignedReadPair.Type}.
		 * @param secondAlignedReadType
		 *            {@link AlignedRead.Type} One of the two {@link AlignedRead.Type}{@code s} that define a specific
		 *            {@link AlignedReadPair.Type}.
		 */
		private Type(AlignedRead.Type firstAlignedReadType, AlignedRead.Type secondAlignedReadType)
		{
			this.firstAlignedReadType = firstAlignedReadType;
			this.secondAlignedReadType = secondAlignedReadType;
		}

		/**
		 * Determines the {@link AlignedReadPair.Type} using two {@link AlignedRead.Type}{@code s}. If either
		 * {@link AlignedRead.Type} is {@link AlignedRead.Type#INVALID}, returns {@link AlignedReadPair.Type#INVALID}.
		 * Otherwise, returns one of the other possible types.
		 * 
		 * @param first
		 *            {@link AlignedRead.Type}
		 * @param second
		 *            {@link AlignedRead.Type}
		 * @return {@link AlignedReadPairType}
		 */
		private static Type determineType(AlignedRead.Type first, AlignedRead.Type second)
		{
			// Goes through all possible options (excluding if one is invalid while another is not).
			for (Type alignedReadPairType : Type.values())
			{
				// As only the combination of the two AlignedReadTypes matter, checks both possibilities.
				if (alignedReadPairType.getFirstAlignedReadType().equals(first)
						&& alignedReadPairType.getSecondAlignedReadType().equals(second))
					return alignedReadPairType;
				else if (alignedReadPairType.getFirstAlignedReadType().equals(second)
						&& alignedReadPairType.getSecondAlignedReadType().equals(first))
					return alignedReadPairType;
			}

			// If any of the tried possibilities did not match, returns invalid (including when only one is invalid).
			return INVALID;
		}

		/**
		 * Add +1 to the {@link TaskAttemptContext} counter of this {@link AlignedReadPair.Type} and returns itself as
		 * well.
		 * 
		 * @param context
		 *            {@link TaskAttemptContext}
		 * @return {@link AlignedReadPair.Type} Of this instance.
		 */
		public Type increment(TaskAttemptContext context)
		{
			context.getCounter(this).increment(1);
			return this;
		}
	}
}
