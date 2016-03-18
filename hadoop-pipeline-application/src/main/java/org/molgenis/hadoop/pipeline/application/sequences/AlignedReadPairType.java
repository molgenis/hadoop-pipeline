package org.molgenis.hadoop.pipeline.application.sequences;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The type of an {@link AlignedReadPair}.
 */
public enum AlignedReadPairType
{
	/**
	 * Both {@link AlignedRead}{@code s} are unmapped.
	 */
	BOTH_UNMAPPED,

	/**
	 * Both {@link AlignedRead}{@code s} are mapped.
	 */
	BOTH_MAPPED,

	/**
	 * Both {@link AlignedRead}{@code s} mapped multiple times.
	 */
	BOTH_MULTIMAPPED,

	/**
	 * Both {@link AlignedRead}{@code s} only had supplementary or secondary alignments.
	 */
	BOTH_MULTIMAPPED_SUPPLEMENTARY_ONLY,

	/**
	 * One {@link AlignedRead} is unmapped and the other {@link AlignedRead} is mapped.
	 */
	ONE_UNMAPPED_ONE_MAPPED,

	/**
	 * One {@link AlignedRead} is unmapped and the other mapped multiple times.
	 */
	ONE_UNMAPPED_ONE_MULTIMAPPED,

	/**
	 * One {@link AlignedRead} is unmapped and the other only had supplementary or secondary alignments.
	 */
	ONE_UNMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY,

	/**
	 * One {@link AlignedRead} is mapped and the other mapped multiple times.
	 */
	ONE_MAPPED_ONE_MULTIMAPPED,

	/**
	 * One {@link AlignedRead} is mapped and the other mapped multiple times.
	 */
	ONE_MAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY,

	/**
	 * One {@link AlignedRead} mapped multiple times and the other only had supplementary or secondary alignments.
	 */
	ONE_MULTIMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY,

	/**
	 * The data is deemed invalid.
	 */
	INVALID;

	/**
	 * Determines the {@link AlignedReadPairType} using two {@link AlignedReadType}{@code s}. If either
	 * {@link AlignedReadType} is {@link AlignedReadType#INVALID}, returns {@link #INVALID}. Otherwise, returns one of
	 * the other possible types.
	 * 
	 * @param first
	 *            {@link AlignedReadType}
	 * @param second
	 *            {@link AlignedReadType}
	 * @return {@link AlignedReadPairType}
	 */
	@SuppressWarnings("incomplete-switch")
	static AlignedReadPairType determineType(AlignedReadType first, AlignedReadType second)
	{
		// Goes through the possible valid combinations of the two AlignedReadTypes.
		switch (first)
		{
			case UNMAPPED:
				switch (second)
				{
					case UNMAPPED:
						return BOTH_UNMAPPED;
					case MAPPED:
						return ONE_UNMAPPED_ONE_MAPPED;
					case MULTIMAPPED:
						return ONE_UNMAPPED_ONE_MULTIMAPPED;
					case MULTIMAPPED_SUPPLEMENTARY_ONLY:
						return ONE_UNMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY;
				}
				break;

			case MAPPED:
				switch (second)
				{
					case UNMAPPED:
						return ONE_UNMAPPED_ONE_MAPPED;
					case MAPPED:
						return BOTH_MAPPED;
					case MULTIMAPPED:
						return ONE_MAPPED_ONE_MULTIMAPPED;
					case MULTIMAPPED_SUPPLEMENTARY_ONLY:
						return ONE_MAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY;
				}
				break;

			case MULTIMAPPED:
				switch (second)
				{
					case UNMAPPED:
						return ONE_UNMAPPED_ONE_MULTIMAPPED;
					case MAPPED:
						return ONE_MAPPED_ONE_MULTIMAPPED;
					case MULTIMAPPED:
						return BOTH_MULTIMAPPED;
					case MULTIMAPPED_SUPPLEMENTARY_ONLY:
						return ONE_MULTIMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY;
				}
				break;

			case MULTIMAPPED_SUPPLEMENTARY_ONLY:
				switch (second)
				{
					case UNMAPPED:
						return ONE_UNMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY;
					case MAPPED:
						return ONE_MAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY;
					case MULTIMAPPED:
						return ONE_MULTIMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY;
					case MULTIMAPPED_SUPPLEMENTARY_ONLY:
						return BOTH_MULTIMAPPED_SUPPLEMENTARY_ONLY;
				}
				break;
		}

		// Returns invalid if none of the above scenarios were true.
		return INVALID;
	}

	/**
	 * Add +1 to the {@link TaskAttemptContext} counter of this {@link AlignedReadPairType} type and returns itself as
	 * well.
	 * 
	 * @param context
	 *            {@link TaskAttemptContext}
	 * @return {@link AlignedReadPairType} Of this instance.
	 */
	public AlignedReadPairType increment(TaskAttemptContext context)
	{
		context.getCounter(this).increment(1);
		return this;
	}
}
