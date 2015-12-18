package org.molgenis.hadoop.pipeline.application.mapreduce;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

/**
 * Can retrieve the {@link BEDFeature}{@code s} a {@link SAMRecord} belongs to when grouping a {@link SAMRecord} on
 * their alignment position and the {@link BEDFeature}{@code s} defining the ranges for each individual group.
 */
public class SamRecordGroupsRetriever
{
	/**
	 * Stores the groups to which a {@link SAMRecord} can match to.
	 */
	ArrayList<BEDFeature> groups;

	/**
	 * Create a new instance using a set of {@link BEDFeature}{@code s} which can be used for retrieving the
	 * {@link BEDFeature}{@code s} a specific {@link SAMRecord} belongs to.
	 * 
	 * @param groups
	 *            {@link ArrayList}{@code <}{@link BEDFeature}{@code >} groups to be used for matching with a
	 *            {@link SAMRecord}.
	 */
	SamRecordGroupsRetriever(ArrayList<BEDFeature> groups)
	{
		this.groups = requireNonNull(groups);
	}

	/**
	 * Returns an {@link ArrayList} containing all {@ BEDFeature}{@code s} which range match with the given
	 * {@link SAMRecord}. These also include {@link BEDFeature}{@code s} which partially match with the given
	 * {@link SAMRecord}.
	 * 
	 * @param record
	 *            {@link SAMRecord} to be used to find the {@link BEDFeature}{@code s} that are within range of it.
	 * @return {@link ArrayList}{@code <}{@link BEDFeature}{@code >} the {@link BEDFeature}{@code s} within range of the
	 *         given {@link SAMRecord}.
	 */
	ArrayList<BEDFeature> retrieveGroupsWithinRange(SAMRecord record)
	{
		ArrayList<BEDFeature> matchingGroups = new ArrayList<BEDFeature>();

		// Retrieves the groups that match the contig of the SAMRecord.
		ArrayList<BEDFeature> contigGroups = getGroupsOfContig(record);

		// Starting from the first BEDFeature which has it's end value higher or equal to the SAMRecord start value,
		// continues through the remaining BEDFeatures until a BEDFeature is found which start value is higher than the
		// SAMRecord end value or if no remaining BEDFeatures are present. If the search for the first BEDFeature
		// returned null, skips looking for any other BEDFeatures that might match and simply returns and empty
		// ArrayList.
		Integer firstGroupIndex = retrieveFirstGroupWithEndHigherThanRecordStart(record, contigGroups);

		// Checks if a single match was found. Skips further looking.
		if (firstGroupIndex != null)
		{
			// Goes through the following BEDFeatures looking for additional matches.
			for (int i = firstGroupIndex; i < contigGroups.size(); i++)
			{
				BEDFeature group = contigGroups.get(i);
				if (group.getStart() > record.getEnd())
				{
					break;
				}
				matchingGroups.add(group);
			}
		}
		return matchingGroups;
	}

	/**
	 * Filters the {@code groups} containing all possible groups for groups that match the contig of the given
	 * {@link SAMRecord}.
	 * 
	 * @param record
	 *            {@link SAMRecord}
	 * @return {@link ArrayList}{@code <}{@link BEDFeature}{@code >} containing groups where the
	 *         {@link BEDFeature#getContig()} matches the {@link SAMRecord#getContig()}.
	 */
	private ArrayList<BEDFeature> getGroupsOfContig(SAMRecord record)
	{
		ArrayList<BEDFeature> contigGroups = new ArrayList<BEDFeature>();

		for (BEDFeature group : groups)
		{
			if (record.getContig().equals(group.getContig()))
			{
				contigGroups.add(group);
			}
		}
		return contigGroups;
	}

	/**
	 * Wrapper for {@link #retrieveFirstGroupWithEndHigherThanRecordStart(Integer, List, int, int)}.
	 * 
	 * @param record
	 *            {@link SAMRecord} to retrieve {@code startPosition} from.
	 * @param list
	 *            used for position comparison with the given {@code recordStart}.
	 * @return {@code int} value if position was found, otherwise {@code null}.
	 */
	private Integer retrieveFirstGroupWithEndHigherThanRecordStart(SAMRecord record, List<BEDFeature> list)
	{
		return retrieveFirstGroupWithEndHigherThanRecordStart(record.getStart(), list, 0, list.size());
	}

	/**
	 * Recursive function that returns the {@code index} as {@link Integer} of the first {@link BEDFeature} that has a
	 * {@link BEDFeature#getEnd()} that is higher than the {@link SAMRecord#getStart()}.
	 * 
	 * @param recordStart
	 *            {@code final} {@link Integer} value stored in {@link SAMRecord#getStart()}.
	 * @param list
	 *            {@link List}{@code <}{@link BEDFeature}{@code >} used for position comparison with the given
	 *            {@code recordStart}.
	 * @param low
	 *            {@code int} bottom position to be used for {@code list}.
	 * @param high
	 *            {@code int} upper position to be used for {@code list}.
	 * @return {@code int} value if position was found, otherwise {@code null}.
	 */
	private Integer retrieveFirstGroupWithEndHigherThanRecordStart(final Integer recordStart, List<BEDFeature> list,
			int low, int high)
	{
		// Retrieves basic information for further usage.
		int middle = (low + high) / 2; // Middle index, middle right if length is even.
		int size = high - low; // Size of remaining list area to be looked at.

		// Retrieves end value of the group present in the middle of the list.
		int middleGroupEnd = list.get(middle).getEnd();

		// If middle is lower than 1, the remaining List only contains 1 remaining BEDFeature. Ignores comparisons using
		// multiple list elements.
		if (size < 2)
		{
			// If the only remaining BEDfeature has a higher end value than the record start, returns it's position.
			if (middleGroupEnd >= recordStart)
			{
				return middle;
			}
		}
		else
		{
			// When there are at least 2 elements in the list, retrieve a second element for comparisons.
			int MiddleMinusOneGroupEnd = list.get(middle - 1).getEnd();

			// If middle position is the first one with an end value equal or higher than the record start, returns the
			// middle position value.
			if (MiddleMinusOneGroupEnd < recordStart && middleGroupEnd >= recordStart)
			{
				return middle;
			}
			// If the middle position has a lower end position than the record start, makes a recursive call with the
			// middle position as lowest index and the end of the list as highest index. Returns results directly
			// afterwards.
			else if (middleGroupEnd < recordStart)
			{
				return retrieveFirstGroupWithEndHigherThanRecordStart(recordStart, list, middle, list.size());
			}
			// If the position before the middle position has a higher or equal end value than the record start
			// position, makes a recursive call with the start of the list as the lowest index and the middle position
			// as the highest index. Returns results directly afterwards.
			// Should be equal to: MiddleMinusOneGroupEnd >= recordStart
			else
			{
				return retrieveFirstGroupWithEndHigherThanRecordStart(recordStart, list, 0, middle);
			}
		}
		// Returns null if the last remaining BEDFeature does not have an end value higher than the sam record start
		// value.
		return null;
	}
}
