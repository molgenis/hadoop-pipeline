package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.molgenis.hadoop.pipeline.application.writables.RegionWithSortableSamRecordWritable;

/**
 * Custom grouping comparator for the {@link RegionWithSortableSamRecordWritable}, where only
 * {@link RegionWithSortableSamRecordWritable#getRegionWritable()} is used within the grouping comparator (so the natural key
 * part from the composite key). This comparator controls which keys are grouped together into a single call to the
 * reduce() method.
 */
public class RegionSamRecordGroupingComparator extends WritableComparator
{
	public RegionSamRecordGroupingComparator()
	{
		// Gives WritableComparable class and the creation of instances for the keys is set to true.
		super(RegionWithSortableSamRecordWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable first, WritableComparable second)
	{
		RegionWithSortableSamRecordWritable castedFirst = (RegionWithSortableSamRecordWritable) first;
		RegionWithSortableSamRecordWritable castedSecond = (RegionWithSortableSamRecordWritable) second;

		// Comparison is done only on the RegionWritable (natural key).
		return castedFirst.getRegionWritable().compareTo(castedSecond.getRegionWritable());
	}
}
