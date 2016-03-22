package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.molgenis.hadoop.pipeline.application.writables.RegionSamRecordStartWritable;

/**
 * Custom grouping comparator for the {@link RegionSamRecordStartWritable}, where only
 * {@link RegionSamRecordStartWritable#getRegionWritable()} is used within the grouping comparator (so the natural key
 * part from the composite key). This comparator controls which keys are grouped together into a single call to the
 * reduce() method.
 */
public class RegionSamRecordGroupingComparator extends WritableComparator
{
	protected RegionSamRecordGroupingComparator()
	{
		// Gives WritableComparable class and the creation of instances for the keys is set to true.
		super(RegionSamRecordStartWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable first, WritableComparable second)
	{
		RegionSamRecordStartWritable castedFirst = (RegionSamRecordStartWritable) first;
		RegionSamRecordStartWritable castedSecond = (RegionSamRecordStartWritable) second;

		// Comparison is done only on the RegionWritable (natural key).
		return castedFirst.getRegionWritable().compareTo(castedSecond.getRegionWritable());
	}
}
