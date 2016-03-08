package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.molgenis.hadoop.pipeline.application.writables.RegionSamRecordStartWritable;

/**
 * Custom grouping comparator for the {@link RegionSamRecordStartWritable}, where only
 * {@link RegionSamRecordStartWritable#getRegionWritable()} is used within the grouping comparator (so the natural key
 * part from the composite key).
 */
public class RegionSamRecordGroupingComparator extends WritableComparator
{
	public RegionSamRecordGroupingComparator()
	{
		super(RegionSamRecordStartWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	/**
	 * This comparator controls which keys are grouped together into a single call to the reduce() method.
	 */
	public int compare(WritableComparable first, WritableComparable second)
	{
		RegionSamRecordStartWritable castedFirst = (RegionSamRecordStartWritable) first;
		RegionSamRecordStartWritable castedSecond = (RegionSamRecordStartWritable) second;
		return castedFirst.getRegionWritable().compareTo(castedSecond.getRegionWritable());
	}
}
