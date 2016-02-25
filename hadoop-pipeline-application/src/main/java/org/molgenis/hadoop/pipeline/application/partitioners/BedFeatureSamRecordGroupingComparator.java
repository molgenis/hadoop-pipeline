package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;

/**
 * Custom grouping comparator for the {@link BedFeatureSamRecordStartWritable}, where only
 * {@link BedFeatureSamRecordStartWritable#getBedFeatureWritable()} is used within the grouping comparator (so the
 * natural key part from the composite key).
 */
public class BedFeatureSamRecordGroupingComparator extends WritableComparator
{
	public BedFeatureSamRecordGroupingComparator()
	{
		super(BedFeatureSamRecordStartWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	/**
	 * This comparator controls which keys are grouped together into a single call to the reduce() method
	 */
	public int compare(WritableComparable first, WritableComparable second)
	{
		BedFeatureSamRecordStartWritable castedFirst = (BedFeatureSamRecordStartWritable) first;
		BedFeatureSamRecordStartWritable castedSecond = (BedFeatureSamRecordStartWritable) second;
		return castedFirst.getBedFeatureWritable().compareTo(castedSecond.getBedFeatureWritable());
	}
}
