package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;
import org.molgenis.hadoop.pipeline.application.writables.RegionWithSortableSamRecordWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 * Custom partitioner for the key:value pair {@link RegionWithSortableSamRecordWritable}:{@link SAMRecordWritable}, where only
 * {@link RegionWithSortableSamRecordWritable#getRegionWritable()} is used within the partitioner (so the natural key part from
 * the composite key). The partitioner controls the splitting of mapper output over the reducers. Each reducer can get
 * multiple keys with accompanying values.
 */
public class RegionSamRecordPartitioner extends Partitioner<RegionWithSortableSamRecordWritable, SAMRecordWritable>
{
	@Override
	public int getPartition(RegionWithSortableSamRecordWritable key, SAMRecordWritable value, int numPartitions)
	{
		return Math.abs(key.getRegionWritable().hashCode() % numPartitions);
	}
}
