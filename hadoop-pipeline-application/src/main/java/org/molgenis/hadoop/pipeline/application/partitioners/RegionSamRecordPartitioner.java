package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;
import org.molgenis.hadoop.pipeline.application.writables.RegionSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 * Custom partitioner for the key:value pair {@link RegionSamRecordStartWritable}:{@link SAMRecordWritable}, where only
 * {@link RegionSamRecordStartWritable#getRegionWritable()} is used within the partitioner (so the natural key part from
 * the composite key).
 */
public class RegionSamRecordPartitioner extends Partitioner<RegionSamRecordStartWritable, SAMRecordWritable>
{
	@Override
	public int getPartition(RegionSamRecordStartWritable key, SAMRecordWritable value, int numPartitions)
	{
		return Math.abs(key.getRegionWritable().hashCode() % numPartitions);
	}
}
