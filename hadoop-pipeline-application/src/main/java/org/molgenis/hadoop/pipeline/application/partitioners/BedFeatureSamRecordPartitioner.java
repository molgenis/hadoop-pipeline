package org.molgenis.hadoop.pipeline.application.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 * Custom partitioner for the key:value pair {@link BedFeatureSamRecordStartWritable}:{@link SAMRecordWritable}, where
 * only {@link BedFeatureSamRecordStartWritable#getBedFeatureWritable()} is used within the partitioner (so the natural
 * key part from the composite key).
 */
public class BedFeatureSamRecordPartitioner extends Partitioner<BedFeatureSamRecordStartWritable, SAMRecordWritable>
{
	@Override
	public int getPartition(BedFeatureSamRecordStartWritable key, SAMRecordWritable value, int numPartitions)
	{
		return Math.abs(key.getBedFeatureWritable().hashCode() % numPartitions);
	}
}
