package org.molgenis.hadoop.pipeline.application.formats;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.molgenis.hadoop.pipeline.application.cachedigestion.SamFileHeaderGenerator;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMRecordWriter;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader.SortOrder;

/**
 * Custom {@link FileOutputFormat} similar to {@link BamOutputFormat}. However, this class defines the output to be
 * sorted based on {@link SortOrder#coordinate}.
 * 
 * @param <K>
 */
public class SortedBamOutputFormat<K> extends BamOutputFormat<K>
{
	@Override
	public RecordWriter<K, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException
	{
		return new KeyIgnoringBAMRecordWriter<K>(getDefaultWorkFile(ctx, ""),
				SamFileHeaderGenerator.retrieveSamFileHeader(ctx, SortOrder.coordinate), true, ctx);
	}
}
