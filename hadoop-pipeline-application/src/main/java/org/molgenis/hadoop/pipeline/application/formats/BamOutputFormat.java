package org.molgenis.hadoop.pipeline.application.formats;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.molgenis.hadoop.pipeline.application.cachedigestion.SamFileHeaderGenerator;
import org.seqdoop.hadoop_bam.BAMOutputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMRecordWriter;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;

/**
 * Custom implementation replacing {@link KeyIgnoringBAMOutputFormat} where the {@link SAMFileHeader} is generated using
 * the {@link TaskAttemptContext} from {@link #getRecordWriter(TaskAttemptContext)}. This means the
 * {@link SAMFileHeader} is generated at the moment a {@link RecordWriter} is retrieved from the {@link OutputFormat},
 * though this allows the distributed cache to be used for generating the {@link SAMFileHeader}.
 * 
 * @param <K>
 */
public class BamOutputFormat<K> extends BAMOutputFormat<K>
{
	@Override
	public RecordWriter<K, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException
	{
		return new KeyIgnoringBAMRecordWriter<K>(getDefaultWorkFile(ctx, ""),
				SamFileHeaderGenerator.retrieveSamFileHeader(ctx), true, ctx);
	}
}
