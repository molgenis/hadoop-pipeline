package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import org.apache.hadoop.io.Writable;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;

/**
 * Writable for {@link SAMRecord}{@code s} that should allow for easier creating of new {@link Writable}{@code s}
 * compared to {@link SAMRecordWritable}.
 */
public class SamRecordWritable extends SAMRecordWritable
{
	/**
	 * Create an empty {@link SamRecordWritable} instance. Otherwise a Hadoop job will throw the following
	 * {@link Exception}:
	 * 
	 * <pre>
	 * java.lang.NoSuchMethodException: org.molgenis.hadoop.pipeline.application.writables.SamRecordWritable.&lt;init&gt;()
	 * </pre>
	 */
	public SamRecordWritable()
	{
	}

	/**
	 * Store a {@link SAMRecord} as {@link Writable}.
	 * 
	 * @param region
	 *            {@link Region}
	 */
	public SamRecordWritable(SAMRecord record)
	{
		set(requireNonNull(record));
	}
}
