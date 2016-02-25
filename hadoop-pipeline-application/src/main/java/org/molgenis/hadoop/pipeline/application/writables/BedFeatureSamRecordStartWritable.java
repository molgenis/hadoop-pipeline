package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

/**
 * {@link Writable} with a composite key that allows for sorting the values when combining this {@link Writable} as key
 * for the {@link Writable} {@link SAMRecordWritable} as value.
 */
public class BedFeatureSamRecordStartWritable implements WritableComparable<BedFeatureSamRecordStartWritable>
{
	/**
	 * Stores the the natural key, which also functions as a part from the composite key.
	 */
	private BedFeatureWritable bedFeatureWritable;

	/**
	 * Stores the second composite key part, which is the start value of the given {@link SAMRecord}.
	 */
	private int samRecordStart;

	/**
	 * Returns the natural key as a non-{@link Writable}.
	 * 
	 * @return {@link BEDFeature}
	 */
	public BEDFeature get()
	{
		return bedFeatureWritable.get();
	}

	public BedFeatureWritable getBedFeatureWritable()
	{
		return bedFeatureWritable;
	}

	public int getSamRecordStart()
	{
		return samRecordStart;
	}

	/**
	 * Create an empty {@link BedFeatureSamRecordStartWritable} instance. Otherwise a Hadoop job will throw the
	 * following {@link Exception}:
	 * 
	 * <pre>
	 * java.lang.NoSuchMethodException: org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable.&lt;init&gt;()
	 * </pre>
	 */
	public BedFeatureSamRecordStartWritable()
	{
	}

	/**
	 * Store a {@link BEDFeature} as {@link Writable} together with {@link SAMRecord#getStart()} from a
	 * {@link SAMRecord}. The {@link BEDFeature} will function as the natural key, while a combination of the
	 * {@link BEDFeature} with {@link SAMRecord#getStart()} will function as composite key.
	 * 
	 * @param bedFeature
	 *            {@link BEDFeature}
	 * @param record
	 *            {@link SAMRecord}
	 * @see {@link BedFeatureWritable#BedFeatureWritable(BEDFeature)}
	 */
	public BedFeatureSamRecordStartWritable(BEDFeature bedFeature, SAMRecord record)
	{
		requireNonNull(bedFeature); // BedFeatureWritable validates individual fields.
		requireNonNull(record.getStart());
		this.bedFeatureWritable = new BedFeatureWritable(bedFeature);
		this.samRecordStart = record.getStart();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		bedFeatureWritable.write(out);
		out.writeInt(samRecordStart);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		bedFeatureWritable = new BedFeatureWritable();
		bedFeatureWritable.readFields(in);
		samRecordStart = in.readInt();
	}

	@Override
	public int compareTo(BedFeatureSamRecordStartWritable o)
	{
		int c = bedFeatureWritable.compareTo(o.bedFeatureWritable);
		if (c == 0) c = samRecordStart - o.samRecordStart;

		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bedFeatureWritable == null) ? 0 : bedFeatureWritable.hashCode());
		result = prime * result + samRecordStart;
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		BedFeatureSamRecordStartWritable other = (BedFeatureSamRecordStartWritable) obj;
		if (bedFeatureWritable == null)
		{
			if (other.bedFeatureWritable != null) return false;
		}
		else if (!bedFeatureWritable.equals(other.bedFeatureWritable)) return false;
		if (samRecordStart != other.samRecordStart) return false;
		return true;
	}
}
