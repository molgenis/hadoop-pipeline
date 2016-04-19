package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;

import htsjdk.samtools.SAMRecord;

/**
 * {@link WritableComparable} storing a {@link Region} (natural key) together with the {@code int} from
 * {@link SAMRecord#getStart()} that together can be used as composite key for a secondary sort.
 */
public class RegionWithSortableSamRecordWritable implements WritableComparable<RegionWithSortableSamRecordWritable>
{
	/**
	 * Stores the the natural key, which also functions as a part from the composite key.
	 */
	private RegionWritable regionWritable;

	/**
	 * Stores a part of the composite key; the index of the reference sequence the record is mapped to.
	 */
	private int samRecordReferenceIndex;

	/**
	 * Stores a part of the composite key; the left-most mapping position on the reference sequence.
	 */
	private int samRecordStart;

	/**
	 * Returns the natural key as a non-{@link Writable}.
	 * 
	 * @return {@link Region}
	 */
	public Region get()
	{
		return regionWritable.get();
	}

	public RegionWritable getRegionWritable()
	{
		return regionWritable;
	}

	public int getSamRecordReferenceIndex()
	{
		return samRecordReferenceIndex;
	}

	public int getSamRecordStart()
	{
		return samRecordStart;
	}

	/**
	 * Create an empty {@link RegionWithSortableSamRecordWritable} instance. Otherwise a Hadoop job will throw the
	 * following {@link Exception}:
	 * 
	 * <pre>
	 * java.lang.NoSuchMethodException: org.molgenis.hadoop.pipeline.application.writables.RegionSamRecordStartWritable.&lt;init&gt;()
	 * </pre>
	 */
	public RegionWithSortableSamRecordWritable()
	{
	}

	/**
	 * Store a {@link Region} as {@link Writable} together with {@link SAMRecord#getStart()} from a {@link SAMRecord}.
	 * The {@link Region} will function as the natural key, while a combination of the {@link Region} with
	 * {@link SAMRecord#getStart()} will function as composite key.
	 * 
	 * @param region
	 *            {@link Region}
	 * @param record
	 *            {@link SAMRecord}
	 * @see {@link RegionWritable#RegionWritable(Region)}
	 * @throws IllegalArgumentException
	 */
	public RegionWithSortableSamRecordWritable(Region region, SAMRecord record) throws IllegalArgumentException
	{
		requireNonNull(region);
		requireNonNull(record.getReferenceIndex());
		requireNonNull(record.getStart());
		this.regionWritable = new RegionWritable(region);
		this.samRecordReferenceIndex = record.getReferenceIndex();
		this.samRecordStart = record.getStart();
	}

	@Override
	public String toString()
	{
		return "RegionSamRecordStartWritable [regionWritable=" + regionWritable + ", samRecordReferenceIndex="
				+ samRecordReferenceIndex + ", samRecordStart=" + samRecordStart + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		regionWritable.write(out);
		out.writeInt(samRecordReferenceIndex);
		out.writeInt(samRecordStart);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		regionWritable = new RegionWritable();
		regionWritable.readFields(in);
		samRecordReferenceIndex = in.readInt();
		samRecordStart = in.readInt();
	}

	@Override
	public int compareTo(RegionWithSortableSamRecordWritable o)
	{
		int c = regionWritable.compareTo(o.regionWritable);
		if (c == 0)
		{
			// Compares reference index where -1 is regarded as highest value.
			if (samRecordReferenceIndex == -1 || o.samRecordReferenceIndex == -1)
				c = o.samRecordReferenceIndex - samRecordReferenceIndex;
			else c = samRecordReferenceIndex - o.samRecordReferenceIndex;
		}
		if (c == 0) c = samRecordStart - o.samRecordStart;

		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((regionWritable == null) ? 0 : regionWritable.hashCode());
		result = prime * result + samRecordReferenceIndex;
		result = prime * result + samRecordStart;
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		RegionWithSortableSamRecordWritable other = (RegionWithSortableSamRecordWritable) obj;
		if (regionWritable == null)
		{
			if (other.regionWritable != null) return false;
		}
		else if (!regionWritable.equals(other.regionWritable)) return false;
		if (samRecordReferenceIndex != other.samRecordReferenceIndex) return false;
		if (samRecordStart != other.samRecordStart) return false;
		return true;
	}
}
