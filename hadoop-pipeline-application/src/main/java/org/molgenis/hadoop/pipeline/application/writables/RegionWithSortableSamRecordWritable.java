package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.ObjectUtils;
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
	 * Stores the second composite key part, which is the contig name of the given {@link SAMRecord}.
	 */
	private String samRecordContig;

	/**
	 * Stores the third composite key part, which is the start value of the given {@link SAMRecord}.
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

	public String getSamRecordContig()
	{
		return samRecordContig;
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
		// record.getContig() is allowed to be null.
		requireNonNull(region);
		requireNonNull(record.getStart());
		this.regionWritable = new RegionWritable(region);
		this.samRecordContig = record.getContig();
		this.samRecordStart = record.getStart();
	}

	@Override
	public String toString()
	{
		return "RegionSamRecordStartWritable [regionWritable=" + regionWritable + ", samRecordContig=" + samRecordContig
				+ ", samRecordStart=" + samRecordStart + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		regionWritable.write(out);

		// Writes a boolean whether contig != null, and if so, writes the actual contig name.
		boolean hasContig = (samRecordContig != null);
		out.writeBoolean(hasContig);
		if (hasContig) out.writeUTF(samRecordContig);

		out.writeInt(samRecordStart);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		regionWritable = new RegionWritable();
		regionWritable.readFields(in);

		// Reads in whether contig name != null, and if so, reads in the actual contig name.
		boolean hasContig = in.readBoolean();
		if (hasContig) samRecordContig = in.readUTF();

		samRecordStart = in.readInt();
	}

	@Override
	public int compareTo(RegionWithSortableSamRecordWritable o)
	{
		int c = regionWritable.compareTo(o.regionWritable);
		// null-safe comparison. A null contig name is assumed the highest value.
		if (c == 0) c = ObjectUtils.compare(samRecordContig, o.samRecordContig, true);
		if (c == 0) c = samRecordStart - o.samRecordStart;

		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((regionWritable == null) ? 0 : regionWritable.hashCode());
		result = prime * result + ((samRecordContig == null) ? 0 : samRecordContig.hashCode());
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
		if (samRecordContig == null)
		{
			if (other.samRecordContig != null) return false;
		}
		else if (!samRecordContig.equals(other.samRecordContig)) return false;
		if (samRecordStart != other.samRecordStart) return false;
		return true;
	}

}
