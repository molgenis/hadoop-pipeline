package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;

/**
 * {@link Writable} for storing a {@link Region}.
 */
public class RegionWritable implements WritableComparable<RegionWritable>
{
	/**
	 * Stores the {@link Region}.
	 */
	private Region region;

	public Region get()
	{
		return region;
	}

	/**
	 * Create an empty {@link RegionWritable} instance. Otherwise a Hadoop job will throw the following
	 * {@link Exception}:
	 * 
	 * <pre>
	 * java.lang.NoSuchMethodException: org.molgenis.hadoop.pipeline.application.writables.RegionWritable.&lt;init&gt;()
	 * </pre>
	 */
	public RegionWritable()
	{
	}

	/**
	 * Store a {@link Region} as {@link Writable}.
	 * 
	 * @param region
	 *            {@link Region}
	 */
	public RegionWritable(Region region)
	{
		// Validates individual fields as htsjdk.samtools.util.Locatable allows for null values in the specification.
		this.region = requireNonNull(region);
	}

	@Override
	public String toString()
	{
		return "RegionWritable [region=" + region + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(region.getContig());
		out.writeInt(region.getStart());
		out.writeInt(region.getEnd());
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		String name = in.readUTF();
		int start = in.readInt();
		int end = in.readInt();
		region = new Region(name, start, end);
	}

	@Override
	public int compareTo(RegionWritable o)
	{
		return region.compareTo(o.region);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((region == null) ? 0 : region.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		RegionWritable other = (RegionWritable) obj;
		if (region == null)
		{
			if (other.region != null) return false;
		}
		else if (!region.equals(other.region)) return false;
		return true;
	}
}
