package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

/**
 * Writable storing the most vital fields of an {@link BEDFeature} that are needed for rebuilding it.
 */
public class BedFeatureWritable implements WritableComparable<BedFeatureWritable>
{
	/**
	 * Stores the {@link BEDFeature}.
	 */
	private BEDFeature bedFeature;

	public BEDFeature get()
	{
		return bedFeature;
	}

	/**
	 * Create an empty {@link BedFeatureWritable} instance. Otherwise a Hadoop job will throw the following
	 * {@link Exception}:
	 * 
	 * <pre>
	 * java.lang.NoSuchMethodException: org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable.&lt;init&gt;()
	 * </pre>
	 */
	public BedFeatureWritable()
	{
	}

	/**
	 * Store a {@link BEDFeature} as {@link Writable}. Unlike defined in {@link htsjdk.samtools.util.Locatable}, this
	 * means the implementation of {@link BEDFeature#getContig()} may not return {@code null}!
	 * 
	 * @param bedFeature
	 *            {@link BEDFeature}
	 */
	public BedFeatureWritable(BEDFeature bedFeature)
	{
		// Validates individual fields as htsjdk.samtools.util.Locatable allows for null values in the specification.
		requireNonNull(bedFeature.getContig());
		requireNonNull(bedFeature.getStart());
		requireNonNull(bedFeature.getEnd());
		this.bedFeature = bedFeature;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(bedFeature.getContig());
		out.writeInt(bedFeature.getStart());
		out.writeInt(bedFeature.getEnd());
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		String name = in.readUTF();
		int start = in.readInt();
		int end = in.readInt();
		bedFeature = new FullBEDFeature(name, start, end);
	}

	@Override
	public int compareTo(BedFeatureWritable o)
	{
		int c = bedFeature.getContig().compareTo(o.bedFeature.getContig());
		if (c == 0) c = bedFeature.getStart() - o.bedFeature.getStart();
		if (c == 0) c = bedFeature.getEnd() - o.bedFeature.getEnd();
		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bedFeature.getContig() == null) ? 0 : bedFeature.getContig().hashCode());
		result = prime * result + bedFeature.getStart();
		result = prime * result + bedFeature.getEnd();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		BedFeatureWritable other = (BedFeatureWritable) obj;
		if (bedFeature.getContig() == null)
		{
			if (other.bedFeature.getContig() != null) return false;
		}
		else if (!bedFeature.getContig().equals(other.bedFeature.getContig())) return false;
		if (bedFeature.getStart() != other.bedFeature.getStart()) return false;
		if (bedFeature.getEnd() != other.bedFeature.getEnd()) return false;
		return true;
	}
}
