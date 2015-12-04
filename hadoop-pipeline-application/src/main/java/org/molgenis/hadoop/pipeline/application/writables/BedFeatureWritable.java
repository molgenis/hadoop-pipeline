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
	 * Store a {@link BEDFeature} as {@link Writable}.
	 * 
	 * @param bedFeature
	 *            {@link BEDFeature}
	 */
	public BedFeatureWritable(BEDFeature bedFeature)
	{
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
		int c = this.get().getContig().compareTo(o.get().getContig());
		if (c == 0) c = this.get().getStart() - o.get().getStart();
		if (c == 0) c = this.get().getEnd() - o.get().getEnd();
		return c;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + bedFeature.getContig().hashCode();
		result = prime * result + bedFeature.getStart();
		result = prime * result + bedFeature.getEnd();
		return result;
	}
}
