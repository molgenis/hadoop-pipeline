package org.molgenis.hadoop.pipeline.application.writables;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

/**
 * Writable storing the most vital fields of an {@link BEDFeature} that are needed for rebuilding it.
 */
public class BedFeatureWritable implements Writable
{
	/**
	 * Represents {@link BEDFeature#getContig()}.
	 */
	private Text name;

	/**
	 * Represents {@link BEDFeature#getStart()}.
	 */
	private IntWritable start;

	/**
	 * Represents {@link BEDFeature#getEnd()}.
	 */
	private IntWritable end;

	public String getName()
	{
		return name.toString();
	}

	public int getStart()
	{
		return start.get();
	}

	public int getEnd()
	{
		return end.get();
	}

	/**
	 * Digests the stored fields to generate a {@link BEDFeature} instance.
	 * 
	 * @return {@link BEDFeature}
	 */
	public BEDFeature getBedFeature()
	{
		return new FullBEDFeature(name.toString(), start.get(), end.get());
	}

	/**
	 * Create an empty {@link BedFeatureWritable} instance.
	 */
	public BedFeatureWritable()
	{
		name = new Text();
		start = new IntWritable();
		end = new IntWritable();
	}

	/**
	 * Create a new {@link BedFeatureWritable} instance that represents a {@link BEDFeature} using the given fields.
	 * 
	 * @param name
	 *            {@link String}
	 * @param start
	 *            {@code int}
	 * @param end
	 *            {@code int}
	 */
	public BedFeatureWritable(String name, int start, int end)
	{
		this.name = new Text(requireNonNull(name));
		this.start = new IntWritable(requireNonNull(start));
		this.end = new IntWritable(requireNonNull(end));
	}

	/**
	 * Create a new {@link BedFeatureWritable} instance.
	 * 
	 * @param feature
	 *            {@link BEDFeature}
	 */
	public BedFeatureWritable(BEDFeature feature)
	{
		this.name = new Text(requireNonNull(feature.getName()));
		this.start = new IntWritable(requireNonNull(feature.getStart()));
		this.end = new IntWritable(requireNonNull(feature.getEnd()));
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		name.write(out);
		start.write(out);
		end.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		name.readFields(in);
		start.readFields(in);
		end.readFields(in);
	}
}
