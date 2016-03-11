package org.molgenis.hadoop.pipeline.application.cachedigestion;

import static java.util.Objects.requireNonNull;

import htsjdk.samtools.util.Locatable;
import htsjdk.tribble.bed.SimpleBEDFeature;

/**
 * A region containing a contig name, start position and end position. Due to the addition {@link #hashCode()},
 * {@link #equals(Object)} and {@link #compareTo(Region)} this has not been made a subclass of {@link SimpleBEDFeature}.
 * As these fields are of vital importance for comparison of regions, they should only use the contig, start and end
 * fields. However, this would cause other fields of {@link SimpleBEDFeature} to be ignored (while still callable if
 * this was a subclass), making non-identical instances appear identical. As it is only a simple container class, it is
 * kept separate.
 */
public class Region implements Locatable, Comparable<Region>
{
	/**
	 * The contig name.
	 */
	String contig;

	/**
	 * The 1-based inclusive start position.
	 */
	int start;

	/**
	 * The 1-based inclusive end position.
	 */
	int end;

	/**
	 * The contig name of the region. Is stricter compared to {@link Locatable} as field may not be {@code null}.
	 * 
	 * @return {@link String} contig name of this region
	 */
	@Override
	public String getContig()
	{
		return contig;
	}

	/**
	 * The contig name of the region. Is stricter compared to {@link Locatable} as field may not be {@code null}.
	 * 
	 * @param contig
	 *            {@link String}
	 */
	public void setContig(String contig)
	{
		this.contig = requireNonNull(contig);
	}

	/**
	 * The contig start of the region. Is stricter compared to {@link Locatable} as field may not be {@code null}.
	 * 
	 * @return {@code int} 1-based start position
	 */
	@Override
	public int getStart()
	{
		return start;
	}

	/**
	 * The contig start of the region. Is stricter compared to {@link Locatable} as field may not be {@code null}.
	 */
	public void setStart(int start)
	{
		this.start = requireNonNull(start);
	}

	/**
	 * The contig end of the region. Is stricter compared to {@link Locatable} as field may not be {@code null}.
	 * 
	 * @return {@code int} 1-based closed-ended position
	 */
	@Override
	public int getEnd()
	{
		return end;
	}

	/**
	 * The contig end of the region. Is stricter compared to {@link Locatable} as field may not be {@code null}.
	 */
	public void setEnd(int end)
	{
		this.end = requireNonNull(end);
	}

	/**
	 * Generates a new region. Note that in contrary to {@link Locatable}, all fields are expected to not be
	 * {@code null}!
	 * 
	 * @param contig
	 *            {@link String} - The contig name.
	 * @param start
	 *            {@code int} - 1-based inclusive position.
	 * @param end
	 *            {@code int} - 1-based inclusive position.
	 */
	public Region(String contig, int start, int end)
	{
		this.contig = requireNonNull(contig);
		this.start = requireNonNull(start);
		this.end = requireNonNull(end);
	}

	@Override
	public String toString()
	{
		return "Region [contig=" + contig + ", start=" + start + ", end=" + end + "]";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((contig == null) ? 0 : contig.hashCode());
		result = prime * result + end;
		result = prime * result + start;
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Region other = (Region) obj;
		if (contig == null)
		{
			if (other.contig != null) return false;
		}
		else if (!contig.equals(other.contig)) return false;
		if (end != other.end) return false;
		if (start != other.start) return false;
		return true;
	}

	@Override
	public int compareTo(Region o)
	{
		int c = contig.compareTo(o.contig);
		if (c == 0) c = start - o.start;
		if (c == 0) c = end - o.end;

		return c;
	}
}
