package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

/**
 * {@link Map} that for each contig contains a {@code key} representing the contig name combined with as {@code value}
 * an {@link ImmutableList} with all {@link Region}{@code s} having that contig name. Uses an {@link ImmutableList} to
 * ensure that the {@link Region}{@code s} are sorted and indexable.
 */
public class ContigRegionsMap implements Map<String, ImmutableList<Region>>
{
	/**
	 * The {@link Region} {@link ImmutableList}{@code s} (value), stored per contig (key) in a {@link Map}.
	 */
	private Map<String, ImmutableList<Region>> contigRegions = new HashMap<>();

	@Override
	public int size()
	{
		return contigRegions.size();
	}

	/**
	 * Returns the total number of {@link Region}{@code s} stored.
	 * 
	 * @return {@code int}
	 */
	public int numberOfRegions()
	{
		int nRegions = 0;
		for (List<Region> regions : contigRegions.values())
		{
			nRegions += regions.size();
		}
		return nRegions;
	}

	@Override
	public boolean isEmpty()
	{
		return contigRegions.isEmpty();
	}

	@Override
	public boolean containsKey(Object key)
	{
		return contigRegions.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value)
	{
		return contigRegions.containsValue(value);
	}

	/**
	 * Checks whether a {@link Region} defined by {@code value} is present in any of the contigs.
	 * 
	 * @param region
	 *            {@link Region}
	 * @return {@code boolean} If present, returns {@code true}, otherwise returns {@code false}.
	 */
	public boolean containsRegion(Region region)
	{
		for (List<Region> regions : contigRegions.values())
		{
			if (regions.contains(region))
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public ImmutableList<Region> get(Object key)
	{
		return contigRegions.get(key);
	}

	/**
	 * Adds an {@link ImmutableList} with {@link Region}{@code s} belonging to a specific {@code contig}. If contig
	 * already exists, replaces it. Furthermore, first checks whether all {@link Region}{@code s} in the {@code value}
	 * {@link ImmutableList} have the same contig as defined by the given {@code key} and whether they are sorted.
	 * Checks occur before any {@link Region} is added.
	 * 
	 * @param key
	 *            {@link String}
	 * @param value
	 *            {@link ImmutableList}{@code <}{@link Region}{@code >}
	 * @return {@link ImmutableList}{@code <}{@link Region}{@code >} if contig already exists, otherwise {@code null}.
	 * @throws IllegalArgumentException
	 *             If {@link Region#getContig()} from a single {@link Region} is different compared to the {@code key}
	 *             or if the {@link ImmutableList} is not sorted.
	 * 
	 * @see {@link Map#put(Object, Object)}
	 */
	@Override
	public ImmutableList<Region> put(String key, ImmutableList<Region> value) throws IllegalArgumentException
	{
		// Validates the input. Throws IllegalArgumentException if it fails.
		validateInputKeyValuePair(key, value);

		// Adds the key-value pair and returns the old list (which can be null).
		return contigRegions.put(key, value);
	}

	/**
	 * Remove a contig from the {@link Map}.
	 * 
	 * @param key
	 *            {@link Object}
	 * @return {@link ImmutableList}{@code <}{@link Region}{@code >}
	 * @see {@link Map#remove(Object)}
	 */
	@Override
	public ImmutableList<Region> remove(Object key)
	{
		return contigRegions.remove(key);
	}

	/**
	 * Adds all key-value pairs. Note that all {@link Region}{@code s} need to pass validation before any of the
	 * key-value pairs are added.
	 * 
	 * @throws IllegalArgumentException
	 *             If {@link Region#getContig()} from a single {@link Region} is different compared to the {@code key}
	 *             or if the {@link ImmutableList} is not sorted.
	 */
	@Override
	public void putAll(Map<? extends String, ? extends ImmutableList<Region>> m)
	{
		// Validates all key-value sets whether the Regions match the defined key.
		for (String key : m.keySet())
		{
			validateInputKeyValuePair(key, m.get(key));
		}

		contigRegions.putAll(m);
	}

	@Override
	public void clear()
	{
		contigRegions.clear();
	}

	@Override
	public Set<String> keySet()
	{
		return contigRegions.keySet();
	}

	@Override
	public Collection<ImmutableList<Region>> values()
	{
		return contigRegions.values();
	}

	@Override
	public Set<java.util.Map.Entry<String, ImmutableList<Region>>> entrySet()
	{
		return contigRegions.entrySet();
	}

	/**
	 * Validates whether the {@link Region#getContig()} of all {@link Region}{@code s} is equal to the {@code key}.
	 * 
	 * @param key
	 *            {@link String}
	 * @param value
	 *            {@link List}{@code <}{@link Region}{@code >}
	 * @throws IllegalArgumentException
	 *             If {@link Region#getContig()} from a single {@link Region} is different compared to the {@code key}.
	 */
	private void validateInputKeyValuePair(String key, List<Region> value) throws IllegalArgumentException
	{
		List<Region> invalidRegions = retrieveInvalidRegions(key, value);
		boolean isOrdered = Ordering.natural().isOrdered(value);

		// Throw invalid regions exception.
		if (invalidRegions.size() > 0)
		{
			StringBuilder errString = new StringBuilder();
			errString.append("The following Regions did not match the key:" + System.lineSeparator());
			for (Region invalidRegion : invalidRegions)
			{
				errString.append(invalidRegion.toString());
			}
			throw new IllegalArgumentException(errString.toString());
		}
		// Throw unordered exception.
		else if (!isOrdered)
		{
			throw new IllegalArgumentException("The Regions were not sorted.");
		}
	}

	/**
	 * Creates a {@link List} of {@link Region}{@code s} that do not match the contig defined by the {@code key}
	 * (compared to {@link Region#getContig()}). Returns an empty {@link List} if all {@link Region}{@code s} are valid.
	 * 
	 * @param key
	 *            {@link String}
	 * @param value
	 *            {@link List}{@code <}{@link Region}{@code >}
	 * @return {@link List}{@code <}{@link Region}{@code >}
	 */
	private List<Region> retrieveInvalidRegions(String key, List<Region> value)
	{
		List<Region> invalidRegions = new ArrayList<>();

		for (Region region : value)
		{
			if (!region.getContig().equals(key))
			{
				invalidRegions.add(region);
			}
		}

		return invalidRegions;
	}
}
