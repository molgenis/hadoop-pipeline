package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

/**
 * Builder for easy creation of {@link ContigRegionsMap}{@code s}.
 */
public class ContigRegionsMapBuilder
{
	/**
	 * Stores the data that should be added to the {@link ContigRegionsMap}.
	 */
	private Map<String, ArrayList<Region>> contigRegions = new HashMap<>();

	/**
	 * Add multiple {@link Region}{@code s} that should be stored. Returns itself for easy chaining.
	 * 
	 * @param regions
	 *            {@link List}{@code <}{@link Region}{@code >}
	 * @return {@link ContigRegionsMapBuilder}
	 * @see {@link #add(Region)}
	 */
	public ContigRegionsMapBuilder addAll(List<Region> regions)
	{
		for (Region region : regions)
		{
			add(region);
		}

		// Returns itself for easy chaining.
		return this;
	}

	/**
	 * Add a {@link Region} that should be stored. Returns itself for easy chaining.
	 * 
	 * @param region
	 *            {@link Region}
	 * @return {@link ContigRegionsMapBuilder}
	 */
	public ContigRegionsMapBuilder add(Region region)
	{
		ArrayList<Region> regions = contigRegions.get(region.getContig());
		if (regions == null)
		{
			ArrayList<Region> newContigRegion = new ArrayList<>();
			newContigRegion.add(region);
			contigRegions.put(region.getContig(), newContigRegion);
		}
		else
		{
			regions.add(region);
		}

		// Returns itself for easy chaining.
		return this;
	}

	/**
	 * Builds a {@link ContigRegionsMap} with the currently stored {@link Region}{@code s}.
	 * 
	 * @return {@link ContigRegionsMap}
	 */
	public ContigRegionsMap build()
	{
		// Creates the ContigRegionsMap
		ContigRegionsMap map = new ContigRegionsMap();

		// Goes through all stored Regions and adds them to the ContigRegionsMap.
		for (String key : contigRegions.keySet())
		{
			// Retrieve and sort the values of a specific contig.
			ArrayList<Region> regions = contigRegions.get(key);
			Collections.sort(regions);

			// Generate an immutable value list.
			ImmutableList.Builder<Region> builder = new ImmutableList.Builder<>();
			builder.addAll(regions);
			ImmutableList<Region> value = builder.build();

			// Adds a key-value pair to the ContigRegionsMap.
			map.put(key, value);
		}

		// Returns a filled ContigRegionsMap.
		return map;
	}

	/**
	 * Removes the currently stored {@link Region}{@code s}.
	 */
	public void clear()
	{
		contigRegions.clear();
	}
}
