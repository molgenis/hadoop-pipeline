package org.molgenis.hadoop.pipeline.application;

import java.util.ArrayList;

import org.testng.Assert;

import htsjdk.tribble.bed.BEDFeature;

/**
 * Superclass for testers using BED files containing generic code.
 */
public class BedFeatureTester extends Tester
{
	/**
	 * Compares two {@link ArrayList}{@code s} with {@link BEDFeature}{@code s} using
	 * {@link Assert#assertEquals(Object[], Object[])}.
	 * 
	 * @param actualBed
	 *            {@link ArrayList}{@code <}{@link BEDFeature}{@code >}
	 * @param expectedBed
	 *            {@link ArrayList}{@code <}{@link BEDFeature}{@code >}
	 */
	protected void compareActualBedWithExpectedBed(ArrayList<BEDFeature> actualBed, ArrayList<BEDFeature> expectedBed)
	{
		// Compares expected data with actual data.
		Assert.assertEquals(actualBed.size(), expectedBed.size());
		for (int i = 0; i < actualBed.size(); i++)
		{
			Assert.assertEquals(actualBed.get(i).getContig(), expectedBed.get(i).getContig());
			Assert.assertEquals(actualBed.get(i).getStart(), expectedBed.get(i).getStart());
			Assert.assertEquals(actualBed.get(i).getEnd(), expectedBed.get(i).getEnd());
		}
	}
}
