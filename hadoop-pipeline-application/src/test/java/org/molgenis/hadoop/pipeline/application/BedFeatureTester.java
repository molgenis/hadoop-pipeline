package org.molgenis.hadoop.pipeline.application;

import java.util.ArrayList;

import org.testng.Assert;

import htsjdk.tribble.bed.BEDFeature;

public class BedFeatureTester extends Tester
{
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
