package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.exceptions.SinkIOException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

public class MapReduceBedFormatReaderTester extends Tester
{
	private MapReduceBedFormatFileReader reader;
	private ArrayList<BEDFeature> expectedValidBed;
	private ArrayList<BEDFeature> expectedBedNoEndValueForFourthLine;

	@BeforeClass
	public void beforeClass() throws IOException
	{
		reader = new MapReduceBedFormatFileReader(FileSystem.get(new Configuration()));

		// IMPORTANT:
		// BED-format is 0-based, start is inclusive, end is exclusive!
		// BEDFeature is 1-based, start is inclusive, end is inclusive!
		expectedValidBed = new ArrayList<BEDFeature>();
		expectedValidBed.add(new FullBEDFeature("1", 1, 200000));
		expectedValidBed.add(new FullBEDFeature("1", 200001, 400000));
		expectedValidBed.add(new FullBEDFeature("1", 400001, 600000));
		expectedValidBed.add(new FullBEDFeature("1", 600001, 800000));
		expectedValidBed.add(new FullBEDFeature("1", 800001, 1000000));

		// Creates a different expected ArrayList when the 4th line does not contain an end value.
		expectedBedNoEndValueForFourthLine = new ArrayList<BEDFeature>(expectedValidBed);
		expectedBedNoEndValueForFourthLine.set(3, new FullBEDFeature("1", 600001, 600001));

	}

	/**
	 * Tests whether a BED-formatted file is loaded correctly (identical to input file).
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidBedFile() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/chr1_20000000-21000000.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	@Test
	public void testUnsortedBedFile() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/unsorted_contig-start-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	@Test
	public void testBedFileWithLineEndingWithATab() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end_tab-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	@Test
	public void testBedFileWithLineWithoutEndValue() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start_normal-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedBedNoEndValueForFourthLine);
	}

	@Test
	public void testBedFileWithLineWithoutEndValueThatEndsWithATab() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start_tab-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedBedNoEndValueForFourthLine);
	}

	@Test(expectedExceptions = SinkIOException.class)
	public void testBedFileWithLineThatOnlyHasContig() throws IOException
	{
		// Runs file reader.
		reader.read(getClassLoader().getResource("bed_files/line_contig_normal-end.bed").getFile());
	}

	@Test(expectedExceptions = SinkIOException.class)
	public void testBedFileWithLineThatOnlyHasContigAndEndsWithATab() throws IOException
	{
		// Runs file reader.
		reader.read(getClassLoader().getResource("bed_files/line_contig_tab-end.bed").getFile());
	}

	@Test
	public void testBedFileWithLineThatAlsoContainsNameField() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end-name_normal-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	@Test
	public void testBedFileWithLineThatAlsoContainsNameFieldAndEndsWithATab() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end-name_tab-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);

	}

	private void compareActualBedWithExpectedBed(ArrayList<BEDFeature> actualBed, ArrayList<BEDFeature> expectedBed)
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
