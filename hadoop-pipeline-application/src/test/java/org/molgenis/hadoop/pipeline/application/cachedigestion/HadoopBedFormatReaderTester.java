package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.util.ArrayList;

import org.molgenis.hadoop.pipeline.application.BedFeatureTester;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

/**
 * Tester for {@link HadoopBedFormatFileReader}.
 */
public class HadoopBedFormatReaderTester extends BedFeatureTester
{
	/**
	 * Reader to be tested.
	 */
	private HadoopBedFormatFileReader reader;

	/**
	 * Expected bed valid results.
	 */
	private ArrayList<BEDFeature> expectedValidBed;

	/**
	 * Expected valid results when the 4th line does not have an end value.
	 */
	private ArrayList<BEDFeature> expectedBedNoEndValueForFourthLine;

	@BeforeClass
	public void beforeClass() throws IOException
	{
		reader = new HadoopBedFormatFileReader();

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

	/**
	 * Test when an unsorted BED-formatted file is loaded.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testUnsortedBedFile() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/unsorted_contig-start-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	/**
	 * Test when a BED-formatted file is loaded that has a tab after the end value on the 4th line.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBedFileWithLineEndingWithATab() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end_tab-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	/**
	 * Test a BED-formatted file that has no end value on the 4th line.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBedFileWithLineWithoutEndValue() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start_normal-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedBedNoEndValueForFourthLine);
	}

	/**
	 * Test a BED-formatted file that has no end value on the 4th line, but still has the tab at the end.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBedFileWithLineWithoutEndValueThatEndsWithATab() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start_tab-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedBedNoEndValueForFourthLine);
	}

	/**
	 * Tests a BED-formatted file that only has the contig field on the 4th line.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testBedFileWithLineThatOnlyHasContig() throws IOException
	{
		// Runs file reader.
		reader.read(getClassLoader().getResource("bed_files/line_contig_normal-end.bed").getFile());
	}

	/**
	 * Tests a BED-formatted file that only has the contig field on the 4th line but still has the tab at the end of the
	 * line.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testBedFileWithLineThatOnlyHasContigAndEndsWithATab() throws IOException
	{
		// Runs file reader.
		reader.read(getClassLoader().getResource("bed_files/line_contig_tab-end.bed").getFile());
	}

	/**
	 * Tests a BED-formatted file that contains an additional field on the 4th line.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBedFileWithLineThatAlsoContainsNameField() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end-name_normal-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);
	}

	/**
	 * Tests a BED-formatted file that contains and additional field on the 4th line and also has a tab after that.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBedFileWithLineThatAlsoContainsNameFieldAndEndsWithATab() throws IOException
	{
		// Runs file reader.
		ArrayList<BEDFeature> actualBed = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end-name_tab-end.bed").getFile());

		// Compares actual data with expected data.
		compareActualBedWithExpectedBed(actualBed, expectedValidBed);

	}
}
