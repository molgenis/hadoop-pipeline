package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.util.ArrayList;

import org.molgenis.hadoop.pipeline.application.Tester;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tester for {@link HadoopBedFormatFileReader}.
 */
public class HadoopBedFormatReaderTester extends Tester
{
	/**
	 * Reader to be tested.
	 */
	private HadoopBedFormatFileReader reader;

	/**
	 * Regions that are expected as output.
	 */
	private ContigRegionsMap expectedRegionsMap;

	/**
	 * egions that are expected as output when the 4th line of the bed file does not have an end value.
	 */
	private ContigRegionsMap expectedRegionsMapNoEndValueForFourthLine;

	@BeforeClass
	public void beforeClass() throws IOException
	{
		ContigRegionsMapBuilder builder = new ContigRegionsMapBuilder();

		reader = new HadoopBedFormatFileReader();

		// IMPORTANT:
		// BED-format is 0-based, start is inclusive, end is exclusive!
		// BEDFeature is 1-based, start is inclusive, end is inclusive!
		// Region is generated from a BEDFeature.
		ArrayList<Region> expectedRegions = new ArrayList<Region>();
		expectedRegions.add(new Region("1", 1, 200000));
		expectedRegions.add(new Region("1", 200001, 400000));
		expectedRegions.add(new Region("1", 400001, 600000));
		expectedRegions.add(new Region("1", 600001, 800000));
		expectedRegions.add(new Region("1", 800001, 1000000));
		expectedRegionsMap = builder.addAndBuild(expectedRegions);

		// Creates a different expected ArrayList when the 4th line does not contain an end value.
		ArrayList<Region> expectedRegionsNoEndValueForFourthLine = new ArrayList<Region>(expectedRegions);
		expectedRegionsNoEndValueForFourthLine.set(3, new Region("1", 600001, 600001));
		expectedRegionsMapNoEndValueForFourthLine = builder.addAndBuild(expectedRegionsNoEndValueForFourthLine);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/chr1_20000000-21000000.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMap);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/unsorted_contig-start-end.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMap);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end_tab-end.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMap);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start_normal-end.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMapNoEndValueForFourthLine);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start_tab-end.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMapNoEndValueForFourthLine);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end-name_normal-end.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMap);
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
		ContigRegionsMap actualRegionsMap = reader
				.read(getClassLoader().getResource("bed_files/line_contig-start-end-name_tab-end.bed").getFile());

		// Compares actual data with expected data.
		Assert.assertEquals(actualRegionsMap, expectedRegionsMap);

	}
}
