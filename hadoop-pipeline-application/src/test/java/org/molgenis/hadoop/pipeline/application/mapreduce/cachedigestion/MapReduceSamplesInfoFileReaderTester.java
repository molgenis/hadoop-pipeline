package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MapReduceSamplesInfoFileReaderTester extends Tester
{
	/**
	 * The reader that is being tested.
	 */
	private MapReduceSamplesInfoFileReader reader;

	ArrayList<Sample> expectedValidSamples;

	/**
	 * Creates a {@link MapReduceToolsXmlReader} needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		reader = new MapReduceSamplesInfoFileReader(FileSystem.get(new Configuration()));

		expectedValidSamples = new ArrayList<Sample>();
		expectedValidSamples.add(new Sample("SN163", 150616, 648, "AHKYLMADXX", 1));
		expectedValidSamples.add(new Sample("SN163", 150616, 648, "AHKYLMADXX", 2));
		expectedValidSamples.add(new Sample("SN163", 150702, 649, "BHJYNKADXX", 5));
	}

	/**
	 * Test when a samplesheet contains many columns (so including unused ones).
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidSamplesheet() throws IOException
	{
		ArrayList<Sample> actualSamples = reader.read(getClassLoader().getResource("samplesheets/valid.csv").getFile());

		Assert.assertEquals(actualSamples, expectedValidSamples);
	}

	/**
	 * Test when the samplesheet only contains the vital columns with data.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidMinimalSamplesheet() throws IOException
	{
		ArrayList<Sample> actualSamples = reader
				.read(getClassLoader().getResource("samplesheets/valid_minimal.csv").getFile());

		Assert.assertEquals(actualSamples, expectedValidSamples);
	}

	/**
	 * Test when the header of th csv file is missing a field.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testHeaderMissingRunColumn() throws IOException
	{
		reader.read(getClassLoader().getResource("samplesheets/header_missing_run_column.csv").getFile());
	}

	/**
	 * Test when a sample is missing a field.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testSampleMissingRun() throws IOException
	{
		reader.read(getClassLoader().getResource("samplesheets/sample_missing_run.csv").getFile());
	}

	/**
	 * Test when a sample is missing the last field.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testSampleMissingLastField() throws IOException
	{
		reader.read(getClassLoader().getResource("samplesheets/sample_missing_last_field.csv").getFile());
	}

	/**
	 * Test when a sample is missing the last field but with a comma ending the previous field.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testSampleMissingLastFieldEndingWithAComma() throws IOException
	{
		reader.read(getClassLoader().getResource("samplesheets/sample_missing_last_field_comma-ended.csv").getFile());
	}

	/**
	 * Test when the run value and date are swapped (so columns do not have a correct seeming value but is of the same
	 * type). Currently, the expected behavior is not checking on this and it should pass as long as the type of the
	 * value is correct.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testSampleRunAndSequencingStartDateSwapped() throws IOException
	{
		@SuppressWarnings("unchecked")
		ArrayList<Sample> expectedSamples = (ArrayList<Sample>) expectedValidSamples.clone();
		expectedSamples.remove(1);
		expectedSamples.add(1, new Sample("SN163", 648, 150616, "AHKYLMADXX", 2));

		ArrayList<Sample> actualSamples = reader.read(
				getClassLoader().getResource("samplesheets/sample_run_sequencingStartDate_swapped.csv").getFile());

		Assert.assertEquals(actualSamples, expectedSamples);
	}
}
