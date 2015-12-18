package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

/**
 * Tester for {@link HadoopPipelineMapper}.
 */
public class HadoopPipelineMapperTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private MapDriver<Text, BytesWritable, BedFeatureWritable, SAMRecordWritable> mDriver;

	/**
	 * Stores the fastq file as byte array to be used.
	 */
	private byte[] fastqData;

	/**
	 * The available groups that results can be grouped into.
	 */
	private ArrayList<BEDFeature> groups;

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		super.beforeClass();
		fastqData = readFileAsByteArray("input_fastq_mini/mini_halvade_0_0.fq.gz");
		groups = readBedFile("bed_files/chr1_20000000-21000000.bed");
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Mapper<Text, BytesWritable, BedFeatureWritable, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		mDriver = new FileCacheSymlinkMapDriver<Text, BytesWritable, BedFeatureWritable, SAMRecordWritable>(mapper);
		setDriver(mDriver);

		super.beforeMethod();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void testMapperWithSingleSample() throws IOException, URISyntaxException
	{
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L2/halvade_0_0.fq.gz"),
				new BytesWritable(fastqData));

		List<Pair<BedFeatureWritable, SAMRecordWritable>> output = mDriver.run();
		sortMapperOutput(output);
		validateOutput(output, generateExpectedMapperOutput(getBwaResultsL2(), groups));
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given that has an incorrect last directory name.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testMapperWithSingleInvalidDirToSample() throws IOException, URISyntaxException
	{
		mDriver.withInput(new Text("hdfs/path/to/999999_SN163_0648_AHKYLMADXX_L2/halvade_0_0.fq.gz"),
				new BytesWritable(fastqData));

		mDriver.run();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given that does not start with "halvade_".
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testMapperWithSingleInvalidInputFileName() throws IOException, URISyntaxException
	{
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L2/prefix_0_0.fq.gz"),
				new BytesWritable(fastqData));

		mDriver.run();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given that does not have the expected file type.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void testMapperWithSingleInvalidInputFileType() throws IOException, URISyntaxException
	{
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L2/halvade_0_0.csv"),
				new BytesWritable(fastqData));

		List<Pair<BedFeatureWritable, SAMRecordWritable>> output = mDriver.run();

		// As the input file "represents" a csv file, it should not be digested and the output should stay empty.
		if (!output.isEmpty())
		{
			Assert.fail();
		}
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when two samples is given.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void testMapperWithMultipleSamples() throws IOException, URISyntaxException
	{
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L2/halvade_0_0.fq.gz"),
				new BytesWritable(fastqData));
		mDriver.withInput(new Text("hdfs/path/to/150702_SN163_0649_BHJYNKADXX_L5/halvade_0_0.fq.gz"),
				new BytesWritable(fastqData));

		List<Pair<BedFeatureWritable, SAMRecordWritable>> output = mDriver.run();
		sortMapperOutput(output);

		validateOutput(output, generateExpectedMapperOutput(getAllBwaResults(), groups));
	}

	/**
	 * Generates the expected output data.
	 * 
	 * @param bwaOutput
	 *            {@link ArrayList}{@code <}{@link SAMRecord}{@code >} bwa output used to base expected output on.
	 * @param groups
	 *            {@link ArrayList}{@code <}{@link BEDFeature}{@code >} groups used for defining keys.
	 * @return {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureWritable}{@code , }{@link SAMRecordWritable}
	 *         {@code >>}
	 */
	private List<Pair<BedFeatureWritable, SAMRecordWritable>> generateExpectedMapperOutput(
			ArrayList<SAMRecord> bwaOutput, ArrayList<BEDFeature> groups)
	{
		List<Pair<BedFeatureWritable, SAMRecordWritable>> expectedMapperOutput = new ArrayList<Pair<BedFeatureWritable, SAMRecordWritable>>();

		for (SAMRecord record : bwaOutput)
		{
			for (BEDFeature group : groups)
			{
				if ((record.getStart() >= group.getStart() && record.getStart() <= group.getEnd())
						|| (record.getStart() >= group.getStart() && record.getStart() <= group.getEnd()))
				{
					SAMRecordWritable writable = new SAMRecordWritable();
					writable.set(record);
					expectedMapperOutput.add(
							new Pair<BedFeatureWritable, SAMRecordWritable>(new BedFeatureWritable(group), writable));
				}
			}
		}

		sortMapperOutput(expectedMapperOutput);

		return expectedMapperOutput;
	}

	/**
	 * Sorts Mapper output data (either actual output data or simulated data which can function as expected output) for
	 * easier comparison.
	 * 
	 * @param mapperData
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	private void sortMapperOutput(List<Pair<BedFeatureWritable, SAMRecordWritable>> mapperData)
	{
		Collections.sort(mapperData, new Comparator<Pair<BedFeatureWritable, SAMRecordWritable>>()
		{
			@Override
			public int compare(Pair<BedFeatureWritable, SAMRecordWritable> o1,
					Pair<BedFeatureWritable, SAMRecordWritable> o2)
			{
				// Sorts on the key contig name first.
				int c = o1.getFirst().get().getContig().compareTo(o2.getFirst().get().getContig());
				// If there is no difference, uses the key start position to sort.
				if (c == 0) c = o1.getFirst().get().getStart() - o2.getFirst().get().getStart();
				// If there is no difference, uses the key end position to sort.
				if (c == 0) c = o1.getFirst().get().getEnd() - o2.getFirst().get().getEnd();
				// If there is no difference in key String, uses the SAMRecord start value to sort.
				if (c == 0) c = o1.getSecond().get().getStart() - o2.getSecond().get().getStart();
				// If above comparisons do not differ, uses the SAMRecord end value to sort.
				if (c == 0) c = o1.getSecond().get().getEnd() - o2.getSecond().get().getEnd();
				return c;
			}
		});
	}

	/**
	 * Compares the output from the driver with the expected output.
	 * 
	 * @param output
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 * @param expectedResults
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	private void validateOutput(List<Pair<BedFeatureWritable, SAMRecordWritable>> output,
			List<Pair<BedFeatureWritable, SAMRecordWritable>> expectedResults)
	{
		Assert.assertEquals(output.size(), expectedResults.size());

		// Compares the actual output data with the expected output data.
		for (int i = 0; i < output.size(); i++)
		{
			validateKeyString(output.get(i).getFirst().get(), expectedResults.get(i).getFirst().get());
			validateSamRecordFields(output.get(i).getSecond().get(), expectedResults.get(i).getSecond().get());
		}
	}

	/**
	 * Compares the key of a single mapper output item with its expected key.
	 * 
	 * @param actualKey
	 *            {@link BEDFeature}
	 * @param expectedKey
	 *            {@link BEDFeature}
	 */
	private void validateKeyString(BEDFeature actualKey, BEDFeature expectedKey)
	{
		Assert.assertEquals(actualKey.getContig(), expectedKey.getContig());
		Assert.assertEquals(actualKey.getStart(), expectedKey.getStart());
		Assert.assertEquals(actualKey.getEnd(), expectedKey.getEnd());
	}

	/**
	 * Compares the value of a single mapper output item with its expected value.
	 * 
	 * @param actualSam
	 *            {@link SAMRecord}
	 * @param expectedSam
	 *            {@link SAMRecord}
	 */
	private void validateSamRecordFields(SAMRecord actualSam, SAMRecord expectedSam)
	{
		// If line below is removed, testMapperWithoutReadGroupLine FAILS due to a
		// "java.lang.IllegalArgumentException: Reference index 0 not found in sequence dictionary."
		// error but testMapperWithReadGroupLine still passes.
		actualSam.setHeader(getSamFileHeader());

		Assert.assertEquals(actualSam.getSAMString(), expectedSam.getSAMString());

		// Compares the header readGroups.
		for (int j = 0; j < actualSam.getHeader().getReadGroups().size(); j++)
		{
			Assert.assertEquals(actualSam.getHeader().getReadGroups().get(j).getId(),
					expectedSam.getHeader().getReadGroups().get(j).getId());
			Assert.assertEquals(actualSam.getHeader().getReadGroups().get(j).getPlatform(),
					expectedSam.getHeader().getReadGroups().get(j).getPlatform());
			Assert.assertEquals(actualSam.getHeader().getReadGroups().get(j).getLibrary(),
					expectedSam.getHeader().getReadGroups().get(j).getLibrary());
			Assert.assertEquals(actualSam.getHeader().getReadGroups().get(j).getSample(),
					expectedSam.getHeader().getReadGroups().get(j).getSample());
		}

		// Compares the header programRecords. If program version differs, the test data was
		// generated with a different bwa version then is used with the tests!
		for (int j = 0; j < actualSam.getHeader().getProgramRecords().size(); j++)
		{
			Assert.assertEquals(actualSam.getHeader().getProgramRecords().get(j).getId(),
					expectedSam.getHeader().getProgramRecords().get(j).getProgramName());
			Assert.assertEquals(actualSam.getHeader().getProgramRecords().get(j).getId(),
					expectedSam.getHeader().getProgramRecords().get(j).getProgramName());
			Assert.assertEquals(actualSam.getHeader().getProgramRecords().get(j).getProgramVersion(),
					expectedSam.getHeader().getProgramRecords().get(j).getProgramVersion());
		}
	}
}
