package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
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

public class HadoopPipelineMapperTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private MapDriver<NullWritable, BytesWritable, BedFeatureWritable, SAMRecordWritable> mDriver;

	/**
	 * Stores the fastq file as byte array to be used.
	 */
	private byte[] fastqData;

	/**
	 * The expected mapper output when no readgroupline was used.
	 */
	private List<Pair<BedFeatureWritable, SAMRecordWritable>> expectedMapperResults;

	/**
	 * The expected mapper output when a readgroupline was used.
	 */
	private List<Pair<BedFeatureWritable, SAMRecordWritable>> expectedMapperResultsWithReadGroupLine;

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
		ArrayList<BEDFeature> groups = readBedFile("bed_files/chr1_20000000-21000000.bed");
		expectedMapperResults = generateExpectedMapperOutput(getBwaResults(), groups);
		expectedMapperResultsWithReadGroupLine = generateExpectedMapperOutput(getBwaResultsWithReadGroupLine(), groups);
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Mapper<NullWritable, BytesWritable, BedFeatureWritable, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		mDriver = new FileCacheSymlinkMapDriver<NullWritable, BytesWritable, BedFeatureWritable, SAMRecordWritable>(
				mapper);
		setDriver(mDriver);

		super.beforeMethod();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when no readgroupline is given in the input.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapperWithoutReadGroupLine() throws IOException
	{
		mDriver.withInput(NullWritable.get(), new BytesWritable(fastqData));

		List<Pair<BedFeatureWritable, SAMRecordWritable>> output = mDriver.run();
		sortMapperOutput(output);
		validateOutput(output, expectedMapperResults);
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a readgroupline is also given in the input.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void testMapperWithReadGroupLine() throws IOException, URISyntaxException
	{
		mDriver.withInput(NullWritable.get(), new BytesWritable(fastqData));
		mDriver.getConfiguration().set("input_readgroupline",
				"@RG\tID:5\tPL:illumina\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3");

		List<Pair<BedFeatureWritable, SAMRecordWritable>> output = mDriver.run();
		sortMapperOutput(output);
		validateOutput(output, expectedMapperResultsWithReadGroupLine);
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
