package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.molgenis.hadoop.pipeline.application.sequences.AlignedReadPairType;
import org.molgenis.hadoop.pipeline.application.writables.RegionWithSortableSamRecordWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;

/**
 * Tester for {@link HadoopPipelineMapper}. Note that it only tests the key:value outputs of the mapper, so any custom
 * grouping comparators that are used within the whole process are not tested. For tests that include these as well,
 * please refer to the {@link HadoopPipelineMapReduceTester}.
 */
public class HadoopPipelineMapperTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private MapDriver<Text, BytesWritable, RegionWithSortableSamRecordWritable, SAMRecordWritable> mDriver;

	/**
	 * Mini test input dataset.
	 */
	private BytesWritable fastqDataCustom;

	/**
	 * Test input dataset.
	 */
	private BytesWritable fastqDataL1;

	/**
	 * Aligned reads results belonging to the mini test input dataset.
	 */
	private List<SAMRecord> alignedReadsMiniL1;

	/**
	 * Aligned reads results belonging to the test input dataset.
	 */
	private List<SAMRecord> alignedReadsL1;

	/**
	 * A list containing grouping information.
	 */
	private List<Region> regions;

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		fastqDataCustom = new BytesWritable(TestFileReader.readFileAsByteArray(TestFile.FASTQ_DATA_CUSTOM));
		fastqDataL1 = new BytesWritable(TestFileReader.readFileAsByteArray(TestFile.FASTQ_DATA_L1));
		alignedReadsMiniL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_CUSTOM);
		alignedReadsL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_L1);
		regions = TestFileReader.readBedFile(TestFile.GROUPS_SET1);
	}

	/**
	 * Sets large data variables to {@code null} and runs the garbage collector to reduce the amount of memory used
	 * after these tests are done.
	 * 
	 * @throws IOException
	 */
	@AfterClass
	public void AfterClass() throws IOException
	{
		fastqDataCustom = null;
		fastqDataL1 = null;
		alignedReadsMiniL1 = null;
		alignedReadsL1 = null;
		regions = null;
		System.gc();
	}

	/**
	 * Preparations for a single map test.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Mapper<Text, BytesWritable, RegionWithSortableSamRecordWritable, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		mDriver = new FileCacheSymlinkMapDriver<Text, BytesWritable, RegionWithSortableSamRecordWritable, SAMRecordWritable>(
				mapper);
		setDriver(mDriver);

		addCacheToDriver();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} with a few reads to allow for faster bug-fixing if something would go
	 * wrong with the full dataset.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapperRunWithCustomInputData() throws IOException
	{
		// Generate expected output.
		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedResults = generateExpectedMapperOutput(
				alignedReadsMiniL1, regions);

		// Run mapper.
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz"), fastqDataCustom);
		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> output = mDriver.run();

		// Print results
		printOutput(output);

		// Validate output.
		validateOutput(output, expectedResults);

		// Validate enum counters.
		Counters counters = mDriver.getCounters();
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.BOTH_UNMAPPED).getValue(), 1);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.BOTH_MAPPED).getValue(), 8);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.BOTH_MULTIMAPPED).getValue(), 0);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.BOTH_MULTIMAPPED_SUPPLEMENTARY_ONLY).getValue(),
				0);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.ONE_UNMAPPED_ONE_MAPPED).getValue(), 1);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.ONE_UNMAPPED_ONE_MULTIMAPPED).getValue(), 0);
		Assert.assertEquals(
				counters.findCounter(AlignedReadPairType.ONE_UNMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY).getValue(),
				0);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.ONE_MAPPED_ONE_MULTIMAPPED).getValue(), 0);
		Assert.assertEquals(mDriver.getCounters()
				.findCounter(AlignedReadPairType.ONE_MAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY).getValue(), 0);
		Assert.assertEquals(
				counters.findCounter(AlignedReadPairType.ONE_MULTIMAPPED_ONE_MULTIMAPPED_SUPPLEMENTARY_ONLY).getValue(),
				0);
		Assert.assertEquals(counters.findCounter(AlignedReadPairType.INVALID).getValue(), 0);
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given. Currently skipped due to taking quite some
	 * time to finish and mini set should be enough for validation.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidMapperRun() throws IOException
	{
		// Generate expected output.
		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedResults = generateExpectedMapperOutput(
				alignedReadsL1, regions);

		// Run mapper.
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz"), fastqDataL1);
		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> output = mDriver.run();

		// Validate output.
		try
		{
			validateOutput(output, expectedResults);
		}
		// Clears the generated output, even if an exception is thrown.
		finally
		{
			output.clear();
		}
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given that is not present in the samplesheet file.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testMapperWithSingleInvalidDirToSample() throws IOException
	{
		mDriver.withInput(new Text("hdfs/path/to/999999_SN163_0649_BHJYNKADXX_L1/halvade_0_0.fq.gz"), fastqDataL1);

		mDriver.run();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given that does not start with "halvade_".
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testMapperWithSingleInvalidInputFileName() throws IOException
	{
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/prefix_0_0.fq.gz"), fastqDataL1);

		mDriver.run();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given that does not have the expected file type.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapperWithSingleInvalidInputFileType() throws IOException
	{
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.csv"), fastqDataL1);

		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> output = mDriver.run();

		// As the input file "represents" a csv file, it should not be digested and the output should stay empty, but it
		// should not cause an exception either (for when multiple lanes are given as input using a single main
		// directory with subdirectories and the main directory also stores the samplesheet csv file).
		if (!output.isEmpty())
		{
			Assert.fail();
		}
	}

	/**
	 * Compares the output from the driver with the expected output.
	 * 
	 * @param output
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 * @param expectedResults
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	private void validateOutput(List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> output,
			List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedResults)
	{
		Assert.assertEquals(output.size(), expectedResults.size());

		// Sorts data for correct comparison (as actual mapper output key "order" is defined by a Set).
		Comparator<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> comparator = new Comparator<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>>()
		{
			@Override
			public int compare(Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable> o1,
					Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable> o2)
			{
				int c = o1.getFirst().compareTo(o2.getFirst());
				if (c == 0) c = o1.getSecond().get().getReadName().compareTo(o2.getSecond().get().getReadName());
				if (c == 0) c = o1.getSecond().get().getStart() - o2.getSecond().get().getStart();
				return c;
			}
		};
		Collections.sort(output, comparator);
		Collections.sort(expectedResults, comparator);

		// Compares the actual output data with the expected output data.
		for (int i = 0; i < output.size(); i++)
		{
			Assert.assertEquals(output.get(i).getFirst(), expectedResults.get(i).getFirst());

			// Adds a header to the SAMRecords so that getSAMString works again and compares whether this String is
			// equal compared to what is expected. See also the JavaDoc from setHeaderForRecord().
			setHeaderForRecord(output.get(i).getSecond().get());
			Assert.assertEquals(output.get(i).getSecond().get().getSAMString(),
					expectedResults.get(i).getSecond().get().getSAMString());
		}
	}
}
