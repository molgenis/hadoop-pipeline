package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

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
	private MapDriver<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable> mDriver;

	/**
	 * Mini test input dataset.
	 */
	private BytesWritable fastqDataMiniL1;

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
	private List<BEDFeature> groups;

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		fastqDataMiniL1 = new BytesWritable(TestFileReader.readFileAsByteArray(TestFile.FASTQ_DATA_MINI_L1));
		fastqDataL1 = new BytesWritable(TestFileReader.readFileAsByteArray(TestFile.FASTQ_DATA_L1));
		alignedReadsMiniL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_MINI_L1);
		alignedReadsL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_L1);
		groups = TestFileReader.readBedFile(TestFile.GROUPS_SET1);

		generateSamFileHeader();
	}

	/**
	 * Preparations for a single map test.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Mapper<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		mDriver = new FileCacheSymlinkMapDriver<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable>(
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
	public void testValidMapperRunWithMiniInputData() throws IOException
	{
		// Generate expected output.
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> expectedResults = generateExpectedMapperOutput(
				alignedReadsMiniL1, groups);

		// Run mapper.
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz"), fastqDataMiniL1);
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> output = mDriver.run();

		// Validate output.
		validateOutput(output, expectedResults);
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testValidMapperRun() throws IOException
	{
		// Generate expected output.
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> expectedResults = generateExpectedMapperOutput(
				alignedReadsL1, groups);

		// Run mapper.
		mDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz"), fastqDataL1);
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> output = mDriver.run();

		// Validate output.
		validateOutput(output, expectedResults);
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

		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> output = mDriver.run();

		// As the input file "represents" a csv file, it should not be digested and the output should stay empty, but it
		// should not cause an exception either (for when multiple lanes are given as input using a single main
		// directory with subdirectories and the main directory also stores the samplesheet csv file).
		if (!output.isEmpty())
		{
			Assert.fail();
		}
	}

	/**
	 * Generates the expected output data.
	 * 
	 * @param bwaOutput
	 *            {@link List}{@code <}{@link SAMRecord}{@code >} bwa output used to base expected output on.
	 * @param groups
	 *            {@link List}{@code <}{@link BEDFeature}{@code >} groups used for defining keys.
	 * @return {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *         {@link SAMRecordWritable} {@code >>}
	 */
	private List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> generateExpectedMapperOutput(
			List<SAMRecord> bwaOutput, List<BEDFeature> groups)
	{
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> expectedMapperOutput = new ArrayList<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>>();

		for (SAMRecord record : bwaOutput)
		{
			for (BEDFeature group : groups)
			{
				if (record.getContig().equals(group.getContig())
						&& ((record.getStart() >= group.getStart() && record.getStart() <= group.getEnd())
								|| (record.getEnd() >= group.getStart() && record.getEnd() <= group.getEnd())))
				{
					SAMRecordWritable writable = new SAMRecordWritable();
					writable.set(record);
					expectedMapperOutput.add(new Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>(
							new BedFeatureSamRecordStartWritable(group, record), writable));
				}
			}
		}

		return expectedMapperOutput;
	}

	/**
	 * Writes the pairs to stdout for manual validation.
	 * 
	 * @param pairsList
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	private void printOutput(List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> pairsList)
	{
		printOutput(pairsList, pairsList.size());
	}

	/**
	 * Writes the pairs to stdout for manual validation.
	 * 
	 * @param pairsList
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 * @param limit
	 *            {@code int} the number of pairs to write to stdout.
	 */
	private void printOutput(List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> pairsList, int limit)
	{
		// If the limit is higher than the actual list size, resets the limit.
		if (limit > pairsList.size()) limit = pairsList.size();

		System.out.format("%-36s%s%n", "region", "SAMRecord");
		System.out.format("%-8s %-8s %-8s%n", "contig", "start", "end");

		// Prints the results.
		for (int i = 0; i < limit; i++)
		{
			BEDFeature group = pairsList.get(i).getFirst().get();
			SAMRecord record = pairsList.get(i).getSecond().get();
			setHeaderForRecord(record);

			System.out.format("%8s:%8d:%8d%10s%s%n", group.getContig(), group.getStart(), group.getEnd(), "",
					record.getSAMString().trim());
		}
	}

	/**
	 * Compares the output from the driver with the expected output.
	 * 
	 * @param output
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 * @param expectedResults
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	private void validateOutput(List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> output,
			List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> expectedResults)
	{
		Assert.assertEquals(output.size(), expectedResults.size());

		// Compares the actual output data with the expected output data.
		for (int i = 0; i < output.size(); i++)
		{
			// assert on BEDFeature would compare whether memory address is the same.
			Assert.assertEquals(output.get(i).getFirst(), expectedResults.get(i).getFirst());

			// assert on SAMRecord SAMString as SAMRecordWritable removes certain data from each record (such as the
			// header), making it incomplete and the comparison fail. The SAMString is the vital part that is used when
			// generating the output files and I/O PipeRunner processes, so should be valid. The header information is
			// generated using distributed cache data within the reducer/output writer, so mapper output is expected to
			// be missing this header information.
			setHeaderForRecord(output.get(i).getSecond().get());
			Assert.assertEquals(output.get(i).getSecond().get().getSAMString(),
					expectedResults.get(i).getSecond().get().getSAMString());
		}
	}
}
