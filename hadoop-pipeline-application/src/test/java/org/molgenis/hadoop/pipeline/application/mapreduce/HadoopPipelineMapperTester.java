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
 * Tester for {@link HadoopPipelineMapper}.
 */
public class HadoopPipelineMapperTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private MapDriver<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable> mDriver;

	/**
	 * The fastq data to be digested (test set L1).
	 */
	private BytesWritable fastqDataL1;

	/**
	 * The SAM data when L1 is aligned using: /path/to/bwa mem -p -M -R
	 * "@RG\tID:1\tPL:illumina\tLB:150616_SN163_648_AHKYLMADXX_L1\tSM:sample1"
	 * hadoop-pipeline-application/src/test/resources/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline-application/src/test/resources/three_samples/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz >
	 * hadoop-pipeline-application/src/test/resources/expected_bwa_outputs/output_L1.sam
	 */
	private List<SAMRecord> alignedReadsL1;

	/**
	 * A list containing grouping information.
	 */
	private List<BEDFeature> groupsSet1;

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		super.beforeClass();

		fastqDataL1 = new BytesWritable(
				readFileAsByteArray("input_fastq/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz"));
		alignedReadsL1 = readSamFile("expected_bwa_outputs/output_L1.sam");
		groupsSet1 = readBedFile("bed_files/chr1_20000000-21000000.bed");
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
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

		super.beforeMethod();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapperWithSingleSample() throws IOException
	{
		// Generate expected output.
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> expectedResults = generateExpectedMapperOutput(
				alignedReadsL1, groupsSet1);

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
