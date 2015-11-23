package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

public class HadoopPipelineMapperTester extends Tester
{
	/**
	 * The mapper to be used for testing.
	 */
	private HadoopPipelineMapper mapper;

	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private MapDriver<NullWritable, BytesWritable, Text, SAMRecordWritable> mDriver;

	/**
	 * Stores the fastq file as byte array to be used.
	 */
	private byte[] fastqData;

	/**
	 * Expected results bwa can be generated with a terminal using: {@code bwa mem -p -M
	 * hadoop-pipeline/hadoop-pipeline-application/target/test-classes/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline/hadoop-pipeline-application/target/test-classes/input_fastq/mini_halvade_0_0.fq.gz}
	 */
	private ArrayList<SAMRecord> bwaResults;

	/**
	 * Expected results bwa can be generated with a terminal using:
	 * {@code bwa mem -p -M -R "@RG\tID:5\tPL:illumina\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3"
	 * hadoop-pipeline/hadoop-pipeline-application/target/test-classes/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline/hadoop-pipeline-application/target/test-classes/input_fastq/mini_halvade_0_0.fq.gz}
	 */
	private ArrayList<SAMRecord> bwaResultsWithReadGroupLine;

	/**
	 * {@link SAMFileHeader} for storing sequence information so that {@link SAMRecord#getSAMString()} can be used
	 * (after {@link SAMRecord#setHeader(SAMFileHeader)} is used).
	 */
	private SAMFileHeader samFileHeader;

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		fastqData = readFileAsByteArray("input_fastq_mini/mini_halvade_0_0.fq.gz");
		bwaResults = readSamFile("mini_halvade_0_0-bwa_results.sam");
		bwaResultsWithReadGroupLine = readSamFile("mini_halvade_0_0-bwa_results_withreadlinegroup.sam");

		samFileHeader = new SAMFileHeader();

		// Generates SAMRecordFileheader SequenceDictionary based upon a @SQ tag with:
		// @SQ\tSN:1:20000000-21000000\tLN:1000001
		samFileHeader.setSequenceDictionary(
				new SAMSequenceDictionary(Arrays.asList(new SAMSequenceRecord("1:20000000-21000000", 1000001))));
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		mapper = new HadoopPipelineMapper();
		mDriver = new FileCacheSymlinkMapDriver<NullWritable, BytesWritable, Text, SAMRecordWritable>(mapper);

		addCacheToMapDriver();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when no readgroupline is also given in the input.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapperWithoutReadGroupLine() throws IOException
	{
		mDriver.withInput(NullWritable.get(), new BytesWritable(fastqData));

		List<Pair<Text, SAMRecordWritable>> output = mDriver.run();
		compareSamRecordFields(output, bwaResults);
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a readgroupline is also given in the input.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMapperWithReadGroupLine() throws IOException, URISyntaxException
	{
		mDriver.withInput(NullWritable.get(), new BytesWritable(fastqData));
		mDriver.getConfiguration().set("input_readgroupline",
				"@RG\tID:5\tPL:illumina\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3");

		List<Pair<Text, SAMRecordWritable>> output = mDriver.run();
		compareSamRecordFields(output, bwaResultsWithReadGroupLine);
	}

	/**
	 * Compares the output from the driver with the expected output.
	 * 
	 * @param output
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link Text}{@code , }{@link SAMRecordWritable}{@code >>}
	 * @param expectedResults
	 *            {@link ArrayList}{@code <}{@link SAMRecord}{@code >}
	 */
	private void compareSamRecordFields(List<Pair<Text, SAMRecordWritable>> output,
			ArrayList<SAMRecord> expectedResults)
	{
		// Compares the actual SAMRecords with the expected SAMRecords.
		for (int i = 0; i < output.size(); i++)
		{
			SAMRecord actualSam = output.get(i).getSecond().get();

			// If line below is removed, testMapperWithoutReadGroupLine FAILS due to a
			// "java.lang.IllegalArgumentException: Reference index 0 not found in sequence dictionary."
			// error but testMapperWithReadGroupLine still passes.
			actualSam.setHeader(samFileHeader);

			SAMRecord expectedSam = expectedResults.get(i);

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

	/**
	 * Adds needed files to the {@link MapDriver} chache.
	 * 
	 * @throws URISyntaxException
	 */
	private void addCacheToMapDriver() throws URISyntaxException
	{
		// IMPORTANT: input order defines position in array for retrieval in mapper/reducer!!!
		mDriver.addCacheArchive(getClassLoader().getResource("tools.tar.gz").toURI());

		// IMPORTANT: input order defines position in array for retrieval in mapper/reducer!!!
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.amb").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.ann").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.bwt").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.fai").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.pac").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.sa").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.dict").toURI());
		mDriver.addCacheFile(getClassLoader().getResource("chr1_20000000-21000000.bed").toURI());
	}

}
