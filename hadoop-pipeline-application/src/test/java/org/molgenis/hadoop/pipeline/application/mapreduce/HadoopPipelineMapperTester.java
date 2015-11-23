package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;

public class HadoopPipelineMapperTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private MapDriver<NullWritable, BytesWritable, Text, SAMRecordWritable> mDriver;

	/**
	 * Stores the fastq file as byte array to be used.
	 */
	private byte[] fastqData;

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
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Mapper<NullWritable, BytesWritable, Text, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		mDriver = new FileCacheSymlinkMapDriver<NullWritable, BytesWritable, Text, SAMRecordWritable>(mapper);
		setDriver(mDriver);

		super.beforeMethod();
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
		compareSamRecordFields(output, getBwaResults());
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
		compareSamRecordFields(output, getBwaResultsWithReadGroupLine());
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
			actualSam.setHeader(getSamFileHeader());

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
}
