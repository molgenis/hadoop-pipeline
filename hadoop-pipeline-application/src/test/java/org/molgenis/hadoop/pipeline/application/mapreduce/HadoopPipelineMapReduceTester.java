package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapReduceDriver;
import org.molgenis.hadoop.pipeline.application.partitioners.RegionSamRecordGroupingComparator;
import org.molgenis.hadoop.pipeline.application.writables.RegionSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;

/**
 * Tests the whole MapReduce process as a whole. Includes the {@link HadoopPipelineMapper},
 * {@link HadoopPipelineReducer} and {@link RegionSamRecordGroupingComparator}. IMPORTANT: Tests in this class are
 * disabled until a fix is found for {@link HadoopPipelineReducerJUnitTester#testValidReducerRun()}.
 * 
 * @deprecated Fix needed in {@link HadoopPipelineReducerJUnitTester#testValidReducerRun()}. It might be outdated as
 *             well.
 */
public class HadoopPipelineMapReduceTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapReduceDriver allowing the mapper to be tested.
	 */
	private MapReduceDriver<Text, BytesWritable, RegionSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> mrDriver;

	/**
	 * Mini test input dataset.
	 */
	private BytesWritable fastqDataCustom;

	/**
	 * Aligned reads results belonging to the mini test input dataset.
	 */
	private List<SAMRecord> alignedReadsCustom;

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
		alignedReadsCustom = TestFileReader.readSamFile(TestFile.ALIGNED_READS_CUSTOM);
		regions = TestFileReader.readBedFile(TestFile.GROUPS_SET1);
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@SuppressWarnings("unchecked")
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Mapper<Text, BytesWritable, RegionSamRecordStartWritable, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		Reducer<RegionSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> reducer = new HadoopPipelineReducer();
		mrDriver = new FileCacheSymlinkMapReduceDriver<Text, BytesWritable, RegionSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable>(
				mapper, reducer);

		mrDriver.setKeyGroupingComparator(new RegionSamRecordGroupingComparator());
		setDriver(mrDriver);

		addCacheToDriver();
	}

	/**
	 * Tests the {@link HadoopPipelineMapper} when a single sample is given. Disabled until a fix is found for
	 * {@link HadoopPipelineReducerJUnitTester#testValidReducerRun()}.
	 *
	 * @throws IOException
	 */
	@Test(enabled = false)
	public void testMapReduceJobWithSingleSample() throws IOException
	{
		// TODO: Add test.
	}

	/**
	 * Compares the output from the driver with the expected output. Note that the {@link SAMRecordWritable}{@code s}
	 * from the expected output are ignored during comparison (as the actual output discarded these, but in the expected
	 * data it is still needed for sorting to simulate the "shuffle & sort" phase between the mapper and reducer. This
	 * sorting should be done using {@link #sortMapperOutput(List)} before calling this method.
	 * 
	 * @param output
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link NullWritable}{@code , } {@link SAMRecordWritable}
	 *            {@code >>}
	 * @param expectedResults
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable} {@code >>}
	 */
	private void validateOutput(List<Pair<NullWritable, SAMRecordWritable>> output,
			List<Pair<RegionSamRecordStartWritable, SAMRecordWritable>> expectedResults)
	{
		Assert.assertEquals(output.size(), expectedResults.size());

		// Compares the actual output data with the expected output data.
		for (int i = 0; i < output.size(); i++)
		{
			// Adds a header to the SAMRecords so that getSAMString works again and compares whether this String is
			// equal compared to what is expected. See also the JavaDoc from setHeaderForRecord().
			setHeaderForRecord(output.get(i).getSecond().get());
			Assert.assertEquals(output.get(i).getSecond().get().getSAMString(),
					expectedResults.get(i).getSecond().get().getSAMString());
		}
	}
}
