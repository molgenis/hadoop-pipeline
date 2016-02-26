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
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapReduceDriver;
import org.molgenis.hadoop.pipeline.application.partitioners.BedFeatureSamRecordGroupingComparator;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

public class HadoopPipelineMapReduceTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapReduceDriver allowing the mapper to be tested.
	 */
	private MapReduceDriver<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> mrDriver;

	/**
	 * Mini test input dataset.
	 */
	private BytesWritable fastqDataMiniL1;

	/**
	 * Aligned reads results belonging to the mini test input dataset.
	 */
	private List<SAMRecord> alignedReadsMiniL1;

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
		alignedReadsMiniL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_MINI_L1);
		groups = TestFileReader.readBedFile(TestFile.GROUPS_SET1);

		generateSamFileHeader();
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
		Mapper<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable> mapper = new HadoopPipelineMapper();
		Reducer<BedFeatureSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> reducer = new HadoopPipelineReducer();
		mrDriver = new FileCacheSymlinkMapReduceDriver<Text, BytesWritable, BedFeatureSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable>(
				mapper, reducer);

		mrDriver.setKeyGroupingComparator(new BedFeatureSamRecordGroupingComparator());
		setDriver(mrDriver);

		addCacheToDriver();
	}

	// /**
	// * Tests the {@link HadoopPipelineMapper} when a single sample is given.
	// *
	// * @throws IOException
	// */
	// @Test
	// public void testMapperWithSingleSample() throws IOException
	// {
	// // Run MapReduce job.
	// mrDriver.withInput(new Text("hdfs/path/to/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz"), fastqDataL1);
	// List<Pair<NullWritable, SAMRecordWritable>> output = mrDriver.run();
	//
	// mrDriver.runTest();
	//
	// // Validate output.
	// validateOutput(output, expectedResults);
	// }
}
