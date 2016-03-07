package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.molgenis.hadoop.pipeline.application.DistributedCacheHandler;
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkReduceDriver;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

/**
 * As {@link MultipleOutputs} requires {@code @RunWith(}{@link PowerMockRunner}{@code .class}) and did not work (after
 * some initial efforts) using TestNG (see https://issues.apache.org/jira/browse/MRUNIT-213), JUnit was used for this
 * test. Do note that {@link #addCacheToDriver()} cannot be used (without some fixes first) as this causes a
 * {@link javax.security.auth.login.LoginException}{@code : Can't find user name}. As the current implementation of the
 * {@link HadoopPipelineReducer} does not use the {@link DistributedCacheHandler} (and the custom
 * {@link FileCacheSymlinkReduceDriver} is therefore not needed), this does not cause any problems.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HadoopPipelineReducer.class)
public class HadoopPipelineReducerJUnitTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapReduceDriver allowing the mapper to be tested.
	 */
	private ReduceDriver<BedFeatureSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> rDriver;

	/**
	 * Aligned reads results belonging to the mini test input dataset.
	 */
	private static List<SAMRecord> alignedReadsMiniL1;

	/**
	 * A list containing grouping information.
	 */
	private static List<BEDFeature> groups;

	@BeforeClass
	public static void beforeClass() throws IOException
	{
		alignedReadsMiniL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_MINI_L1);
		groups = TestFileReader.readBedFile(TestFile.GROUPS_SET1);
	}

	/**
	 * Method that should be called first at every JUnit test. Uses exact same naming convention as TestNG tests to stay
	 * coherent (even though this class is not automatically run in the JUnit tests).
	 * 
	 * @throws URISyntaxException
	 */
	public void beforeMethod() throws URISyntaxException
	{
		Reducer<BedFeatureSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> reducer = new HadoopPipelineReducer();
		rDriver = new ReduceDriver<BedFeatureSamRecordStartWritable, SAMRecordWritable, NullWritable, SAMRecordWritable>(
				reducer);
		setDriver(rDriver);
	}

	/**
	 * Tests the reducer and whether {@link MultipleOutputs} works correctly. Due to current incompatibilities between
	 * MRUnit and {@link SAMRecordWritable} from Hadoop-BAM, this test is currently disabled. This is because
	 * {@link SAMRecordWritable} does not have custom {@link Class#equals(Object)}, so the {@link SAMRecordWritable}
	 * {@code s} are compared to whether it is the same {@link Object} instead of whether it is equal in the data it
	 * represents. A custom implementation using {@link SAMRecord#equals(Object)} might be a possible fix, but could
	 * lead to issues if variables are compared that are stored in the {@link SAMFileHeader} from the {@link SAMRecord}
	 * (as this data is lost during serialization in the {@link SAMRecordWritable). This would mean that unless this
	 * incompatibility is fixed, a complete different {@link Writable} would need to be created ONLY for correct
	 * unit-testing. Furthermore, as {@link SAMRecordWritable#toString()} uses {@link SAMRecord#getSAMString()}, when
	 * this test fails an {@link java.lang.IllegalArgumentException} {@code : Reference index 0 not found in sequence
	 * dictionary} is returned. Comparison of mismatching output would therefore also not be possible with the current
	 * implementation.
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	@Ignore
	public void testValidReducerRun() throws IOException, URISyntaxException
	{
		beforeMethod();

		// Generate input.
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> mapperOutput = generateExpectedMapperOutput(
				alignedReadsMiniL1, groups);
		sortMapperOutput(mapperOutput);
		BedFeatureSamRecordStartWritable inputKey = generateBedFeatureSamRecordStartWritable(
				new FullBEDFeature("1", 800001, 1000000), 812735);
		List<SAMRecordWritable> inputValues = filterMapperOutput(mapperOutput, inputKey);

		// Generate expected output.
		List<Pair<NullWritable, SAMRecordWritable>> expectedReducerOutput = generateExpectedReducerOutput(inputValues);

		// Run test.
		rDriver.withInput(inputKey, inputValues).withMultiOutput("recordsPerRegion", expectedReducerOutput.get(0))
				.withMultiOutput("recordsPerRegion", expectedReducerOutput.get(1)).runTest();
	}
}
