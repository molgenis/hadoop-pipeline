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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.molgenis.hadoop.pipeline.application.DistributedCacheHandler;
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkReduceDriver;
import org.molgenis.hadoop.pipeline.application.writables.RegionWithSortableSamRecordWritable;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

/**
 * Tester for the {@link HadoopPipelineReducer}. As {@link MultipleOutputs} requires {@code @RunWith(}
 * {@link PowerMockRunner}{@code .class}) and did not work (after some initial efforts) using TestNG (see
 * <a href="https://issues.apache.org/jira/browse/MRUNIT-213">https://issues.apache.org/jira/browse/MRUNIT-213</a>),
 * JUnit was used for this test. Do note that {@link #addCacheToDriver()} cannot be used (without some fixes first) as
 * this causes a {@link javax.security.auth.login.LoginException}{@code : Can't find user name}. As the current
 * implementation of the {@link HadoopPipelineReducer} does not use the {@link DistributedCacheHandler} (and the custom
 * {@link FileCacheSymlinkReduceDriver} is therefore not needed), this does not cause any problems.
 * 
 * @deprecated Bugs in unit-testing packages. See also description. It might be outdated as well.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HadoopPipelineReducer.class)
public class HadoopPipelineReducerJUnitTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapReduceDriver allowing the mapper to be tested.
	 */
	private ReduceDriver<RegionWithSortableSamRecordWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> rDriver;

	/**
	 * Aligned reads results belonging to the custom test input dataset.
	 */
	private static List<SAMRecord> alignedReadsCustom;

	/**
	 * A list containing grouping information.
	 */
	private static List<Region> regions;

	@BeforeClass
	public static void beforeClass() throws IOException
	{
		alignedReadsCustom = TestFileReader.readSamFile(TestFile.ALIGNED_READS_CUSTOM);
		regions = TestFileReader.readBedFile(TestFile.GROUPS_SET1);
	}

	/**
	 * Sets large data variables to {@code null} and runs the garbage collector to reduce the amount of memory used
	 * after these tests are done.
	 * 
	 * @throws IOException
	 */
	@AfterClass
	public static void afterClass() throws IOException
	{
		alignedReadsCustom = null;
		regions = null;
		System.gc();
	}

	/**
	 * Method that should be called first at every JUnit test. Uses exact same naming convention as TestNG tests to stay
	 * coherent (even though this class is not automatically run in the JUnit tests).
	 * 
	 * @throws URISyntaxException
	 */
	public void beforeMethod() throws URISyntaxException
	{
		Reducer<RegionWithSortableSamRecordWritable, SAMRecordWritable, NullWritable, SAMRecordWritable> reducer = new HadoopPipelineReducer();
		rDriver = new ReduceDriver<RegionWithSortableSamRecordWritable, SAMRecordWritable, NullWritable, SAMRecordWritable>(
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
	 * IMPORTANT: Removed outdated code and replaced it with TODO's.
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
		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> mapperOutput = generateExpectedMapperOutput(
				alignedReadsCustom, regions);

		// TODO: Convert mapper output to reducer input format.
		// TODO: Generate expected reducer output based on reducer input.

		// TODO: Update "Run test" code.
		// rDriver.withInput(inputKey, inputValues).withMultiOutput("recordsPerRegion", expectedReducerOutput.get(0))
		// .withMultiOutput("recordsPerRegion", expectedReducerOutput.get(1)).runTest();
	}
}
