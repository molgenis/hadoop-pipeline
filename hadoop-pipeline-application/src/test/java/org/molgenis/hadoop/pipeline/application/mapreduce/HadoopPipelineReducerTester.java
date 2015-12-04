package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkReduceDriver;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable;
//import org.powermock.modules.testng.PowerMockObjectFactory;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.FullBEDFeature;

/**
 * Tests the Reducer as a whole.
 * 
 * @deprecated Currently there is a bug within MRUnit that does not allow testing when using {@link MultipleOutputs}.
 *             Please refrain from using this test until this bug is fixed. This class was not removed so it can
 *             function as reference (or for re-use) when creating a working test class for the reducer.
 * @see <a href='https://issues.apache.org/jira/browse/MRUNIT-213'>https://issues.apache.org/jira/browse/MRUNIT-213</a>
 */
// @PrepareForTest(HadoopPipelineReducer.class)
// @PowerMockIgnore(
// { "javax.xml.*", "org.xml.sax.*", "org.apache.xerces.*", "org.springframework.context.*", "org.apache.log4j.*",
// "org.w3c.dom.*" })
public class HadoopPipelineReducerTester extends HadoopPipelineTester
{
	/**
	 * A mrunit MapDriver allowing the mapper to be tested.
	 */
	private ReduceDriver<BedFeatureWritable, SAMRecordWritable, NullWritable, Text> rDriver;

	// @ObjectFactory
	// public IObjectFactory getObjectFactory()
	// {
	// return new PowerMockObjectFactory();
	// }

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		Reducer<BedFeatureWritable, SAMRecordWritable, NullWritable, Text> reducer = new HadoopPipelineReducer();
		rDriver = new FileCacheSymlinkReduceDriver<BedFeatureWritable, SAMRecordWritable, NullWritable, Text>(reducer);
		setDriver(rDriver);

		super.beforeMethod();
	}

	/**
	 * Tests the {@link HadoopPipelineReducer} when no readgroupline is also given in the input.
	 * 
	 * @throws IOException
	 */
	@Test(enabled = false)
	public void testReducerWithoutReadGroupLine() throws IOException
	{
		// Generates expected header information.
		SAMFileHeader header = getBwaResults().get(0).getHeader();
		String expectedSqTag = "@SQ\tSN:" + header.getSequence(0).getSequenceName() + "\tLN:"
				+ header.getSequence(0).getSequenceLength();
		String expectedPgTag = "@PG\tID:" + header.getProgramRecords().get(0).getId() + "\tPN:"
				+ header.getProgramRecords().get(0).getProgramName() + "\tVN:"
				+ header.getProgramRecords().get(0).getProgramVersion();
				// Does not compare header.getProgramRecords().get(0).getCommandLine() as this would be different anyhow
				// due to paths to files that can differ and alike.

		// Defines input.
		ArrayList<SAMRecordWritable> input = new ArrayList<SAMRecordWritable>();

		for (SAMRecord record : getBwaResults())
		{
			SAMRecordWritable writable = new SAMRecordWritable();
			writable.set(record);
			input.add(writable);
		}
		rDriver.withInput(new BedFeatureWritable(new FullBEDFeature("1", 1, 1000000)), input);
		rDriver.addMultiOutput("output", NullWritable.get(), expectedSqTag);
		rDriver.addMultiOutput("output", NullWritable.get(), expectedPgTag);
		for (SAMRecord record : getBwaResults())
		{
			rDriver.addMultiOutput("output", NullWritable.get(), record.getSAMString());
		}
		rDriver.runTest(true);

		// Runs driver.
		List<Pair<NullWritable, Text>> output = rDriver.run();

		// Validates output.
		Assert.assertEquals(getValueAsStringFromOutput(output, 0), expectedSqTag);
		Assert.assertEquals(getValueAsStringFromOutput(output, 1), expectedPgTag);
		for (int i = 2; i < getBwaResults().size() + 2; i++)
		{
			Assert.assertEquals(getValueAsStringFromOutput(output, i),
					getBwaResults().get(i - 2).getSAMString().trim());
		}
	}

	/**
	 * Tests the {@link HadoopPipelineReducer} when a readgroupline is also given in the input.
	 *
	 * @throws IOException
	 */
	@Test(enabled = false)
	public void testReducerWithReadGroupLine() throws IOException
	{
		// Generates expected header information.
		SAMFileHeader header = getBwaResultsWithReadGroupLine().get(0).getHeader();
		String expectedSqTag = "@SQ\tSN:" + header.getSequence(0).getSequenceName() + "\tLN:"
				+ header.getSequence(0).getSequenceLength();
		String expectedRgTag = "@RG\tID:" + header.getReadGroups().get(0).getId() + "\tPL:"
				+ header.getReadGroups().get(0).getPlatform() + "\tLB:" + header.getReadGroups().get(0).getLibrary()
				+ "\tSN:" + header.getReadGroups().get(0).getSample();
		String expectedPgTag = "@PG\tID:" + header.getProgramRecords().get(0).getId() + "\tPN:"
				+ header.getProgramRecords().get(0).getProgramName() + "\tVN:"
				+ header.getProgramRecords().get(0).getProgramVersion();
				// Does not compare header.getProgramRecords().get(0).getCommandLine() as this would be different anyhow
				// due to paths to files that can differ and alike.

		// Defines input.
		ArrayList<SAMRecordWritable> input = new ArrayList<SAMRecordWritable>();

		for (SAMRecord record : getBwaResultsWithReadGroupLine())
		{
			SAMRecordWritable writable = new SAMRecordWritable();
			writable.set(record);
			input.add(writable);
		}
		rDriver.withInput(new BedFeatureWritable(new FullBEDFeature("1", 1, 1000000)), input);

		rDriver.getConfiguration().set("input_readgroupline", expectedRgTag);

		// Runs driver.
		List<Pair<NullWritable, Text>> output = rDriver.run();

		// Validates output.
		Assert.assertEquals(getValueAsStringFromOutput(output, 0), expectedSqTag);
		Assert.assertEquals(getValueAsStringFromOutput(output, 1), expectedRgTag);
		Assert.assertEquals(getValueAsStringFromOutput(output, 2), expectedPgTag);
		for (int i = 3; i < getBwaResults().size() + 3; i++)
		{
			Assert.assertEquals(getValueAsStringFromOutput(output, i),
					getBwaResultsWithReadGroupLine().get(i - 3).getSAMString().trim());
		}
	}

	/**
	 * Retrieve a single value as {@link String} from the output created by a Test. Use {@code index} to define the
	 * position from the {@code output} {@link List} which should be retrieved.
	 * 
	 * @param output
	 *            {@link List}{@code <}{@link NullWritable}{@code ,}{@link Text}{@code >>}
	 * @param index
	 *            {@code int}
	 * @return {@link String}
	 * @throws IOException
	 * @throws IndexOutOfBoundsException
	 */
	private String getValueAsStringFromOutput(List<Pair<NullWritable, Text>> output, int index)
			throws IOException, IndexOutOfBoundsException
	{
		return output.get(index).getSecond().toString();
	}
}
