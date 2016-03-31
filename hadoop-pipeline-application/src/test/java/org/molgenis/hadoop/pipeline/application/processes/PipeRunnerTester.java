package org.molgenis.hadoop.pipeline.application.processes;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.molgenis.hadoop.pipeline.application.TestFile;
import org.molgenis.hadoop.pipeline.application.TestFileReader;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.SamRecordSink;
import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.StringSink;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;

/**
 * Tester for {@link PipeRunnerTester}.
 */
public class PipeRunnerTester extends Tester
{
	/**
	 * Writer to catch logger output with.
	 */
	private StringWriter stringWriter;

	/**
	 * The fastq data to be digested.
	 */
	private byte[] fastqDataL1;

	/**
	 * Stores the expected results of the bwa alignment.
	 */
	private ArrayList<SAMRecord> expectedBwaResultsL1;

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		fastqDataL1 = TestFileReader.readFileAsByteArray(TestFile.FASTQ_DATA_L1);
		expectedBwaResultsL1 = TestFileReader.readSamFile(TestFile.ALIGNED_READS_L1);
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
		fastqDataL1 = null;
		expectedBwaResultsL1 = null;
		System.gc();
	}

	/**
	 * Creates an empty {@link StringWriter} to which logger information can be written to.
	 */
	@BeforeMethod
	public void beforeMethod()
	{
		stringWriter = new StringWriter();
		Logger.getRootLogger().addAppender(new WriterAppender(new SimpleLayout(), stringWriter));
	}

	/**
	 * Prints the logger information.
	 */
	@AfterMethod
	public void afterMethod()
	{
		System.out.println(stringWriter.toString());
	}

	/**
	 * Tests the {@link PipeRunner} by calling a simple python character replacing script.
	 * 
	 * @throws Exception
	 */
	@Test
	public void runSinkWithSingleProces() throws Exception
	{
		// Reads in the intertwined fastq file as a binary array.
		byte[] inputData = new String("Hello world?" + System.lineSeparator() + "This is 1 demo!")
				.getBytes(StandardCharsets.UTF_8);

		// Creates a StringBuilder to store the stream in.
		final StringBuffer sb = new StringBuffer();

		// Describes the sink functionality (what has to be done for each line).
		StringSink sink = new StringSink()
		{
			@Override
			public void digestStreamItem(String item)
			{
				sb.append(item + System.lineSeparator());
			}
		};

		// Runs the pipeline.
		PipeRunner.startPipeline(inputData, sink,
				new ProcessBuilder("python",
						getClassLoader().getResource("character_replacer/CharacterReplacer.py").getPath(), "1", "a")
								.start());

		// Splits the StringBuilder results into a String array with each line being one element.
		String[] lines = sb.toString().split(System.lineSeparator());
		Assert.assertEquals(lines[0], "Hello world?");
		Assert.assertEquals(lines[1], "This is a demo!");
	}

	/**
	 * Tests the {@link PipeRunner} by calling a simple python character replacing script multiple times.
	 * 
	 * @throws Exception
	 */
	@Test
	public void runSinkWithMultipleProcesses() throws Exception
	{
		// Reads in the intertwined fastq file as a binary array.
		byte[] inputData = new String("Hello world?" + System.lineSeparator() + "This is 1 demo!")
				.getBytes(StandardCharsets.UTF_8);

		// Creates a StringBuilder to store the stream in.
		final StringBuffer sb = new StringBuffer();

		// Describes the sink functionality (what has to be done for each line).
		StringSink sink = new StringSink()
		{
			@Override
			public void digestStreamItem(String item)
			{
				sb.append(item + System.lineSeparator());
			}
		};

		// Runs the pipeline.
		PipeRunner.startPipeline(inputData, sink,
				new ProcessBuilder("python",
						getClassLoader().getResource("character_replacer/CharacterReplacer.py").getPath(), "?", ".")
								.start(),
				new ProcessBuilder("python",
						getClassLoader().getResource("character_replacer/CharacterReplacer.py").getPath(), "1", "a")
								.start(),
				new ProcessBuilder("python",
						getClassLoader().getResource("character_replacer/CharacterReplacer.py").getPath(), "!", ".")
								.start());

		// Splits the StringBuilder results into a String array with each line being one element.
		String[] lines = sb.toString().split(System.lineSeparator());
		Assert.assertEquals(lines[0], "Hello world.");
		Assert.assertEquals(lines[1], "This is a demo.");
	}

	/**
	 * Tests the {@link PipeRunner} using the bwa binary and a {@link SAMRecordSink}. It is important to note that for
	 * correct validation, the reads that are aligned cannot have multiple locations they can be aligned to. While the
	 * fastq 150616_SN163_0648_AHKYLMADXX_L1 test data set was not validated to have reads that all align uniquely all
	 * the time, running this test locally several times caused no errors. Therefore, the assumption was made that the
	 * reads align to the same locations every time. In case this test causes errors without relevant code being
	 * adjusted, a manual validation is advised. Nevertheless, external tools are assumed to work correctly anyway as
	 * tests belonging to those tools should be part of those tools and not here. So this test is purely for an extra
	 * layer of confidence that the code works as it should. Testing whether the alignment locations of the SAMRecords
	 * are correct is not the core idea behind this test.
	 *
	 * @throws Exception
	 */
	@Test
	public void runBwaAlignmentWithSAMRecordSink() throws Exception
	{
		// Stores the actual SAMRecords by the bwa Process.
		final ArrayList<SAMRecord> actualSamRecords = new ArrayList<>();

		// Describes the sink functionality (what has to be done for each record).
		SamRecordSink sink = new SamRecordSink()
		{
			@Override
			public void digestStreamItem(SAMRecord item)
			{
				actualSamRecords.add(item);
			}
		};

		// Runs the pipeline.
		PipeRunner.startPipeline(fastqDataL1, sink,
				new ProcessBuilder(getClassLoader().getResource("tools/bwa").getPath(), "mem", "-p", "-M", "-R",
						"@RG\\tID:1\\tPL:illumina\\tLB:150616_SN163_648_AHKYLMADXX_L1\\tSM:sample1",
						getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa").getPath(), "-")
								.start());

		// Tests whether the actual output is the same as the expected output.
		try
		{
			Assert.assertEquals(actualSamRecords, expectedBwaResultsL1);
		}
		// Clears the generated output, even if an exception is thrown.
		finally
		{
			actualSamRecords.clear();
		}
	}
}