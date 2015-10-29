package org.molgenis.hadoop.pipeline.application.processes;

import java.io.StringWriter;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;

public class BwaAlignerTester extends Tester
{
	StringWriter stringWriter;
	String bwaTool;
	String referenceData;

	@AfterClass
	public void afterClass()
	{
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
	 * Tests the bwa alignment. The validity is checked by comparing the generated output to a select number of lines
	 * taken from an output file that was created locally by executing a bwa binary using the command line (
	 * {@code bwa mem -p -M chr1_20000000-21000000.fa - < halvade_0_0.fq.gz > output.sam}).
	 * 
	 * @throws Exception
	 */
	@Test
	public void runBwaAlignment() throws Exception
	{
		// Reads in the intertwined fastq file as a binary array.
		byte[] inputData = readFileAsByteArray("input_fastq/halvade_0_0.fq.gz");

		// Prepares and executes the alignment
		BwaAligner bwaAlinger = new BwaAligner(getClassLoader().getResource("tools/bwa").getPath(),
				new String(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa").getPath()));
		bwaAlinger.setProcessInputData(inputData);
		SamInContainer results = bwaAlinger.call();
		ArrayList<SAMRecord> records = results.get();

		System.out.println("### Command line stored in SamInContainer instance ###");
		System.out.println(results.getHeader().getProgramRecords().get(0).getCommandLine());

		// Writes the first few lines from the bwa alignment to stdout (for manual inspection as no automated comparison
		// has yet been implemented due to reads that can be assigned to multiple locations being assigned randomly
		// every time).
		System.out.println("### Bwa Aligner output (first few lines) ###");
		for (int i = 0; i < 10; i++)
		{
			System.out.println(records.get(i).getSAMString().trim());
		}

		// Compares the @SQ line.
		Assert.assertEquals("1:20000000-21000000",
				results.getHeader().getSequenceDictionary().getSequence(0).getSequenceName());
		Assert.assertEquals(1000001, results.getHeader().getSequenceDictionary().getSequence(0).getSequenceLength());

		// Compares non-variable parts of the @PG line.
		Assert.assertEquals("bwa", results.getHeader().getProgramRecords().get(0).getId());
		Assert.assertEquals("bwa", results.getHeader().getProgramRecords().get(0).getProgramName());
		Assert.assertEquals("0.7.12-r1039", results.getHeader().getProgramRecords().get(0).getProgramVersion());
	}
}
