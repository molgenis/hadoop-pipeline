package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

public class HadoopPipelineTester extends Tester
{
	/**
	 * A mrunit driver allowing the Mapper/Reducer/MapReducer to be tested.
	 */
	private TestDriver<?, ?, ?> driver;

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

	TestDriver<?, ?, ?> getDriver()
	{
		return driver;
	}

	void setDriver(TestDriver<?, ?, ?> driver)
	{
		this.driver = driver;
	}

	ArrayList<SAMRecord> getBwaResults()
	{
		return bwaResults;
	}

	ArrayList<SAMRecord> getBwaResultsWithReadGroupLine()
	{
		return bwaResultsWithReadGroupLine;
	}

	SAMFileHeader getSamFileHeader()
	{
		return samFileHeader;
	}

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
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
		addCacheToMapDriver();
	}

	/**
	 * Adds needed files to the {@link MapDriver} chache.
	 * 
	 * @throws URISyntaxException
	 */
	private void addCacheToMapDriver() throws URISyntaxException
	{
		// IMPORTANT: input order defines position in array for retrieval in mapper/reducer!!!
		driver.addCacheArchive(getClassLoader().getResource("tools.tar.gz").toURI());

		// IMPORTANT: input order defines position in array for retrieval in mapper/reducer!!!
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.amb").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.ann").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.bwt").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.fai").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.pac").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.sa").toURI());
		driver.addCacheFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.dict").toURI());
		driver.addCacheFile(getClassLoader().getResource("bed_files/chr1_20000000-21000000.bed").toURI());
	}
}
