package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.mrunit.TestDriver;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.mapreduce.drivers.FileCacheSymlinkMapDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

/**
 * Contains general code of Mapper/Reducer testing.
 */
public class HadoopPipelineTester extends Tester
{
	/**
	 * A mrunit driver allowing the Mapper/Reducer/MapReducer to be tested.
	 */
	private TestDriver<?, ?, ?> driver;

	/**
	 * Expected results bwa when using readgroup example L2. Can be generated with a terminal using:
	 * {@code /path/to/bwa mem -p -M -R "@RG\tID:2\tPL:illumina\tLB:150616_SN163_648_AHKYLMADXX_L2\tSM:sample2"
	 * hadoop-pipeline-application/src/test/resources/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline-application/src/test/resources/input_fastq_mini/mini_halvade_0_0.fq.gz >
	 * hadoop-pipeline-application/src/test/resources/expected_bwa_outputs_mini/output_mini_L2.sam}
	 */
	private ArrayList<SAMRecord> bwaResultsL2;

	/**
	 * Expected results bwa when using readgroup example L5. Can be generated with a terminal using:
	 * {@code /path/to/bwa mem -p -M -R "@RG\tID:5\tPL:illumina\tLB:150702_SN163_649_BHJYNKADXX_L5\tSM:sample3"
	 * hadoop-pipeline-application/src/test/resources/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline-application/src/test/resources/input_fastq_mini/mini_halvade_0_0.fq.gz >
	 * hadoop-pipeline-application/src/test/resources/expected_bwa_outputs_mini/output_mini_L5.sam}
	 */
	private ArrayList<SAMRecord> bwaResultsL5;

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

	ArrayList<SAMRecord> getBwaResultsL2()
	{
		return bwaResultsL2;
	}

	ArrayList<SAMRecord> getBwaResultsL5()
	{
		return bwaResultsL5;
	}

	/**
	 * Returns an {@link ArrayList} that stores the data from both {@link #getBwaResultsL2()} and
	 * {@link #getBwaResultsL5()}.
	 * 
	 * @return {@link ArrayList}{@code <}{@link SAMRecord}{@code >}
	 */
	ArrayList<SAMRecord> getAllBwaResults()
	{
		ArrayList<SAMRecord> allResults = new ArrayList<SAMRecord>();
		allResults.addAll(bwaResultsL2);
		allResults.addAll(bwaResultsL5);
		return allResults;
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
		bwaResultsL2 = readSamFile("expected_bwa_outputs_mini/output_mini_L2.sam");
		bwaResultsL5 = readSamFile("expected_bwa_outputs_mini/output_mini_L5.sam");

		samFileHeader = new SAMFileHeader();
		// Generates SAMRecordFileheader SequenceDictionary based upon a @SQ tag with:
		// @SQ\tSN:1\tLN:1000001
		samFileHeader
				.setSequenceDictionary(new SAMSequenceDictionary(Arrays.asList(new SAMSequenceRecord("1", 1000001))));
	}

	/**
	 * Generates a new {@link FileCacheSymlinkMapDriver} for testing the {@link HadoopPipelineMapper}.
	 * 
	 * @throws URISyntaxException
	 */
	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		addCacheToDriver();
	}

	/**
	 * Adds needed files to the {@link MapDriver} chache.
	 * 
	 * @throws URISyntaxException
	 */
	private void addCacheToDriver() throws URISyntaxException
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
		driver.addCacheFile(getClassLoader().getResource("samplesheets/samplesheet.csv").toURI());
	}
}
