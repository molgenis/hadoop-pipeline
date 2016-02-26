package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
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
	 * SAMFileHeader to add to the reads for comparison as it is lost when the mapper results are written to the
	 * context. Only adds the @SQ tag which is vital as otherwise exceptions are thrown during comparison.
	 */
	private SAMFileHeader samFileHeader = new SAMFileHeader();

	TestDriver<?, ?, ?> getDriver()
	{
		return driver;
	}

	void setDriver(TestDriver<?, ?, ?> driver)
	{
		this.driver = driver;
	}

	/**
	 * Loads/generates general data needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
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

	void setHeaderForRecord(SAMRecord record)
	{
		record.setHeader(samFileHeader);
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
