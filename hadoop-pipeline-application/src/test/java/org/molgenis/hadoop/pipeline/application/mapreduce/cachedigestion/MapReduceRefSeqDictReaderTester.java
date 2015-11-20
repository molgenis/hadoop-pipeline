package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.exceptions.SinkIOException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

public class MapReduceRefSeqDictReaderTester extends Tester
{
	private MapReduceRefSeqDictReader reader;

	/**
	 * Creates a {@link MapReduceRefSeqDictReader} needed for testing.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		reader = new MapReduceRefSeqDictReader(FileSystem.get(new Configuration()));
	}

	@Test
	public void testRefSeqDictReadingValidInput() throws IOException
	{
		SAMSequenceDictionary expectedSeqDict = new SAMSequenceDictionary();

		expectedSeqDict.addSequence(new SAMSequenceRecord("1:20000000-21000000", 1000001));

		SAMSequenceDictionary actualSeqDict = reader
				.readFile(getClassLoader().getResource("reference_data/chr1_20000000-21000000.dict").getFile());

		assertEqualsSamSequenceDictionaries(expectedSeqDict, actualSeqDict);
	}

	@Test(expectedExceptions = SinkIOException.class)
	public void testRefSeqDictReadingSeqWithoutName() throws IOException
	{
		reader.readFile(getClassLoader().getResource("extra_dict_files/no_name_field.dict").getFile());
	}

	@Test(expectedExceptions = SinkIOException.class)
	public void testRefSeqDictReadingSeqWithoutLength() throws IOException
	{
		reader.readFile(getClassLoader().getResource("extra_dict_files/no_length_field.dict").getFile());
	}

	@Test
	public void testRefSeqDictReadingSeqWithoutSqTag() throws IOException
	{
		SAMSequenceDictionary expectedSeqDict = new SAMSequenceDictionary();

		SAMSequenceDictionary actualSeqDict = reader
				.readFile(getClassLoader().getResource("extra_dict_files/no_sq_tag.dict").getFile());

		assertEqualsSamSequenceDictionaries(expectedSeqDict, actualSeqDict);
	}

	private void assertEqualsSamSequenceDictionaries(SAMSequenceDictionary expectedSeqDict,
			SAMSequenceDictionary actualSeqDict)
	{
		// Asserts whether the number of SAMSequenceRecord is equal.
		Assert.assertEquals(expectedSeqDict.size(), actualSeqDict.size());

		// Asserts whether the expected name and length are the actual name and length.
		for (int i = 0; i < actualSeqDict.size(); i++)
		{
			Assert.assertEquals(expectedSeqDict.getSequence(i).getSequenceName(),
					actualSeqDict.getSequence(i).getSequenceName());
			Assert.assertEquals(expectedSeqDict.getSequence(i).getSequenceLength(),
					actualSeqDict.getSequence(i).getSequenceLength());
		}
	}

}
