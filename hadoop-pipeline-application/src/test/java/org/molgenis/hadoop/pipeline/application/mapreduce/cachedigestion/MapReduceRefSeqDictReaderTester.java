package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.exceptions.SinkIOException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

public class MapReduceRefSeqDictReaderTester extends Tester
{
	/**
	 * The reader that is being tested.
	 */
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

	/**
	 * Tests the digestion of a valid .dict file.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testRefSeqDictReadingValidInput() throws IOException
	{
		SAMSequenceDictionary expectedSeqDict = new SAMSequenceDictionary();

		expectedSeqDict.addSequence(new SAMSequenceRecord("1:20000000-21000000", 1000001));

		SAMSequenceDictionary actualSeqDict = reader
				.read(getClassLoader().getResource("reference_data/chr1_20000000-21000000.dict").getFile());

		assertEqualsSamSequenceDictionaries(actualSeqDict, expectedSeqDict);
	}

	/**
	 * Tests the digestion of a .dict file missing a name ("SN:") field.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = SinkIOException.class)
	public void testRefSeqDictReadingSeqWithoutName() throws IOException
	{
		reader.read(getClassLoader().getResource("extra_dict_files/no_name_field.dict").getFile());
	}

	/**
	 * Tests a .dict file that misses a length ("LN:") field.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = SinkIOException.class)
	public void testRefSeqDictReadingSeqWithoutLength() throws IOException
	{
		reader.read(getClassLoader().getResource("extra_dict_files/no_length_field.dict").getFile());
	}

	/**
	 * Tests a .dict file that misses a a sequence tag ("@SQ") at the start of the line.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testRefSeqDictReadingSeqWithoutSqTag() throws IOException
	{
		SAMSequenceDictionary expectedSeqDict = new SAMSequenceDictionary();

		SAMSequenceDictionary actualSeqDict = reader
				.read(getClassLoader().getResource("extra_dict_files/no_sq_tag.dict").getFile());

		assertEqualsSamSequenceDictionaries(actualSeqDict, expectedSeqDict);
	}

	/**
	 * Compares a {@link SAMSequenceDictionary} containing the digested data with a {@link SAMSequenceDictionary}
	 * containing the expected data.
	 * 
	 * @param actualSeqDict
	 *            {@link SAMSequenceDictionary}
	 * @param expectedSeqDict
	 *            {@link SAMSequenceDictionary}
	 */
	private void assertEqualsSamSequenceDictionaries(SAMSequenceDictionary actualSeqDict,
			SAMSequenceDictionary expectedSeqDict)
	{
		// Asserts whether the number of SAMSequenceRecord is equal.
		Assert.assertEquals(actualSeqDict.size(), expectedSeqDict.size());

		// Asserts whether the expected name and length are the actual name and length.
		for (int i = 0; i < actualSeqDict.size(); i++)
		{
			Assert.assertEquals(actualSeqDict.getSequence(i).getSequenceName(),
					expectedSeqDict.getSequence(i).getSequenceName());
			Assert.assertEquals(actualSeqDict.getSequence(i).getSequenceLength(),
					expectedSeqDict.getSequence(i).getSequenceLength());
		}
	}

}
