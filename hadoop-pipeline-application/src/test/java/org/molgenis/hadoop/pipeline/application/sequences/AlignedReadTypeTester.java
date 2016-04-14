package org.molgenis.hadoop.pipeline.application.sequences;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMRecord;

public class AlignedReadTypeTester
{
	/**
	 * Test with a single records, which is unmapped.
	 */
	@Test
	public void testWithSingleUnMappedRecord()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false };
		boolean[] isSupplementary =
		{ false };
		boolean[] isUnmapped =
		{ true };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.UNMAPPED);
	}

	/**
	 * Test with a single records, which is mapped.
	 */
	@Test
	public void testWithSingleMappedRecord()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false };
		boolean[] isSupplementary =
		{ false };
		boolean[] isUnmapped =
		{ false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.MAPPED);
	}

	/**
	 * Test with multiple records, of which the primary record is not mapped.
	 */
	@Test
	public void testWithMappedPrimaryRecordAndMappedSupplementaryRecords()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false, false, false, false };
		boolean[] isSupplementary =
		{ true, false, true, true };
		boolean[] isUnmapped =
		{ false, false, false, false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.MULTIMAPPED);
	}

	/**
	 * Test with multiple records, of which the primary record is not mapped.
	 */
	@Test
	public void testWithUnmappedPrimaryRecordAndMappedSupplementaryRecords()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false, false, false, false };
		boolean[] isSupplementary =
		{ true, true, false, true };
		boolean[] isUnmapped =
		{ false, false, true, false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.MULTIMAPPED_SUPPLEMENTARY_ONLY);
	}

	/**
	 * Test when an empty list is given.
	 */
	@Test
	public void testWithEmptyRecordList()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{};
		boolean[] isSupplementary =
		{};
		boolean[] isUnmapped =
		{};
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.INVALID);
	}

	/**
	 * Test with a single records, which is a supplementary record.
	 */
	@Test
	public void testWithSingleNonPrimaryRecord()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false };
		boolean[] isSupplementary =
		{ true };
		boolean[] isUnmapped =
		{ false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.INVALID);
	}

	/**
	 * Test with multiple records, which are all supplementary records.
	 */
	@Test
	public void testWithMultipleNonPrimaryRecord()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false, false, false, false };
		boolean[] isSupplementary =
		{ true, true, true, true };
		boolean[] isUnmapped =
		{ false, false, false, false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.INVALID);
	}

	/**
	 * Test when two primary records are found.
	 */
	@Test
	public void testWithTwoPrimaryRecords()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false, false, false, false };
		boolean[] isSupplementary =
		{ false, true, false, true };
		boolean[] isUnmapped =
		{ false, false, false, false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.INVALID);
	}

	/**
	 * Test when a record is given that has a different flag regarding first/second record reads (regarding read pairs).
	 */
	@Test
	public void testWithRecordFromPairedRead()
	{
		// Each record n is generated using the array values on position n.
		boolean[] isFirst =
		{ false, false, true, false };
		boolean[] isSupplementary =
		{ false, true, true, true };
		boolean[] isUnmapped =
		{ false, false, false, false };
		AlignedRead actualRead = new AlignedRead(generatePairedSamRecordList(isFirst, isSupplementary, isUnmapped));

		Assert.assertEquals(actualRead.getType(), AlignedRead.Type.INVALID);
	}

	/**
	 * Generates a {@link List} with {@link SAMRecord} using the parameter data.
	 *
	 * @param isFirst
	 *            {@code boolean[]} Each position n determines whether each {@link SAMRecord} on position n in the
	 *            {@link List} represent an aligned record from the first read of a read pair.
	 * @param isSupplementary
	 *            {@code boolean[]} Each position n determines whether each {@link SAMRecord} on position n in the
	 *            {@link List} represents a supplementary {@link SAMRecord}.
	 * @param isUnmapped
	 *            {@code boolean[]} Each position n determines whether each {@link SAMRecord} on position n in the
	 *            {@link List} represents an unmapped {@link SAMRecord}.
	 * @return {@link List}{@code <}{@link SAMRecord}{@code >}
	 */
	private List<SAMRecord> generatePairedSamRecordList(boolean[] isFirst, boolean[] isSupplementary,
			boolean[] isUnmapped)
	{
		// Validates input.
		if (isFirst.length != isSupplementary.length) return null;
		if (isSupplementary.length != isUnmapped.length) return null;

		// Generates SAMRecord list.
		List<SAMRecord> records = new ArrayList<>();
		for (int i = 0; i < isFirst.length; i++)
		{
			records.add(generateSamRecord(true, isFirst[i], isSupplementary[i], isUnmapped[i]));
		}

		return records;
	}

	/**
	 * Generates a {@link SAMRecord} using the parameter data.
	 * 
	 * @param isPaired
	 *            {@code boolean} Whether the generated {@link SAMRecord} should represent a {@link SAMRecord} from
	 *            paired reads. If {@code false}, {@link isFirst} will be not be used and can be of any (allowed) value.
	 * @param isFirst
	 *            {@code boolean} Whether the generated {@link SAMRecord} should represent an aligned record from the
	 *            first read of a read pair. Is not used if {@code isPaired == false}.
	 * @param isSupplementary
	 *            {@code boolean} Whether the generated {@link SAMRecord} should represent a supplementary
	 *            {@link SAMRecord}.
	 * @param isUnmapped
	 *            {@code boolean} Whether the generated {@link SAMRecord} should represent an unmapped {@link SAMRecord}
	 *            .
	 * @return {@link SAMRecord}
	 */
	private SAMRecord generateSamRecord(boolean isPaired, boolean isFirst, boolean isSupplementary, boolean isUnmapped)
	{
		SAMRecord record = new SAMRecord(null);
		record.setReadPairedFlag(isPaired);
		if (isPaired) record.setFirstOfPairFlag(isFirst);
		record.setSupplementaryAlignmentFlag(isSupplementary);
		record.setReadUnmappedFlag(isUnmapped);
		return record;
	}
}
