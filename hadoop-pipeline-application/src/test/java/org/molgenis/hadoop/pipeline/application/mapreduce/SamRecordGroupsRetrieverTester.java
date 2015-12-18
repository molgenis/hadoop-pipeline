package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.molgenis.hadoop.pipeline.application.BedFeatureTester;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.SimpleBEDFeature;

public class SamRecordGroupsRetrieverTester extends BedFeatureTester
{
	SamRecordGroupsRetriever grouper;
	SAMRecord record;
	SAMRecord record2;
	ArrayList<BEDFeature> inputGroups;
	ArrayList<BEDFeature> expectedOutputGroups;

	@BeforeClass
	public void beforeClass() throws IOException
	{
		SAMFileHeader header = new SAMFileHeader();
		SAMSequenceDictionary seqDict = new SAMSequenceDictionary();
		seqDict.addSequence(new SAMSequenceRecord("1:1-301", 300));
		header.setSequenceDictionary(seqDict);
		record = new SAMRecord(header);

		// Sets a record on contig 1 with range 100-200 (inclusive start/end).
		record.setReferenceName("1");
		record.setAlignmentStart(100);
		record.setCigarString("101M");

		SAMFileHeader header2 = new SAMFileHeader();
		SAMSequenceDictionary seqDict2 = new SAMSequenceDictionary();
		seqDict2.addSequence(new SAMSequenceRecord("1:1-301", 300));
		seqDict2.addSequence(new SAMSequenceRecord("2:1-301", 300));
		header2.setSequenceDictionary(seqDict2);
		record2 = new SAMRecord(header2);

		// Sets a record on contig 2 with range 100-200 (inclusive start/end).
		record2.setReferenceName("2");
		record2.setAlignmentStart(100);
		record2.setCigarString("101M");
	}

	@BeforeMethod
	public void beforeMethod()
	{
		// Creates a new ArrayList to be filled with BEDFeatures that can be used to compare with the SAMRecord.
		inputGroups = new ArrayList<BEDFeature>();
		expectedOutputGroups = new ArrayList<BEDFeature>();

	}

	@AfterMethod
	public void afterMethod()
	{
		// Clears the grouper.
		grouper = null;
	}

	@Test
	public void testWithSingleBedJustBeforeRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(89, 99, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Expected output should be empty, so no additional adjustments are made to the expected output.

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithSingleBedJustOnStartOfRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(90, 100, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// All input groups should be returned, so output is equal to input.
		expectedOutputGroups = inputGroups;

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithSingleBedJustAfterRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(201, 211, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Expected output should be empty, so no additional adjustments are made to the expected output.

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithSingleBedJustOnEndOfRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(200, 210, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// All input groups should be returned, so output is equal to input.
		expectedOutputGroups = inputGroups;

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithSingleBedWithinRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(150, 160, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// All input groups should be returned, so output is equal to input.
		expectedOutputGroups = inputGroups;

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithSingleBedCompletelyOverlappingRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(90, 210, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// All input groups should be returned, so output is equal to input.
		expectedOutputGroups = inputGroups;

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsOddArrayLengthAllWithinRange()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(98, 123, "1"));
		inputGroups.add(new SimpleBEDFeature(124, 148, "1"));
		inputGroups.add(new SimpleBEDFeature(149, 173, "1"));
		inputGroups.add(new SimpleBEDFeature(174, 198, "1"));
		inputGroups.add(new SimpleBEDFeature(199, 223, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// All input groups should be returned, so output is equal to input.
		expectedOutputGroups = inputGroups;

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthAllWithinRange()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(91, 110, "1"));
		inputGroups.add(new SimpleBEDFeature(111, 130, "1"));
		inputGroups.add(new SimpleBEDFeature(131, 150, "1"));
		inputGroups.add(new SimpleBEDFeature(151, 170, "1"));
		inputGroups.add(new SimpleBEDFeature(171, 190, "1"));
		inputGroups.add(new SimpleBEDFeature(191, 220, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// All input groups should be returned, so output is equal to input.
		expectedOutputGroups = inputGroups;

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsOddArrayLengthAllBeforeRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(11, 20, "1"));
		inputGroups.add(new SimpleBEDFeature(21, 30, "1"));
		inputGroups.add(new SimpleBEDFeature(31, 40, "1"));
		inputGroups.add(new SimpleBEDFeature(41, 50, "1"));
		inputGroups.add(new SimpleBEDFeature(51, 60, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Expected output should be empty, so no additional adjustments are made to the expected output.

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthAllBeforeRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(11, 20, "1"));
		inputGroups.add(new SimpleBEDFeature(21, 30, "1"));
		inputGroups.add(new SimpleBEDFeature(31, 40, "1"));
		inputGroups.add(new SimpleBEDFeature(41, 50, "1"));
		inputGroups.add(new SimpleBEDFeature(51, 60, "1"));
		inputGroups.add(new SimpleBEDFeature(61, 70, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Expected output should be empty, so no additional adjustments are made to the expected output.

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsOddArrayLengthAllAfterRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(211, 220, "1"));
		inputGroups.add(new SimpleBEDFeature(221, 230, "1"));
		inputGroups.add(new SimpleBEDFeature(231, 240, "1"));
		inputGroups.add(new SimpleBEDFeature(241, 250, "1"));
		inputGroups.add(new SimpleBEDFeature(251, 260, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Expected output should be empty, so no additional adjustments are made to the expected output.

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthAllAfterRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(211, 220, "1"));
		inputGroups.add(new SimpleBEDFeature(221, 230, "1"));
		inputGroups.add(new SimpleBEDFeature(231, 240, "1"));
		inputGroups.add(new SimpleBEDFeature(241, 250, "1"));
		inputGroups.add(new SimpleBEDFeature(251, 260, "1"));
		inputGroups.add(new SimpleBEDFeature(261, 270, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Expected output should be empty, so no additional adjustments are made to the expected output.

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsOddArrayLengthSomeBeforeInAfterRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(10, 60, "1"));
		inputGroups.add(new SimpleBEDFeature(61, 110, "1"));
		inputGroups.add(new SimpleBEDFeature(111, 160, "1"));
		inputGroups.add(new SimpleBEDFeature(161, 210, "1"));
		inputGroups.add(new SimpleBEDFeature(211, 260, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Sublist of input should be returned.
		expectedOutputGroups.addAll(inputGroups.subList(1, 4));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthSomeBeforeInAfterRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(61, 90, "1"));
		inputGroups.add(new SimpleBEDFeature(91, 120, "1"));
		inputGroups.add(new SimpleBEDFeature(121, 150, "1"));
		inputGroups.add(new SimpleBEDFeature(151, 180, "1"));
		inputGroups.add(new SimpleBEDFeature(181, 210, "1"));
		inputGroups.add(new SimpleBEDFeature(211, 240, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Sublist of input should be returned.
		expectedOutputGroups.addAll(inputGroups.subList(1, 5));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsOddArrayLengthOnlyFirstWithinRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(196, 205, "1"));
		inputGroups.add(new SimpleBEDFeature(206, 215, "1"));
		inputGroups.add(new SimpleBEDFeature(216, 225, "1"));
		inputGroups.add(new SimpleBEDFeature(226, 235, "1"));
		inputGroups.add(new SimpleBEDFeature(236, 245, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Only first item of input should be returned.
		expectedOutputGroups.add(inputGroups.get(0));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthOnlyFirstWithinRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(196, 205, "1"));
		inputGroups.add(new SimpleBEDFeature(206, 215, "1"));
		inputGroups.add(new SimpleBEDFeature(216, 225, "1"));
		inputGroups.add(new SimpleBEDFeature(226, 235, "1"));
		inputGroups.add(new SimpleBEDFeature(236, 245, "1"));
		inputGroups.add(new SimpleBEDFeature(246, 255, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Only first item of input should be returned.
		expectedOutputGroups.add(inputGroups.get(0));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsOddArrayLengthOnlyLastWithinRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(56, 65, "1"));
		inputGroups.add(new SimpleBEDFeature(66, 75, "1"));
		inputGroups.add(new SimpleBEDFeature(76, 85, "1"));
		inputGroups.add(new SimpleBEDFeature(86, 95, "1"));
		inputGroups.add(new SimpleBEDFeature(96, 105, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Only last item of input should be returned.
		expectedOutputGroups.add(inputGroups.get(inputGroups.size() - 1));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthOnlyLastWithinRecord()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(46, 55, "1"));
		inputGroups.add(new SimpleBEDFeature(56, 65, "1"));
		inputGroups.add(new SimpleBEDFeature(66, 75, "1"));
		inputGroups.add(new SimpleBEDFeature(76, 85, "1"));
		inputGroups.add(new SimpleBEDFeature(86, 95, "1"));
		inputGroups.add(new SimpleBEDFeature(96, 105, "1"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Only last item of input should be returned.
		expectedOutputGroups.add(inputGroups.get(inputGroups.size() - 1));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthTwoOnSameContigOfWhichAllInRange()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(91, 110, "1"));
		inputGroups.add(new SimpleBEDFeature(111, 130, "1"));
		inputGroups.add(new SimpleBEDFeature(131, 150, "1"));
		inputGroups.add(new SimpleBEDFeature(151, 170, "1"));
		inputGroups.add(new SimpleBEDFeature(171, 190, "1"));
		inputGroups.add(new SimpleBEDFeature(191, 220, "1"));
		inputGroups.add(new SimpleBEDFeature(131, 150, "2"));
		inputGroups.add(new SimpleBEDFeature(151, 170, "2"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Sublist of input should be returned.
		expectedOutputGroups.addAll(inputGroups.subList(6, 8));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record2);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsUnevenArrayLengthThreeOnSameContigOfWhichAllInRange()
	{
		// Prepares/executes bed with record matching.
		inputGroups.add(new SimpleBEDFeature(91, 110, "1"));
		inputGroups.add(new SimpleBEDFeature(111, 130, "1"));
		inputGroups.add(new SimpleBEDFeature(131, 150, "1"));
		inputGroups.add(new SimpleBEDFeature(151, 170, "1"));
		inputGroups.add(new SimpleBEDFeature(171, 190, "1"));
		inputGroups.add(new SimpleBEDFeature(191, 220, "1"));
		inputGroups.add(new SimpleBEDFeature(131, 150, "2"));
		inputGroups.add(new SimpleBEDFeature(151, 170, "2"));
		inputGroups.add(new SimpleBEDFeature(171, 190, "2"));
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Sublist of input should be returned.
		expectedOutputGroups.addAll(inputGroups.subList(6, 9));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record2);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}

	@Test
	public void testWithMultipleBedsEvenArrayLengthSixOnSameContigOfWhichSomeBeforeInAfterRecord()
	{
		// Prepares/executes bed with record matching.
		for (int i = 1; i < 3; i++)
		{
			String iStr = Integer.toString(i);

			inputGroups.add(new SimpleBEDFeature(61, 90, iStr));
			inputGroups.add(new SimpleBEDFeature(91, 120, iStr));
			inputGroups.add(new SimpleBEDFeature(121, 150, iStr));
			inputGroups.add(new SimpleBEDFeature(151, 180, iStr));
			inputGroups.add(new SimpleBEDFeature(181, 210, iStr));
			inputGroups.add(new SimpleBEDFeature(211, 240, iStr));
		}
		grouper = new SamRecordGroupsRetriever(inputGroups);

		// Sublist of input should be returned.
		expectedOutputGroups.addAll(inputGroups.subList(7, 11));

		// Executes and runs comparison.
		ArrayList<BEDFeature> actualOutputGroups = grouper.retrieveGroupsWithinRange(record2);
		compareActualBedWithExpectedBed(actualOutputGroups, expectedOutputGroups);
	}
}
