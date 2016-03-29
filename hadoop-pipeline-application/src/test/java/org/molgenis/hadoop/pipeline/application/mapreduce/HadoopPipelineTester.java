package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.molgenis.hadoop.pipeline.application.DistributedCacheHandler;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Region;
import org.molgenis.hadoop.pipeline.application.cachedigestion.SamFileHeaderGenerator;
import org.molgenis.hadoop.pipeline.application.writables.RegionWithSortableSamRecordWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

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
	private static SAMFileHeader samFileHeader;

	/**
	 * Runs some initial vital code for static variables/methods.
	 */
	static
	{
		samFileHeader = new SAMFileHeader();
		samFileHeader
				.setSequenceDictionary(new SAMSequenceDictionary(Arrays.asList(new SAMSequenceRecord("1", 1000001))));
	}

	TestDriver<?, ?, ?> getDriver()
	{
		return driver;
	}

	void setDriver(TestDriver<?, ?, ?> driver)
	{
		this.driver = driver;
	}

	/**
	 * Sets the header created in {@link #generateSamFileHeader()} for the {@link SAMRecord}. As
	 * {@link SAMRecordWritable} removes the {@link SAMFileHeader} from each {@link SAMRecord} during serialization,
	 * some vital information needs to be added again before {@link SAMRecord#getSAMString()} can be used again. In the
	 * actual application this header information is generated using
	 * {@link SamFileHeaderGenerator#retrieveSamFileHeader(org.apache.hadoop.mapreduce.TaskAttemptContext)}. The
	 * {@link SAMRecord#getSAMString()} is the vital part that is used when generating the output files and I/O
	 * PipeRunner processes, so is the most important part to be checked whether it is valid.
	 *
	 * @param record
	 *            {@link SAMRecord}
	 */
	void setHeaderForRecord(SAMRecord record)
	{
		record.setHeader(samFileHeader);
	}

	/**
	 * Adds needed files to the {@link MapDriver} cache. Be sure the order is exactly the same as in
	 * {@link DistributedCacheHandler}!
	 * 
	 * @throws URISyntaxException
	 */
	void addCacheToDriver() throws URISyntaxException
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

	/**
	 * Writes key:value pairs representing the mapper output to stdout for manual validation.
	 * 
	 * @param pairsList
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	void printOutput(List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> pairsList)
	{
		printOutput(pairsList, pairsList.size());
	}

	/**
	 * Writes key:value pairs representing the mapper output to stdout for manual validation.
	 * 
	 * @param pairsList
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 * @param limit
	 *            {@code int} - The number of pairs to write to stdout.
	 */
	void printOutput(List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> pairsList, int limit)
	{
		// If the limit is higher than the actual list size, resets the limit.
		if (limit > pairsList.size()) limit = pairsList.size();

		System.out.format("%-36s%s%n", "region", "SAMRecord");
		System.out.format("%-8s %-8s %-8s%n", "contig", "start", "end");

		// Prints the results.
		for (int i = 0; i < limit; i++)
		{
			Region group = pairsList.get(i).getFirst().get();
			SAMRecord record = pairsList.get(i).getSecond().get();
			setHeaderForRecord(record);

			System.out.format("%8s:%8d:%8d%10s%s%n", group.getContig(), group.getStart(), group.getEnd(), "",
					record.getSAMString().trim());
		}
	}

	/**
	 * Generates the expected {@link HadoopPipelineMapper} output.
	 * 
	 * @param bwaOutput
	 *            {@link List}{@code <}{@link SAMRecord}{@code >} - The bwa output used to base expected output on.
	 * @param groups
	 *            {@link List}{@code <}{@link Region}{@code >} - The groups used for defining keys.
	 * @return {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *         {@link SAMRecordWritable} {@code >>}
	 */
	List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> generateExpectedMapperOutput(List<SAMRecord> bwaOutput,
			List<Region> regions)
	{
		// Stores the created expected output.
		List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedMapperOutput = new ArrayList<>();

		// Stores records of a single read and starts with the first record.
		ArrayList<SAMRecord> regionRecords = new ArrayList<>();
		regionRecords.add(bwaOutput.get(0));

		// Iterates through the other records.
		for (int i = 1; i < bwaOutput.size(); i++)
		{
			SAMRecord currentRecord = bwaOutput.get(i);

			// If the record is from the same read, adds it to regionRecords.
			if (currentRecord.getReadName().equals(regionRecords.get(0).getReadName()))
			{
				regionRecords.add(currentRecord);
			}

			// Otherwise, generate expected output from the currently stored records and start an empty regionRecords
			// starting with the current record.
			else
			{
				addRecordRegionsToExpectedMapperOutput(expectedMapperOutput, regions, regionRecords);
				regionRecords.clear();
				regionRecords.add(currentRecord);
			}
		}

		// If after the last iteration the regionRecords is not empty, creates expected output from it.
		if (!regionRecords.isEmpty())
		{
			addRecordRegionsToExpectedMapperOutput(expectedMapperOutput, regions, regionRecords);
		}

		return expectedMapperOutput;
	}

	/**
	 * Processes the given {@code regionRecords} using the {@code regions} and adds expected output to
	 * {@code expectedMapperOutput}.
	 * 
	 * @param expectedMapperOutput
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable} {@code >>} To which the expected output should be added to.
	 * @param regions
	 *            {@link List}{@code <}{@link Region}{@code >} Used for generating the expected output. Defines part of
	 *            the key for each output key-value pair.
	 * @param regionRecords
	 *            {@link List}{@code <}{@link SAMRecord}{@code >} The aligned records (from a single read pair) which
	 *            should be used together with the {@link Region}{@code s} for generating the expected output. Defines
	 *            the value value of a key-value pair and also a part of the key.
	 */
	private void addRecordRegionsToExpectedMapperOutput(
			List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedMapperOutput, List<Region> regions,
			ArrayList<SAMRecord> regionRecords)
	{
		// Stores the two primary records from the algined reads from a read pair.
		SAMRecord firstPrimaryRecord = null;
		SAMRecord secondPrimaryRecord = null;

		// Iterates through the records to look for the two primary records.
		for (SAMRecord regionRecord : regionRecords)
		{
			if (!regionRecord.isSecondaryOrSupplementary())
			{
				if (regionRecord.getFirstOfPairFlag()) firstPrimaryRecord = regionRecord;
				else secondPrimaryRecord = regionRecord;
			}
		}

		// If either primary record is unmapped, generates expected output with an unmapped region.
		if (firstPrimaryRecord.getReadUnmappedFlag() || secondPrimaryRecord.getReadUnmappedFlag())
		{
			addRecordsListToExpectedMapperOutput(expectedMapperOutput, regionRecords, Region.unmapped());
		}

		// If either record is mapped, retrieve all regions any of the records map to and generate expected output.
		if (!firstPrimaryRecord.getReadUnmappedFlag() || !secondPrimaryRecord.getReadUnmappedFlag())
		{
			List<Region> outputRegions = new ArrayList<>();

			// Look for regions any of the records match with.
			for (Region region : regions)
			{
				for (SAMRecord regionRecord : regionRecords)
				{
					// If a single record matches, adds it to the outputRegions and continue to the next Region.
					if (checkIfRecordIsInRegionRange(regionRecord, region))
					{
						outputRegions.add(region);
						break;
					}
				}
			}

			// Add the records with the regions to the expected output.
			for (Region outputRegion : outputRegions)
			{
				addRecordsListToExpectedMapperOutput(expectedMapperOutput, regionRecords, outputRegion);
			}

		}
	}

	/**
	 * Checks whether the given {@code record} is in range of the {@code region}.
	 * 
	 * @param outputRegions
	 *            {@link List}{@code <}{@link Region}{@code >}
	 * @param record
	 *            {@link SAMRecord}
	 * @param region
	 *            {@link Region}
	 * @return {@code boolean} - True if a {@code record} was added to {@code outputRegions}, otherwise false.
	 */
	private boolean checkIfRecordIsInRegionRange(SAMRecord record, Region region)
	{
		if ((record.getStart() >= region.getStart() && record.getStart() <= region.getEnd())
				|| (record.getEnd() >= region.getStart() && record.getEnd() <= region.getEnd()))
		{
			return true;
		}
		return false;
	}

	/**
	 * Adds all {@code records} to the {@code expectedMapperOutput}.
	 * 
	 * @param expectedMapperOutput
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable} {@code >>}
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecord}{@code >}
	 * @param region
	 *            {@link Region}
	 */
	private void addRecordsListToExpectedMapperOutput(
			List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedMapperOutput, List<SAMRecord> records,
			Region region)
	{
		for (SAMRecord record : records)
		{
			addRecordToExpectedMapperOutput(expectedMapperOutput, record, region);
		}
	}

	/**
	 * Adds the {code record} to the {@code expectedMapperOutput}.
	 * 
	 * @param expectedMapperOutput
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }
	 *            {@link SAMRecordWritable} {@code >>}
	 * @param record
	 *            {@link SAMRecord}
	 * @param region
	 *            {@link Region}
	 */
	private void addRecordToExpectedMapperOutput(
			List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> expectedMapperOutput, SAMRecord record,
			Region region)
	{
		SAMRecordWritable writable = new SAMRecordWritable();
		writable.set(record);
		expectedMapperOutput.add(new Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>(
				new RegionWithSortableSamRecordWritable(region, record), writable));
	}

	/**
	 * Filter out all key:value pairs from (expected) mapper output that do not match to the given
	 * {@code regionToKeepWritable}.
	 * 
	 * @param mapperOutput
	 *            {@link List}{@code <}{@link RegionWithSortableSamRecordWritable}{@code , }{@link SAMRecordWritable}
	 *            {@code >>}
	 * @param regionToKeep
	 *            {@link RegionWithSortableSamRecordWritable}
	 * @return {@link List}{@code <}{@link SAMRecordWritable}{@code >}
	 */
	public List<SAMRecordWritable> filterMapperOutput(
			List<Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable>> mapperOutput,
			RegionWithSortableSamRecordWritable regionToKeepWritable)
	{
		ArrayList<SAMRecordWritable> recordsToKeep = new ArrayList<>();
		Region regionToKeep = regionToKeepWritable.get();

		for (Pair<RegionWithSortableSamRecordWritable, SAMRecordWritable> outputItem : mapperOutput)
		{
			Region recordRegion = outputItem.getFirst().get();
			SAMRecord record = outputItem.getSecond().get();
			if (recordRegion.getContig().equals(regionToKeep.getContig())
					&& recordRegion.getStart() == regionToKeep.getStart()
					&& recordRegion.getEnd() == regionToKeep.getEnd())
			{
				SAMRecordWritable writable = new SAMRecordWritable();
				writable.set(record);
				recordsToKeep.add(writable);
			}
		}

		return recordsToKeep;
	}

	/**
	 * Generates a {@link RegionWithSortableSamRecordWritable}.
	 * 
	 * @param region
	 *            {@link Region}
	 * @param recordStart
	 *            {@code int} - Represents the start value from a {@link SAMRecord}.
	 * @return {@link RegionWithSortableSamRecordWritable}
	 */
	RegionWithSortableSamRecordWritable generateRegionSamRecordStartWritable(Region region, int recordStart)
	{
		return new RegionWithSortableSamRecordWritable(region, generateSamRecordWithStart(recordStart));
	}

	/**
	 * Generate a {@link SAMRecord} with only a value for {@link SAMRecord#getStart()}.
	 * 
	 * @param recordStart
	 *            {@code int} - Represents the start value from a {@link SAMRecord}.
	 * @return {@link SAMRecord}
	 */
	private SAMRecord generateSamRecordWithStart(int recordStart)
	{
		SAMRecord record = new SAMRecord(null);
		record.setAlignmentStart(recordStart);
		return record;
	}

	/**
	 * Generates the expected {@link HadoopPipelineReducer} output.
	 * 
	 * @param records
	 *            {@link List}{@code <}{@link SAMRecordWritable}{@code >}
	 * @return {@link List}{@code <}{@link Pair}{@code <}{@link NullWritable}{@code , }{@link SAMRecordWritable}
	 *         {@code >>}
	 */
	List<Pair<NullWritable, SAMRecordWritable>> generateExpectedReducerOutput(List<SAMRecordWritable> records)
	{
		List<Pair<NullWritable, SAMRecordWritable>> expectedReducerOutput = new ArrayList<Pair<NullWritable, SAMRecordWritable>>();
		for (SAMRecordWritable record : records)
		{
			expectedReducerOutput.add(new Pair<NullWritable, SAMRecordWritable>(NullWritable.get(), record));
		}

		return expectedReducerOutput;
	}
}
