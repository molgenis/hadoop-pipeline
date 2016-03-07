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
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureSamRecordStartWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.tribble.bed.BEDFeature;

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

	TestDriver<?, ?, ?> getDriver()
	{
		return driver;
	}

	void setDriver(TestDriver<?, ?, ?> driver)
	{
		this.driver = driver;
	}

	/**
	 * Generates a {@code samFileHeader} that can be set for {@link SAMRecord}{@code s} which were serialized using the
	 * {@link SAMRecordWritable}. In this way, {@link SAMRecord#getSAMString()} can be used again.
	 */
	public static void generateSamFileHeader()
	{
		samFileHeader = new SAMFileHeader();
		samFileHeader
				.setSequenceDictionary(new SAMSequenceDictionary(Arrays.asList(new SAMSequenceRecord("1", 1000001))));
	}

	/**
	 * Sets the header created in {@link #generateSamFileHeader()} for the {@link SAMRecord}. If
	 * {@link #generateSamFileHeader()} was not called yet, do that first!
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
	 * Writes the pairs to stdout for manual validation.
	 * 
	 * @param pairsList
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 */
	void printOutput(List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> pairsList)
	{
		printOutput(pairsList, pairsList.size());
	}

	/**
	 * Writes the pairs to stdout for manual validation.
	 * 
	 * @param pairsList
	 *            {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *            {@link SAMRecordWritable}{@code >>}
	 * @param limit
	 *            {@code int} the number of pairs to write to stdout.
	 */
	void printOutput(List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> pairsList, int limit)
	{
		// If the limit is higher than the actual list size, resets the limit.
		if (limit > pairsList.size()) limit = pairsList.size();

		System.out.format("%-36s%s%n", "region", "SAMRecord");
		System.out.format("%-8s %-8s %-8s%n", "contig", "start", "end");

		// Prints the results.
		for (int i = 0; i < limit; i++)
		{
			BEDFeature group = pairsList.get(i).getFirst().get();
			SAMRecord record = pairsList.get(i).getSecond().get();
			// setHeaderForRecord(record); // Requires mocks?

			System.out.format("%8s:%8d:%8d%10s%s%n", group.getContig(), group.getStart(), group.getEnd(), "",
					record.getSAMString().trim());
		}
	}

	/**
	 * Generates the expected output data.
	 * 
	 * @param bwaOutput
	 *            {@link List}{@code <}{@link SAMRecord}{@code >} bwa output used to base expected output on.
	 * @param groups
	 *            {@link List}{@code <}{@link BEDFeature}{@code >} groups used for defining keys.
	 * @return {@link List}{@code <}{@link Pair}{@code <}{@link BedFeatureSamRecordStartWritable}{@code , }
	 *         {@link SAMRecordWritable} {@code >>}
	 */
	List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> generateExpectedMapperOutput(
			List<SAMRecord> bwaOutput, List<BEDFeature> groups)
	{
		List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> expectedMapperOutput = new ArrayList<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>>();

		for (SAMRecord record : bwaOutput)
		{
			for (BEDFeature group : groups)
			{
				if (record.getContig().equals(group.getContig())
						&& ((record.getStart() >= group.getStart() && record.getStart() <= group.getEnd())
								|| (record.getEnd() >= group.getStart() && record.getEnd() <= group.getEnd())))
				{
					SAMRecordWritable writable = new SAMRecordWritable();
					writable.set(record);
					expectedMapperOutput.add(new Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>(
							new BedFeatureSamRecordStartWritable(group, record), writable));
				}
			}
		}

		return expectedMapperOutput;
	}

	public List<SAMRecordWritable> filterMapperOutput(
			List<Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable>> mapperOutput,
			BedFeatureSamRecordStartWritable regionToKeepWritable)
	{
		ArrayList<SAMRecordWritable> recordsToKeep = new ArrayList<>();
		BEDFeature regionToKeep = regionToKeepWritable.get();

		for (Pair<BedFeatureSamRecordStartWritable, SAMRecordWritable> outputItem : mapperOutput)
		{
			BEDFeature recordRegion = outputItem.getFirst().get();
			SAMRecord record = outputItem.getSecond().get();
			if (recordRegion.getContig().equals(regionToKeep.getContig())
					&& recordRegion.getStart() == regionToKeep.getStart()
					&& recordRegion.getEnd() == regionToKeep.getEnd())
			{
				recordsToKeep.add(convertSamRecordToWritable(record));
			}
		}

		return recordsToKeep;
	}

	private SAMRecordWritable convertSamRecordToWritable(SAMRecord record)
	{
		SAMRecordWritable writable = new SAMRecordWritable();
		writable.set(record);
		return writable;
	}

	BedFeatureSamRecordStartWritable generateBedFeatureSamRecordStartWritable(BEDFeature region, int recordStart)
	{
		return new BedFeatureSamRecordStartWritable(region, generateSamRecordWithStart(recordStart));
	}

	private SAMRecord generateSamRecordWithStart(int recordStart)
	{
		SAMRecord record = new SAMRecord(null);
		record.setAlignmentStart(recordStart);
		return record;
	}

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
