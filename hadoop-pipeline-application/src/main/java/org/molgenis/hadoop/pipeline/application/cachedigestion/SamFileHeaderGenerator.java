package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.molgenis.hadoop.pipeline.application.DistributedCacheHandler;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileHeader.SortOrder;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMSequenceDictionary;

public abstract class SamFileHeaderGenerator
{
	/**
	 * Retrieves the {@link SAMFileHeader} using the cache files. When adjusting the Mapper/Reducer, a manual validation
	 * of this method is required to see if it is still up-to-date!!!
	 * 
	 * @param context
	 *            {@link TaskAttemptContext}
	 * @return {@link SAMFileHeader}
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static SAMFileHeader retrieveSamFileHeader(TaskAttemptContext context)
			throws IllegalArgumentException, IOException
	{
		DistributedCacheHandler cacheHandler = new DistributedCacheHandler(context);

		SAMFileHeader samFileHeader = new SAMFileHeader();

		// Adds @SQ tags data to the SAMFileHeader.
		String alignmentReferenceDictFile = cacheHandler.getReferenceDictFile();
		SAMSequenceDictionary seqDict = new HadoopRefSeqDictReader().read(alignmentReferenceDictFile);
		samFileHeader.setSequenceDictionary(seqDict);

		// Retrieves the tools data stored in the tools archive info.xml file.
		String toolsArchiveInfoXml = cacheHandler.getInfoXmlFileFromToolsArchive();
		Map<String, SAMProgramRecord> tools = new HadoopToolsXmlReader().read(toolsArchiveInfoXml);

		// Add the @PG tags for the tools within the tools archive that were used (define manually!!!).
		samFileHeader.addProgramRecord(tools.get("bwa"));

		// Retrieves the samples stored in the samples information file and adds them as SAMReadGroupRecords (@RG tags).
		String samplesInfoFile = cacheHandler.getSamplesInfoFile();
		List<Sample> samples = new HadoopSamplesInfoFileReader().read(samplesInfoFile);
		for (Sample sample : samples)
		{
			samFileHeader.addReadGroup(sample.getAsReadGroupRecord());
		}

		// Returns the completed SAMFileHeader.
		return samFileHeader;
	}

	public static SAMFileHeader retrieveSamFileHeader(TaskAttemptContext context, SortOrder order)
			throws IllegalArgumentException, IOException
	{
		SAMFileHeader header = retrieveSamFileHeader(context);
		header.setSortOrder(order);
		return header;
	}
}
