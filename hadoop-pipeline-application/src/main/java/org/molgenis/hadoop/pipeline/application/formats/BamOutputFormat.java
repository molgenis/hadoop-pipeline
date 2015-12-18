package org.molgenis.hadoop.pipeline.application.formats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HadoopRefSeqDictReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HadoopSamplesInfoFileReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HadoopToolsXmlReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Sample;
import org.seqdoop.hadoop_bam.BAMOutputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMRecordWriter;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMProgramRecord;
import htsjdk.samtools.SAMSequenceDictionary;

/**
 * Custom implementation replacing {@link KeyIgnoringBAMOutputFormat} where the {@link SAMFileHeader} is generated using
 * the {@link TaskAttemptContext} from {@link #getRecordWriter(TaskAttemptContext)}. This means the
 * {@link SAMFileHeader} is generated at the moment a {@link RecordWriter} is retrieved from the {@link OutputFormat},
 * though this allows the distributed cache to be used for generating the {@link SAMFileHeader}.
 * 
 * @param <K>
 */
public class BamOutputFormat<K> extends BAMOutputFormat<K>
{
	@Override
	public RecordWriter<K, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException
	{
		return new KeyIgnoringBAMRecordWriter<K>(getDefaultWorkFile(ctx, ""), retrieveSamFileHeader(ctx), true, ctx);
	}

	/**
	 * Retrieves the {@link SAMFileHeader} using the cache files. When adjusting the Mapper/Reducer, a manual validation
	 * of this method is required to see if it is still up-to-date!!!
	 * 
	 * IMPORTANT: Be sure the exact same array order is used as defined in {@link HadoopPipelineApplication}!
	 * 
	 * @param context
	 *            {@link TaskAttemptContext}
	 * @return {@link SAMFileHeader}
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private SAMFileHeader retrieveSamFileHeader(TaskAttemptContext context) throws IllegalArgumentException, IOException
	{
		SAMFileHeader samFileHeader = new SAMFileHeader();

		// Adds @SQ tags data to the SAMFileHeader.
		String alignmentReferenceDictFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[7]));
		SAMSequenceDictionary seqDict = new HadoopRefSeqDictReader().read(alignmentReferenceDictFile);
		samFileHeader.setSequenceDictionary(seqDict);

		// Retrieves the tools data stored in the tools archive info.xml file.
		String toolsArchiveInfoXml = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheArchives()[0]))
				+ "/tools/info.xml";
		HashMap<String, SAMProgramRecord> tools = new HadoopToolsXmlReader().read(toolsArchiveInfoXml);

		// Add the @PG tags for the tools within the tools archive that were used (define manually!!!).
		samFileHeader.addProgramRecord(tools.get("bwa"));

		// Retrieves the samples stored in the samples information file and adds them as SAMReadGroupRecords (@RG tags).
		String samplesInfoFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[9]));
		ArrayList<Sample> samples = new HadoopSamplesInfoFileReader().read(samplesInfoFile);
		for (Sample sample : samples)
		{
			samFileHeader.addReadGroup(sample.getAsReadGroupRecord());
		}

		// Returns the completed SAMFileHeader.
		return samFileHeader;
	}
}