package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.MapReduceRefSeqDictReader;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.MapReduceToolsXmlReader;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.Tool;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.tribble.bed.BEDFeature;

/**
 * Hadoop MapReduce Job reducer.
 */
public class HadoopPipelineReducer extends Reducer<BedFeatureWritable, SAMRecordWritable, NullWritable, Text>
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPipelineReducer.class);

	/**
	 * Collector for reducer output.
	 */
	private MultipleOutputs<NullWritable, Text> outputCollector;

	/**
	 * Stores the generated {@link SAMFileHeader}.
	 */
	private SAMFileHeader samFileHeader = new SAMFileHeader();

	/**
	 * Stores the read group line that should be added to the bwa alignment, if present.
	 */
	private String readGroupLine;

	/**
	 * Stores the tools that have been used in this Job and that should be added as {@code @PG} tags to the
	 * SAM-formatted file.
	 */
	private ArrayList<Tool> usedTools;

	/**
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Initiate a new output collector.
		outputCollector = new MultipleOutputs<NullWritable, Text>(context);

		// Digests cache.
		digestCache(context);
	}

	/**
	 * Function run on a single key with an {@link Iterable} containing the values belonging to that key.
	 */
	@Override
	protected void reduce(BedFeatureWritable key, Iterable<SAMRecordWritable> values, Context context)
			throws IOException, InterruptedException
	{
		// Retrieve the BEDFeature from the Writable.
		BEDFeature bedFeature = key.get();

		// Writes the @SQ tags to the output collector.
		for (SAMSequenceRecord samSeq : samFileHeader.getSequenceDictionary().getSequences())
		{
			String SqString = new String("@SQ\tSN:" + samSeq.getSequenceName() + "\tLN:" + samSeq.getSequenceLength());
			outputCollector.write("output", NullWritable.get(), new Text(SqString), generateOutputFileName(bedFeature));
		}

		// Writes the @RG tag to output collector, if the @RG tag is present.
		if (readGroupLine != null)
		{
			outputCollector.write(NullWritable.get(), new Text(readGroupLine), generateOutputFileName(bedFeature));
		}

		// Writes @PG tags.
		for (Tool usedTool : usedTools)
		{
			outputCollector.write(NullWritable.get(), new Text(usedTool.getSamString()),
					generateOutputFileName(bedFeature));
		}

		// Writes the aligned SAMRecord data.
		Iterator<SAMRecordWritable> iterator = values.iterator();
		while (iterator.hasNext())
		{
			SAMRecordWritable samWritable = iterator.next();
			SAMRecord record = samWritable.get();
			record.setHeader(samFileHeader);

			outputCollector.write(NullWritable.get(), new Text(record.getSAMString().trim()),
					generateOutputFileName(bedFeature));
		}
	}

	/**
	 * Function called at the end of a task.
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		outputCollector.close();
	}

	/**
	 * Digests the cache files that are needed into the required formats.
	 * 
	 * IMPORTANT: Be sure the exact same array order is used as defined in {@link HadoopPipelineApplication}!
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void digestCache(Context context) throws IllegalArgumentException, IOException
	{
		// Adds @SQ tags data to the SAMFileHeader.
		String alignmentReferenceDictFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[7]));
		SAMSequenceDictionary seqDict = new MapReduceRefSeqDictReader(FileSystem.get(context.getConfiguration()))
				.read(alignmentReferenceDictFile);
		samFileHeader.setSequenceDictionary(seqDict);

		// Retrieves the @RG tag String, if present.
		readGroupLine = context.getConfiguration().get("input_readgroupline");

		// Retrieves the tools data stored in the tools archive info.xml file.
		String toolsArchiveInfoXml = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheArchives()[0]))
				+ "/tools/info.xml";
		HashMap<String, Tool> tools = new MapReduceToolsXmlReader(FileSystem.get(context.getConfiguration()))
				.read(toolsArchiveInfoXml);

		// Filters the tools present in the tools archive info.xml file for the used tools by this Job.
		usedTools = new ArrayList<Tool>();
		usedTools.add(tools.get("bwa"));
	}

	/**
	 * Generates a {@link String} containing the subdirectory path/part of the filename to where the output is written
	 * to (inside the output directory). Default {@link OutputFormat}{@code s} might add additional text to this path
	 * such as adding something like {@code -m-<mapper number>} or {@code -r-<reducer number>} or similar after each
	 * file.
	 * 
	 * @param bedFeature
	 *            {@link BEDFeature}
	 * @return {@link String}
	 */
	private String generateOutputFileName(BEDFeature bedFeature)
	{
		return bedFeature.getContig() + "-" + bedFeature.getStart() + "-" + bedFeature.getEnd();
	}
}
