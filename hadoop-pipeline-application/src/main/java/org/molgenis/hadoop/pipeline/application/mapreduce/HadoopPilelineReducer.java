package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.MapReduceRefSeqDictReader;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;

/**
 * Hadoop MapReduce Job reducer.
 */
public class HadoopPilelineReducer extends Reducer<Text, SAMRecordWritable, NullWritable, Text>
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPilelineReducer.class);

	/**
	 * Stores the generated {@link SAMFileHeader}.
	 */
	private SAMFileHeader samFileHeader = new SAMFileHeader();

	/**
	 * Stores the read group line that should be added to the bwa alignment, if present.
	 */
	private String readGroupLine;

	/**
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		digestCache(context);
	}

	/**
	 * Function run on a single key with an {@link Iterable} containing the values belonging to that key.
	 */
	@Override
	protected void reduce(Text key, Iterable<SAMRecordWritable> values, Context context)
			throws IOException, InterruptedException
	{
		// Writes the @SQ tags to the context.
		for (SAMSequenceRecord samSeq : samFileHeader.getSequenceDictionary().getSequences())
		{
			context.write(NullWritable.get(),
					new Text("@SQ\t" + samSeq.getSequenceName() + "\t" + samSeq.getSequenceLength()));
		}

		// Writes the @RG tag to context, if it is present.
		// if (readGroupLine != null)
		// {
		// context.write(NullWritable.get(), new Text(readGroupLine));
		// }

		// Writes the aligned SAMRecord data to context.
		Iterator<SAMRecordWritable> iterator = values.iterator();
		while (iterator.hasNext())
		{
			SAMRecordWritable samWritable = iterator.next();
			SAMRecord record = samWritable.get();
			record.setHeader(samFileHeader);

			context.write(NullWritable.get(), new Text(record.getSAMString().trim()));
		}
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
				.readFile(alignmentReferenceDictFile);
		samFileHeader.setSequenceDictionary(seqDict);

		// Retrieves the @RG tag String, if present.
		// readGroupLine = context.getConfiguration().get("input_readgroupline");
	}
}
