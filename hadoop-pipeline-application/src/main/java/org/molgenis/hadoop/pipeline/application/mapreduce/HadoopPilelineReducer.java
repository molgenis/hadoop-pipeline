package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.MapReduceRefSeqDictReader;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;

/**
 * Hadoop MapReduce Job reducer.
 */
public class HadoopPilelineReducer extends Reducer<Text, SAMRecordWritable, NullWritable, Text>
{
	/**
	 * Stores the generated {@link SAMFileHeader}.
	 */
	private SAMFileHeader samFileHeader = new SAMFileHeader();

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
		String alignmentReferenceDictFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[7]));

		SAMSequenceDictionary seqDict = new MapReduceRefSeqDictReader(FileSystem.get(context.getConfiguration()))
				.readFile(alignmentReferenceDictFile);

		samFileHeader.setSequenceDictionary(seqDict);
	}
}
