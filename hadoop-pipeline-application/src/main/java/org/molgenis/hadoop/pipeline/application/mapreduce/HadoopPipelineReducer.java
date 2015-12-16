package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.tribble.bed.BEDFeature;

/**
 * Hadoop MapReduce Job reducer.
 */
public class HadoopPipelineReducer
		extends Reducer<BedFeatureWritable, SAMRecordWritable, NullWritable, SAMRecordWritable>
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPipelineReducer.class);

	/**
	 * Collector for reducer output.
	 */
	private MultipleOutputs<NullWritable, SAMRecordWritable> outputCollector;

	/**
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Initiate a new output collector.
		outputCollector = new MultipleOutputs<NullWritable, SAMRecordWritable>(context);
	}

	/**
	 * Function run on a key with an {@link Iterable} containing the values belonging to that key.
	 */
	@Override
	protected void reduce(BedFeatureWritable key, Iterable<SAMRecordWritable> values, Context context)
			throws IOException, InterruptedException
	{
		// Retrieve the BEDFeature from the Writable.
		BEDFeature bedFeature = key.get();

		// Writes the aligned SAMRecord data.
		Iterator<SAMRecordWritable> iterator = values.iterator();
		while (iterator.hasNext())
		{
			outputCollector.write(NullWritable.get(), iterator.next(), generateOutputFileName(bedFeature));
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
	 * Generates a {@link String} containing the start of the file name prefix to where the output should be written to
	 * (inside the output directory). The actual output file name might slightly differ however (for example by having
	 * something like {@code -r-<reducer number>} appended after the given name).
	 * 
	 * @param bedFeature
	 *            {@link BEDFeature} used to define the file name.
	 * @return {@link String} file name to be used.
	 */
	private String generateOutputFileName(BEDFeature bedFeature)
	{
		return bedFeature.getContig() + "-" + bedFeature.getStart() + "-" + bedFeature.getEnd();
	}
}
