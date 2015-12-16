package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
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
	 * Function run on a single key with an {@link Iterable} containing the values belonging to that key.
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
