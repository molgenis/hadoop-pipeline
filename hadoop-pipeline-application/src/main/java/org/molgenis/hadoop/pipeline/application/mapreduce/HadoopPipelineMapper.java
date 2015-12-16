package org.molgenis.hadoop.pipeline.application.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.molgenis.hadoop.pipeline.application.HadoopPipelineApplication;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HadoopBedFormatFileReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HadoopSamplesInfoFileReader;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HdfsFileMetaDataHandler;
import org.molgenis.hadoop.pipeline.application.cachedigestion.Sample;
import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.SamRecordSink;
import org.molgenis.hadoop.pipeline.application.processes.PipeRunner;
import org.molgenis.hadoop.pipeline.application.writables.BedFeatureWritable;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMRecord;
import htsjdk.tribble.bed.BEDFeature;

/**
 * Hadoop MapReduce Job mapper.
 */
public class HadoopPipelineMapper extends Mapper<Text, BytesWritable, BedFeatureWritable, SAMRecordWritable>
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(HadoopPipelineMapper.class);

	/**
	 * BwaTool executable location.
	 */
	private String bwaTool;

	/**
	 * Alignment reference fasta file location (with index file having the same prefix and being in the same directory.
	 */
	private String alignmentReferenceFastaFile;

	/**
	 * Allows retrieval of the groups to which a specific {@link SAMRecord} belongs to (based upon the area the
	 * {@link SAMRecord} was aligned to on the reference data compared to a BED file stored start/end regions).
	 */
	private SamRecordGroupsRetriever groupsRetriever;

	/**
	 * The possible samples an input split can belong to.
	 */
	private ArrayList<Sample> samples;

	/**
	 * Function called at the beginning of a task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		digestCache(context);
	}

	/**
	 * Function run on individual chunks of the data.
	 */
	@Override
	public void map(final Text key, BytesWritable value, final Context context) throws IOException, InterruptedException
	{
		// Only digests an input split if it is an ".fq.gz" file that starts with "halvade_" in the filename.
		// Non-".fq.gz" files will simply be ignored while ".fq.gz" files that start with a different name will cause an
		// IOException.
		if (validateInputFileType(key.toString()))
		{
			// Retrieve the sample belonging to the input split.
			Sample sample = retrieveCorrectSample(key.toString());

			SamRecordSink sink = new SamRecordSink()
			{
				@Override
				public void digestStreamItem(SAMRecord item) throws IOException
				{
					try
					{
						// Creates value.
						SAMRecordWritable samWritable = new SAMRecordWritable();
						samWritable.set(item);

						// Retrieves all keys for the value.
						ArrayList<BEDFeature> groups = groupsRetriever.retrieveGroupsWithinRange(item);

						// Writes a key-value pair for each key found that matched with the SAMRecord alignment
						// position.
						for (BEDFeature key : groups)
						{
							context.write(new BedFeatureWritable(key), samWritable);
						}
					}
					catch (InterruptedException e)
					{
						throw new RuntimeException(e);
					}
				}
			};

			logger.debug("Executing pipeline with input split: \"" + key.toString() + "\" and read group line \""
					+ sample.getReadGroupLine() + "\".");
			PipeRunner.startPipeline(value.getBytes(), sink, new ProcessBuilder(bwaTool, "mem", "-p", "-M", "-R",
					sample.getSafeReadGroupLine(), alignmentReferenceFastaFile, "-").start());
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
		bwaTool = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheArchives()[0])) + "/tools/bwa";
		alignmentReferenceFastaFile = HdfsFileMetaDataHandler.retrieveFileName(context.getCacheFiles()[0]);

		// Retrieves the groups stored in the bed-file which can be used for SAMRecord grouping.
		String bedFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[8]));
		ArrayList<BEDFeature> possibleGroups = new HadoopBedFormatFileReader(
				FileSystem.get(context.getConfiguration())).read(bedFile);
		groupsRetriever = new SamRecordGroupsRetriever(possibleGroups);

		// Retrieves the samples stored in the samples information file.
		String samplesInfoFile = HdfsFileMetaDataHandler.retrieveFileName((context.getCacheFiles()[9]));
		samples = new HadoopSamplesInfoFileReader(FileSystem.get(context.getConfiguration())).read(samplesInfoFile);
	}

	/**
	 * Checks whether an input split is a valid halvade input chunk.
	 * 
	 * @param inputSplitPath
	 *            {@link String}
	 * @return {@code true} if input split is a file with a name that starts with "halvade_" and ends with ".fq.gz",
	 *         false if it has a file extension different to ".fq.gz".
	 * @throws IOException
	 *             if the given input split is a ".fq.gz" file but does not start with "halvade_", throws an
	 *             {@link Exception) as safety measure as the to-be-digested could be invalid due to being wrongly
	 *             uploaded (or some other reason that should result in the file not being processed).
	 */
	private boolean validateInputFileType(String inputSplitPath) throws IOException
	{
		// Retrieves the file name.
		String[] pathParts = inputSplitPath.split("/");
		String fileName = pathParts[pathParts.length - 1];

		// If a .fq.gz file is found that starts with a different name than expected, throws an Exception.
		if (fileName.endsWith(".fq.gz") && !fileName.startsWith("halvade_"))
		{
			throw new IOException("Invalid .fq.gz file found: " + inputSplitPath);
		}

		// Non-".fq.gz" files return false.
		if (!fileName.endsWith(".fq.gz"))
		{
			return false;
		}

		//  Otherwise returns true.
		return true;
	}

	/**
	 * Returns the first found {@link Sample} that matches to the current input split (only one match should be present
	 * in {@code this.samples}).
	 * 
	 * @param inputSplitPath
	 *            {@link String}
	 * @return {@link Sample} the {@link Sample} that matches with the {@code inputSplitPath}.
	 * @throws IOException
	 *             if no {@link Sample} could be found within {@code this.samples} that matches the
	 *             {@code inputSplitPath}.
	 */
	private Sample retrieveCorrectSample(String inputSplitPath) throws IOException
	{
		// Splits the path into individual directories with the last item being the file name.
		String[] pathParts = inputSplitPath.split("/");

		// Goes through each available sample for comparison.
		for (Sample sample : samples)
		{
			// If the sample comparison name equals the last directory in the input split path,
			// returns the sample.
			if (sample.getComparisonName().equals(pathParts[pathParts.length - 2]))
			{
				return sample;
			}
		}
		// If no matching sample was found, throws an Exception.
		throw new IOException("Incorrectly named path or samplesheet missing information about: " + inputSplitPath);
	}
}
