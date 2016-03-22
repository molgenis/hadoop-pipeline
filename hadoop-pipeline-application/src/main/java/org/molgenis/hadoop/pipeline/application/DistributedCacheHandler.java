package org.molgenis.hadoop.pipeline.application;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.molgenis.hadoop.pipeline.application.inputdigestion.CommandLineInputParser;
import org.molgenis.hadoop.pipeline.application.inputdigestion.InputParser;

/**
 * Class for adding archives/files to the distributed cache from a Hadoop MapReduce Job and retrieval of these
 * archives/files.
 */
public class DistributedCacheHandler
{
	/**
	 * The object storing the files added to the distributed cache.
	 */
	private JobContext context;

	/**
	 * Stores whether the instance is created using a {@link Job} or not.
	 */
	private boolean isJob = false;

	/**
	 * Creates a new {@link DistributedCacheHandler} for handling the distributed cache of a Hadoop MapReduce
	 * {@link Job}. Using this initialization, files can be added and retrieved from the distributed cache.
	 * 
	 * @param context
	 *            {@link Job}
	 */
	DistributedCacheHandler(Job context)
	{
		this.context = requireNonNull(context);

		// Safety measure so the added JobContext is a Job which can be used to add files to the DistributedCache.
		isJob = true;
	}

	/**
	 * Creates a new {@link DistributedCacheHandler} for handling the distributed cache of a Hadoop MapReduce Job. Using
	 * this initialization, files can be retrieved from the distributed cache. When also wanting to add files to the
	 * distributed cache, use {@link #DistributedCacheHandler(Job)} instead.
	 * 
	 * @param context
	 *            {@link JobContext}
	 */
	public DistributedCacheHandler(JobContext context)
	{
		this.context = requireNonNull(context);
	}

	public boolean isJob()
	{
		return isJob;
	}

	/**
	 * Adds the needed archives/files which are stored in the {@link CommandLineInputParser} for the MapReduce
	 * {@link Job} to the distributed cache. Please view the source code for the actual order of the added cache
	 * archives/files.
	 * 
	 * @param parser
	 *            {@link InputParser}
	 */
	public void addCacheToJob(InputParser parser)
	{
		// Quick validation if the JobContext is a Job.
		if (!isJob) return;

		// Casts JobContext to usable Job.
		Job job = (Job) context;

		// IMPORTANT: input order defines position in array for retrieval!!!
		job.addCacheArchive(parser.getToolsArchiveLocation().toUri()); // [0]

		// IMPORTANT: input order defines position in array for retrieval!!!
		job.addCacheFile(parser.getAlignmentReferenceFastaFile().toUri()); // [0]
		job.addCacheFile(parser.getAlignmentReferenceFastaAmbFile().toUri()); // [1]
		job.addCacheFile(parser.getAlignmentReferenceFastaAnnFile().toUri()); // [2]
		job.addCacheFile(parser.getAlignmentReferenceFastaBwtFile().toUri()); // [3]
		job.addCacheFile(parser.getAlignmentReferenceFastaFaiFile().toUri()); // [4]
		job.addCacheFile(parser.getAlignmentReferenceFastaPacFile().toUri()); // [5]
		job.addCacheFile(parser.getAlignmentReferenceFastaSaFile().toUri()); // [6]
		job.addCacheFile(parser.getAlignmentReferenceDictFile().toUri()); // [7]
		job.addCacheFile(parser.getBedFile().toUri()); // [8]
		job.addCacheFile(parser.getSamplesInfoFile().toUri()); // [9]
	}

	/**
	 * {@link String} of the tools archive stored in {@link JobContext#getCacheArchives()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getToolsArchive() throws IOException
	{
		return getArchiveFromCache(0);
	}

	/**
	 * {@link String} of the information xml file from the tools archive stored in {@link #getToolsArchive()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 * @see {@link #getToolsArchive()}
	 */
	public String getInfoXmlFileFromToolsArchive() throws IOException
	{
		return getToolsArchive() + "/tools/info.xml";
	}

	/**
	 * {@link String} of the bwa tool from the tools archive stored in {@link #getToolsArchive()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 * @see {@link #getToolsArchive()}
	 */
	public String getBwaToolFromToolsArchive() throws IOException
	{
		return getToolsArchive() + "/tools/bwa";
	}

	/**
	 * {@link String} of the reference fasta file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getReferenceFastaFile() throws IOException
	{
		return getFileFromCache(0);
	}

	/**
	 * {@link String} of the reference dict file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getReferenceDictFile() throws IOException
	{
		return getFileFromCache(7);
	}

	/**
	 * {@link String} of the bed file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getBedFile() throws IOException
	{
		return getFileFromCache(8);
	}

	/**
	 * {@link String} of the samples information file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getSamplesInfoFile() throws IOException
	{
		return getFileFromCache(9);
	}

	/**
	 * Get the location of the archive added to the distributed cache on position {@code pos}.
	 * 
	 * @param pos
	 *            {@code int}
	 * @return {@link String} Archive path.
	 * @throws IOException
	 */
	private String getArchiveFromCache(int pos) throws IOException
	{
		return FilenameUtils.getName(context.getCacheArchives()[pos].toString());
	}

	/**
	 * Get the location of the file added to the distributed file on position {@code pos}.
	 * 
	 * @param pos
	 *            {@code int}
	 * @return {@link String} File path.
	 * @throws IOException
	 */
	private String getFileFromCache(int pos) throws IOException
	{
		return FilenameUtils.getName(context.getCacheFiles()[pos].toString());
	}
}
