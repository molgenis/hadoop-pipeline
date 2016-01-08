package org.molgenis.hadoop.pipeline.application;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.molgenis.hadoop.pipeline.application.inputdigestion.CommandLineInputParser;

/**
 * Class for adding archives/files to the distributed cache from a Hadoop MapReduce Job and retrieval of these
 * archives/files.
 */
public class DistributedCacheHandler
{
	/**
	 * The object storing the files added to the distributed cache.
	 */
	JobContext context;
	boolean isJob = false;

	/**
	 * Creates a new {@link DistributedCacheHandler} for handling the distributed cache of a Hadoop MapReduce Job. Using
	 * this initialization, files can be added and retrieved from the distributed cache.
	 * 
	 * @param context
	 *            {@link Job}
	 */
	DistributedCacheHandler(Job context)
	{
		this.context = context;

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
		this.context = context;
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
	 *            {@link CommandLineInputParser}
	 */
	public void addCacheToJob(CommandLineInputParser parser)
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
	 * {@link URI} of the tools archive stored in {@link JobContext#getCacheArchives()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getToolsArchive() throws IOException
	{
		return FilenameUtils.getName(context.getCacheArchives()[0].toString());
	}

	/**
	 * {@link String} of the information xml file from the tools archive stored in {@link JobContext#getCacheArchives()}
	 * .
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getInfoXmlFileFromToolsArchive() throws IOException
	{
		return getToolsArchive() + "/tools/info.xml";
	}

	/**
	 * {@link String} of the bwa tool from the tools archive stored in {@link JobContext#getCacheArchives()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
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
		return FilenameUtils.getName(context.getCacheFiles()[0].toString());
	}

	/**
	 * {@link String} of the reference dict file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getReferenceDictFile() throws IOException
	{
		return FilenameUtils.getName(context.getCacheFiles()[7].toString());
	}

	/**
	 * {@link String} of the bed file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getBedFile() throws IOException
	{
		return FilenameUtils.getName(context.getCacheFiles()[8].toString());
	}

	/**
	 * {@link String} of the samples information file stored in {@link JobContext#getCacheFiles()}.
	 * 
	 * @return {@link String}
	 * @throws IOException
	 */
	public String getSamplesInfoFile() throws IOException
	{
		return FilenameUtils.getName(context.getCacheFiles()[9].toString());
	}
}
