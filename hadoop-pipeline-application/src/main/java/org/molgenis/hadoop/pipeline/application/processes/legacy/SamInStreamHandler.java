package org.molgenis.hadoop.pipeline.application.processes.legacy;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;

/**
 * Handles a sam-formatted stream retrieved from the process.
 */
public class SamInStreamHandler extends InStreamHandler
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(SamInStreamHandler.class);

	/**
	 * Replaces the superclass' default {@link InContainer} to be a {@link SamInContainer} instead.
	 */
	private SamInContainer inContainer;

	/**
	 * Initiates a new {@link SamInStreamHandler} instance.
	 * 
	 * @param processStream
	 * @param inContainer
	 */
	SamInStreamHandler(InputStream processStream, SamInContainer inContainer)
	{
		super(processStream);
		this.inContainer = requireNonNull(inContainer);
	}

	/**
	 * Digest a sam-formatted {@link InputStream}.
	 */
	@Override
	public void run()
	{
		SamReader samReader = null;
		try
		{
			SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault()
					.validationStringency(ValidationStringency.LENIENT);
			samReader = samReaderFactory.open(SamInputResource.of(getProcessStream()));
			inContainer.setHeader(samReader.getFileHeader());
			SAMRecordIterator samIterator = samReader.iterator();
			while (samIterator.hasNext())
			{
				inContainer.add(samIterator.next());
			}
		}
		finally
		{
			try
			{
				samReader.close();
			}
			catch (IOException e)
			{
				logger.error("An error occured while trying to close the SamReader.");
				logger.debug(ExceptionUtils.getFullStackTrace(e));
			}
		}
	}
}