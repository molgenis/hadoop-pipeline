package org.molgenis.hadoop.pipeline.application;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;

/**
 * Superclass for the TestNG tests containing general code.
 */
public abstract class Tester
{
	/**
	 * ClassLoader object to view test resource files. Test files can be retrieved using {@code getResource()}, where an
	 * empty {@link String} will refer to the folder {@code target/test-classes}.
	 */
	private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

	protected static ClassLoader getClassLoader()
	{
		return classLoader;
	}

	/**
	 * Reads in the defined file as a {@code byte[]}.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @return {@code byte[]}
	 * @throws IOException
	 */
	protected byte[] readFileAsByteArray(String fileName) throws IOException
	{
		InputStream in = null;
		try
		{
			in = getClassLoader().getResource(fileName).openStream();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, baos, 10240);
			return baos.toByteArray();
		}
		finally
		{
			IOUtils.closeStream(in);
		}
	}

	/**
	 * Reads in the defined file as a {@link ArrayList}{@code <}{@link SAMRecord}{@code >}.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @return {@code byte[]}
	 * @throws IOException
	 */
	protected ArrayList<SAMRecord> readSamFile(String fileName) throws IOException
	{
		ArrayList<SAMRecord> records = new ArrayList<SAMRecord>();
		SamReader samReader = null;

		try
		{
			SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault()
					.validationStringency(ValidationStringency.LENIENT);
			samReader = samReaderFactory.open(SamInputResource.of(getClassLoader().getResource(fileName).openStream()));
			SAMRecordIterator samIterator = samReader.iterator();
			while (samIterator.hasNext())
			{
				records.add(samIterator.next());
			}
			return records;
		}
		finally
		{
			IOUtils.closeStream(samReader);
		}
	}
}
