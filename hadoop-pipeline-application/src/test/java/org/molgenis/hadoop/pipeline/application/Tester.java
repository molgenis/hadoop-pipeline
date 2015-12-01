package org.molgenis.hadoop.pipeline.application;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion.MapReduceBedFormatFileReader;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

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
			IOUtils.copy(in, baos);
			return baos.toByteArray();
		}
		finally
		{
			IOUtils.closeQuietly(in);
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
			IOUtils.closeQuietly(samReader);
		}
	}

	/**
	 * Reads in the defined file as a {@link ArrayList}{@code <}{@link SAMRecord}{@code >}. Conversion from 0-based
	 * bed-formatted file with an exclusive end to 1-based {@link BEDFeature} with an inclusive end is implemented by
	 * adding 1 to the {@link BEDFeature#getStart()}. For more information about the difference between a bed-formatted
	 * file and a {@link BEDFeature}, please view the Javadoc from
	 * {@link MapReduceBedFormatFileReader#read(java.io.File)}.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @return {@code byte[]}
	 * @throws IOException
	 * 
	 * @see {@link MapReduceBedFormatFileReader#read(java.io.File)}
	 */
	protected ArrayList<BEDFeature> readBedFile(String fileName) throws IOException
	{
		ArrayList<BEDFeature> groups = new ArrayList<BEDFeature>();
		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new FileReader(getClassLoader().getResource(fileName).getFile()));

			String line;
			while ((line = reader.readLine()) != null)
			{
				String[] splits = line.split("\\t");
				// +1 for start: From 0-based exclusive end to 1-based inclusive end. See
				groups.add(new FullBEDFeature(splits[0], Integer.parseInt(splits[1]) + 1, Integer.parseInt(splits[2])));
			}
		}
		finally
		{
			IOUtils.closeQuietly(reader);
		}

		return groups;
	}
}
