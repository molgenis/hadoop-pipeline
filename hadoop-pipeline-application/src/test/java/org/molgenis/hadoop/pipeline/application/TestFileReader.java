package org.molgenis.hadoop.pipeline.application;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.molgenis.hadoop.pipeline.application.TestFile.TestFileType;
import org.molgenis.hadoop.pipeline.application.cachedigestion.HadoopBedFormatFileReader;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.tribble.bed.BEDFeature;
import htsjdk.tribble.bed.FullBEDFeature;

/**
 * Reads in test files for usage within unit testing. Do note that depending on the {@link TestFileType} of a
 * {@link TestFile}, some methods are not usable. As this class is reader for all possible test file formats, individual
 * methods check whether the given {@link TestFile} is of the correct {@link TestFileType) and throws an
 * {@link IOException} if it has an invalid {@link TestFileType}.
 */
public abstract class TestFileReader
{
	/**
	 * ClassLoader object to view test resource files. Test files can be retrieved using {@code getResource()}, where an
	 * empty {@link String} will refer to the folder {@code target/test-classes}.
	 */
	private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

	/**
	 * Reads in the defined file as a {@code byte[]}.
	 * 
	 * @param file
	 *            {@link TestFile}
	 * @return {@code byte[]}
	 * @throws IOException
	 */
	public static byte[] readFileAsByteArray(TestFile file) throws IOException
	{
		InputStream in = null;
		try
		{
			in = classLoader.getResource(file.getFilePath()).openStream();
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
	 * Reads in the defined file as an {@link ArrayList}{@code <}{@link SAMRecord}{@code >}.
	 * 
	 * IMPORTANT: {@link TestFile#getFileType()} must be {@link TestFileType#SAM}.
	 * 
	 * @param file
	 *            {@link TestFile}
	 * @return {@link ArrayList}{@code <}{@link SAMRecord}{@code >}
	 * @throws IOException
	 */
	public static ArrayList<SAMRecord> readSamFile(TestFile file) throws IOException
	{
		// Validates whether given test file is of correct format.
		if (file.getFileType() != TestFileType.SAM) throw new IOException();

		// Digests file.
		ArrayList<SAMRecord> records = new ArrayList<SAMRecord>();
		SamReader samReader = null;

		try
		{
			SamReaderFactory samReaderFactory = SamReaderFactory.makeDefault()
					.validationStringency(ValidationStringency.LENIENT);
			samReader = samReaderFactory
					.open(SamInputResource.of(classLoader.getResource(file.getFilePath()).openStream()));
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
	 * Reads in the defined file as a {@link ArrayList}{@code <}{@link BEDFeature}{@code >}. Conversion from 0-based
	 * bed-formatted file with an exclusive end to 1-based {@link BEDFeature} with an inclusive end is implemented by
	 * adding 1 to the {@link BEDFeature#getStart()}. For more information about the difference between a bed-formatted
	 * file and a {@link BEDFeature}, please view the Javadoc from {@link HadoopBedFormatFileReader#read(java.io.File)}.
	 * 
	 * IMPORTANT: {@link TestFile#getFileType()} must be {@link TestFileType#BED}.
	 * 
	 * @param file
	 *            {@link TestFile}
	 * @return {@link ArrayList}{@code <}{@link BEDFeature}{@code >}
	 * @throws IOException
	 * 
	 * @see {@link HadoopBedFormatFileReader#read(java.io.File)}
	 */
	public static ArrayList<BEDFeature> readBedFile(TestFile file) throws IOException
	{
		// Validates whether given test file is of correct format.
		if (file.getFileType() != TestFileType.BED) throw new IOException();

		// Digests file.
		ArrayList<BEDFeature> groups = new ArrayList<BEDFeature>();
		BufferedReader reader = null;
		try
		{
			reader = new BufferedReader(new FileReader(classLoader.getResource(file.getFilePath()).getFile()));

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
