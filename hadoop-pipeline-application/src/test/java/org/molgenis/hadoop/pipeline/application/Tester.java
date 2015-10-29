package org.molgenis.hadoop.pipeline.application;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.IOUtils;

/**
 * Superclass for the TestNG tests containing general code.
 */
public abstract class Tester
{
	/**
	 * ClassLoader object to view test resource files. Test files can be retrieved using {@code getResource()}, where an
	 * empty {@link String} will refer to the folder {@code target/test-classes}.
	 */
	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

	protected ClassLoader getClassLoader()
	{
		return classLoader;
	}

	/**
	 * Reads in the defined file as a {@code byte array}.
	 * 
	 * @param fileName
	 * @return {@code byte[]}
	 * @throws IOException
	 */
	protected byte[] readFileAsByteArray(String fileName) throws IOException
	{
		InputStream in = getClassLoader().getResource(fileName).openStream();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, baos, 10240);
		return baos.toByteArray();
	}

}
