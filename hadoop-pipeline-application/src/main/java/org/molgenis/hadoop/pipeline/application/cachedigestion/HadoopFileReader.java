package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;

/**
 * Read a file that was added to the distributed cache of a {@link org.apache.hadoop.mapreduce.Job}.
 * 
 * @param <T>
 */
public abstract class HadoopFileReader<T>
{
	/**
	 * Wrapper for {@link #read(InputStream)}. The created {@link InputStream} is closed as well.
	 * 
	 * @param file
	 *            {@link File}
	 * @return {@code T}
	 * @throws IOException
	 * @see {@link #read(InputStream)
	 */
	public T read(File file) throws IOException
	{
		InputStream inputStream = FileUtils.openInputStream(file);
		T data = read(inputStream);
		inputStream.close();
		return data;
	}

	/**
	 * Wrapper for {@link #read(File)}.
	 * 
	 * @param path
	 *            {@link String}
	 * @return {@code T}
	 * @throws IOException
	 * @see {@link #read(File)
	 */
	public T read(String path) throws IOException
	{
		return read(new File(path));
	}

	/**
	 * Reads and digests an {@link InputStream}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @return {@code T}
	 * @throws IOException
	 */
	public abstract T read(InputStream inputStream) throws IOException;
}
