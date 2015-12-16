package org.molgenis.hadoop.pipeline.application.cachedigestion;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;

/**
 * Read a file that was added to the distributed cache of a {@link org.apache.hadoop.mapreduce.Job}.
 * 
 * @param <T>
 */
public abstract class HadoopFileReader<T>
{
	/**
	 * Stores the {@link FileSystem} to be used for reading the file.
	 */
	private FileSystem fileSys;

	public FileSystem getFileSys()
	{
		return fileSys;
	}

	/**
	 * Create a new {@link HadoopFileReader} instance.
	 * 
	 * @param fileSys
	 *            {@link FileSystem}
	 */
	HadoopFileReader(FileSystem fileSys)
	{
		this.fileSys = requireNonNull(fileSys);
	}

	/**
	 * Wrapper for {@link #read(InputStream)}.
	 * 
	 * @param file
	 *            {@link File}
	 * @return {@code T}
	 * @throws IOException
	 */
	public T read(File file) throws IOException
	{
		return read(FileUtils.openInputStream(file));
	}

	/**
	 * Wrapper for {@link #read(File)}.
	 * 
	 * @param path
	 *            {@link String}
	 * @return {@code T}
	 * @throws IOException
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
