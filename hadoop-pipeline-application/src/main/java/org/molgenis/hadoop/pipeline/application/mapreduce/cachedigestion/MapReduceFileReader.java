package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;

/**
 * Read a file that was added to {@link org.apache.hadoop.mapreduce.Job} using
 * {@link org.apache.hadoop.mapreduce.Job#addCacheArchive(java.net.URI)} or
 * {@link org.apache.hadoop.mapreduce.Job#addCacheFile(java.net.URI)} from within a
 * {@link org.apache.hadoop.mapreduce.Mapper} or {@link org.apache.hadoop.mapreduce.Reducer}.
 * 
 * @param <T>
 */
public abstract class MapReduceFileReader<T>
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
	 * Create a new {@link MapReduceFileReader} instance.
	 * 
	 * @param fileSys
	 *            {@link FileSystem}
	 */
	MapReduceFileReader(FileSystem fileSys)
	{
		this.fileSys = requireNonNull(fileSys);
	}

	/**
	 * Wrapper for {@link #readInputStream(InputStream)}.
	 * 
	 * @param file
	 *            {@link File}
	 * @return {@code T}
	 * @throws IOException
	 */
	public T readFile(File file) throws IOException
	{
		return readInputStream(FileUtils.openInputStream(file));
	}

	/**
	 * Wrapper for {@link #readFile(File)}.
	 * 
	 * @param path
	 *            {@link String}
	 * @return {@code T}
	 * @throws IOException
	 */
	public T readFile(String path) throws IOException
	{
		return readFile(new File(path));
	}

	/**
	 * Reads and digests an {@link InputStream}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @return {@code T}
	 * @throws IOException
	 */
	public abstract T readInputStream(InputStream inputStream) throws IOException;
}
