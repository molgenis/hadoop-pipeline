package org.molgenis.hadoop.pipeline.application.localtmpfiles;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;

/**
 * Abstract class for creating/writing to node-local temporary files on a Hadoop cluster.
 * 
 * @param <K>
 *            extends {@link Closeable}
 */
public abstract class HadoopLocalTmpFileWriter<K extends Closeable> implements Closeable
{
	/**
	 * The created temporary file.
	 */
	File file;

	/**
	 * The writer used to write to the file.
	 */
	K writer;

	File getFile()
	{
		return file;
	}

	/**
	 * Creates a new instance of a {@link HadoopLocalTmpFileWriter}.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @throws IOException
	 */
	HadoopLocalTmpFileWriter(String fileName) throws IOException
	{
		file = FileUtil.createLocalTempFile(null, fileName, true);
	}

	@Override
	public void close() throws IOException
	{
		writer.close();
	}
}
