package org.molgenis.hadoop.pipeline.application.localtmpfiles;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileUtil;
import org.molgenis.hadoop.pipeline.application.processes.PipeRunner;

/**
 * Abstract class for creating a node-local temporary files on a Hadoop cluster and writing to it.
 * 
 * @param <T1>
 *            {@code extends} {@link Closeable} - The file writer to be used.
 * @param <T2>
 *            - The instance type to be used when giving data to the {@link HadoopLocalTmpFileWriter} for writing to the
 *            Hadoop tmp file.
 * 
 * @deprecated Still untested! Possible solution for creating local files if binaries tools would be supported in
 *             {@link PipeRunner} that do not support I/O streaming (but require a file as input).
 */
public abstract class HadoopLocalTmpFileWriter<T1 extends Closeable, T2> implements Closeable
{
	/**
	 * The created temporary file.
	 */
	private File file;

	/**
	 * The writer used to write to the file.
	 * 
	 * @see {@link T1}
	 */
	private T1 writer;

	File getFile()
	{
		return file;
	}

	T1 getWriter()
	{
		return writer;
	}

	void setWriter(T1 writer)
	{
		this.writer = writer;
	}

	/**
	 * Add a single {@code item} to the {@code writer}.
	 * 
	 * @param item
	 *            {@link T2}
	 */
	public abstract void add(T2 item);

	/**
	 * Add multiple {@code items} to the {@code writer}.
	 * 
	 * @param items
	 *            {@link Iterator}{@code <}{@link T2}{@code >}
	 */
	public void add(Iterator<T2> items)
	{
		while (items.hasNext())
		{
			add(items.next());
		}
	}

	/**
	 * Creates a new instance of a {@link HadoopLocalTmpFileWriter}.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @throws IOException
	 */
	public HadoopLocalTmpFileWriter(String fileName) throws IOException
	{
		file = FileUtil.createLocalTempFile(null, fileName, true);
	}

	@Override
	public void close() throws IOException
	{
		writer.close();
	}
}
