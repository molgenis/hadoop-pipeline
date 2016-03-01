package org.molgenis.hadoop.pipeline.application.localtmpfiles;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.molgenis.hadoop.pipeline.application.cachedigestion.SamFileHeaderGenerator;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.ProgressLoggerInterface;

/**
 * Creates a bam file locally on the node.
 */
public class HadoopLocalTmpBamFileWriter extends HadoopLocalTmpFileWriter<SAMFileWriter, SAMRecord>
		implements SAMFileWriter, WritableWriter<SAMRecordWritable>
{
	/**
	 * Header of the bam file.
	 */
	SAMFileHeader header;

	@Override
	public SAMFileHeader getFileHeader()
	{
		return header;
	}

	/**
	 * Generates a new {@link HadoopLocalTmpBamFileWriter} instance.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @param context
	 *            {@link TaskAttemptContext}
	 * @throws IOException
	 */
	public HadoopLocalTmpBamFileWriter(String fileName, TaskAttemptContext context) throws IOException
	{
		super(fileName);
		header = SamFileHeaderGenerator.retrieveSamFileHeader(context);
		SAMFileWriterFactory factory = new SAMFileWriterFactory();
		writer = factory.makeBAMWriter(header, false, file);
	}

	/**
	 * Add a {@link SAMRecord} to the {@link SAMFileWriter}. Does exactly the same as {@link #add(SAMRecord)}, but is
	 * added to be conform with {@link SAMFileWriter}.
	 */
	@Override
	public void addAlignment(SAMRecord alignment)
	{
		add(alignment);
	}

	/**
	 * Add a {@link SAMRecord} to the {@link SAMFileWriter}.
	 */
	@Override
	public void add(SAMRecord item)
	{
		writer.addAlignment(item);
	}

	/**
	 * Add a {@link SAMRecordWritable} to the {@link SAMFileWriter}.
	 */
	@Override
	public void addWritable(SAMRecordWritable item)
	{
		add(item.get());
	}

	/**
	 * Add multiple {@link SAMRecordWritable}{@code s} to the {link SAMFileWriter}.
	 */
	@Override
	public void addWritables(Iterator<SAMRecordWritable> items)
	{
		while (items.hasNext())
		{
			addWritable(items.next());
		}
	}

	@Override
	public void setProgressLogger(ProgressLoggerInterface progress)
	{
		writer.setProgressLogger(progress);
	}

	@Override
	public void close()
	{
		// Overrides close() of HadoopLocalTmpFileWriter as SAMFileWriter requires no exceptions to be thrown.
		writer.close();
	}
}
