package org.molgenis.hadoop.pipeline.application.localtmpfiles;

import java.io.IOException;

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
public class HadoopLocalTmpBamFileWriter extends HadoopLocalTmpFileWriter<SAMFileWriter> implements SAMFileWriter
{
	/**
	 * Header of the bam file.
	 */
	SAMFileHeader header;

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
	 * Adds a {@link SAMRecord} to the tmp file.
	 */
	@Override
	public void addAlignment(SAMRecord alignment)
	{
		writer.addAlignment(alignment);
	}

	/**
	 * Adds a {@link SAMRecord} to the tmp file.
	 */
	public void addAlignment(SAMRecordWritable alignment)
	{
		writer.addAlignment(alignment.get());
	}

	/**
	 * Retrieve the {@link SAMFileHeader}.
	 */
	@Override
	public SAMFileHeader getFileHeader()
	{
		return header;
	}

	@Override
	public void setProgressLogger(ProgressLoggerInterface progress)
	{
		// No progress logger is used, so method does nothing.
	}

	@Override
	public void close()
	{
		// Overrides close() of TmpFileWriter as SAMFileWriter requires no exceptions to be thrown.
		writer.close();
	}
}
