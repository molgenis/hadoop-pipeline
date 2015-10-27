package org.molgenis.hadoop.pipeline.application.processes;

import java.util.ArrayList;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;

/**
 * Container for storing {@link SAMRecord}{@code s}.
 */
public class SamInContainer implements InContainer
{
	/**
	 * Registers if during adding items a {@link CLassCastException} occurred.
	 */
	boolean classCastExceptionOccured = false;

	/**
	 * Stores the {@link SAMFileHeader} (if present).
	 */
	private SAMFileHeader header = null;

	/**
	 * Stores the {@link SAMRecord}{@code s}.
	 */
	private ArrayList<SAMRecord> records = new ArrayList<SAMRecord>();

	/**
	 * Adds a {@link SAMRecord} to the container. If {@code item} cannot be cast to a {@link SAMRecord}, sets
	 * {@code classCastExceptionOccured} to true.
	 */
	@Override
	public void add(Object item)
	{
		try
		{
			records.add((SAMRecord) item);
		}
		catch (ClassCastException e)
		{
			classCastExceptionOccured = true;
		}

	}

	/**
	 * Retrieve an {@link ArrayList} containing the lines as {@link SAMRecord}.
	 * 
	 * @return {@link ArrayList }{@code <}{@link SAMRecord}{@code >}
	 */
	@Override
	public ArrayList<SAMRecord> get()
	{
		return records;
	}

	/**
	 * Set a single {@link SAMFileHeader} (belonging to the {@link SAMRecord}{@code s}).
	 * 
	 * @param header
	 */
	public void setHeader(SAMFileHeader header)
	{
		this.header = header;
	}

	/**
	 * Returns the {@link SAMFileHeader} if set, otherwise returns {code null}.
	 * 
	 * @return {@link SAMFileHeader} if set, otherwise {@code null}
	 */
	public SAMFileHeader getHeader()
	{
		return header;
	}

	@Override
	public boolean isClassCastExceptionOccured()
	{
		return classCastExceptionOccured;
	}

	@Override
	public void clear()
	{
		header = null;
		records.clear();
		classCastExceptionOccured = false;
	}
}
