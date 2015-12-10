package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.StringSink;

public class MapReduceSamplesInfoFileReader extends MapReduceFileReader<ArrayList<Sample>>
{
	/**
	 * Create a new {@link MapReduceSamplesInfoFileReader} instance.
	 * 
	 * @param fileSys
	 *            {@link FileSystem}
	 */
	public MapReduceSamplesInfoFileReader(FileSystem fileSys)
	{
		super(fileSys);
	}

	@Override
	public ArrayList<Sample> read(InputStream inputStream) throws IOException
	{
		// Stores the samples.
		final ArrayList<Sample> samples = new ArrayList<Sample>();

		StringSink sink = new StringSink()
		{
			/**
			 * Csv externalSampleID tag column.
			 */
			Integer externalSampleIdPos;

			/**
			 * Csv sequencer tag column.
			 */
			Integer sequencerPos;

			/**
			 * Csv sequencingStartDate tag column.
			 */
			Integer sequencingStartDatePos;

			/**
			 * Csv run tag column.
			 */
			Integer runPos;

			/**
			 * Csv flowcell tag column.
			 */
			Integer flowcellPos;

			/**
			 * Csv lane tag column.
			 */
			Integer lanePos;

			@Override
			public void digestStreamItem(String item) throws IOException
			{
				try
				{
					// Splits the line
					String[] lineSplits = item.split(",");

					// Retrieves data from columns of interest.
					String externalSampleId = lineSplits[externalSampleIdPos];
					String sequencer = lineSplits[sequencerPos];
					int sequencingStartDate = Integer.parseInt(lineSplits[sequencingStartDatePos]);
					int run = Integer.parseInt(lineSplits[runPos]);
					String flowcell = lineSplits[flowcellPos];
					int lane = Integer.parseInt(lineSplits[lanePos]);

					// Adds the data to the ArrayList.
					samples.add(new Sample(externalSampleId, sequencer, sequencingStartDate, run, flowcell, lane));
				}
				catch (NumberFormatException | ArrayIndexOutOfBoundsException e)
				{
					throw new IOException(e);
				}
			}

			/**
			 * Digests the header of a comma-seperated csv file containing sample information. Looks whether all vital
			 * header fields are present (case-insensitive) and stores their column number for usage in
			 * {@link #digestStreamItem(String)}.
			 * 
			 * @throws IOException
			 *             if not all vital header fields are present.
			 */
			@Override
			protected void digestHeader(String item) throws IOException
			{
				// Splits the line
				String[] lineSplits = item.split(",");

				for (int i = 0; i < lineSplits.length; i++)
				{
					switch (lineSplits[i].toLowerCase())
					{
						case "externalsampleid":
							externalSampleIdPos = i;
							break;
						case "sequencer":
							sequencerPos = i;
							break;
						case "sequencingstartdate":
							sequencingStartDatePos = i;
							break;
						case "run":
							runPos = i;
							break;
						case "flowcell":
							flowcellPos = i;
							break;
						case "lane":
							lanePos = i;
					}
				}
				// Throws an exception if the header is missing vital fields.
				if (sequencerPos == null || sequencingStartDatePos == null || runPos == null || flowcellPos == null
						|| lanePos == null)
				{
					throw new IOException("Header line missing fields.");
				}
			}
		};

		sink.handleInputStream(inputStream);

		return samples;
	}

}
