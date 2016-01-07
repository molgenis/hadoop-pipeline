package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.molgenis.hadoop.pipeline.application.inputstreamdigestion.StringSink;

/**
 * Reads a samples information file that was added to the distributed cache of a {@link org.apache.hadoop.mapreduce.Job}
 * .
 */
public class HadoopSamplesInfoFileReader extends HadoopFileReader<List<Sample>>
{
	/**
	 * Reads an {@link InputStream} and digests each line of it into a {@link Sample}, which is added to the
	 * {@link List} that is returned when done.
	 * 
	 * @return {@link List}{@code <}{@link Sample}{@code >}
	 */
	@Override
	public List<Sample> read(InputStream inputStream) throws IOException
	{
		// Stores the samples.
		final List<Sample> samples = new ArrayList<Sample>();

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

					// Adds the data to the List.
					samples.add(new Sample(externalSampleId, sequencer, sequencingStartDate, run, flowcell, lane));
				}
				catch (NumberFormatException | ArrayIndexOutOfBoundsException e)
				{
					throw new IOException(e);
				}
			}

			/**
			 * Digests the header of a comma-separated csv file containing sample information. Looks whether all vital
			 * header fields are present (case-insensitive) and stores their column number for usage in
			 * {@link #digestStreamItem(String)}.
			 * 
			 * @throws IOException
			 *             if not all required header fields are present.
			 */
			@Override
			protected void digestHeader(String item) throws IOException
			{
				retrieveFieldPositions(item);
				checkForMissingHeaderFields();
			}

			/**
			 * Looks for the position of each required header field and stores it into the appropriate class field from
			 * this object.
			 * 
			 * @param header
			 *            {@link String}
			 */
			private void retrieveFieldPositions(String header)
			{
				// Splits the line
				String[] lineSplits = header.split(",");

				// Retrieves positions of the required fields.
				for (int i = 0; i < lineSplits.length; i++)
				{
					if (SamplesInfoFileField.EXTERNALSAMPLEID.nameEquals(lineSplits[i]))
					{
						externalSampleIdPos = i;
					}
					else if (SamplesInfoFileField.SEQUENCER.nameEquals(lineSplits[i]))
					{
						sequencerPos = i;
					}
					else if (SamplesInfoFileField.SEQUENCINGSTARTDATE.nameEquals(lineSplits[i]))
					{
						sequencingStartDatePos = i;
					}
					else if (SamplesInfoFileField.RUN.nameEquals(lineSplits[i]))
					{
						runPos = i;
					}
					else if (SamplesInfoFileField.FLOWCELL.nameEquals(lineSplits[i]))
					{
						flowcellPos = i;
					}
					else if (SamplesInfoFileField.LANE.nameEquals(lineSplits[i]))
					{
						lanePos = i;
					}
				}
			}

			/**
			 * Checks whether there are any required fields of which no column position could be found and throws an
			 * {@link IOException} if this is the case.
			 * 
			 * @throws IOException
			 *             If a required field is missing.
			 */
			private void checkForMissingHeaderFields() throws IOException
			{
				List<SamplesInfoFileField> missingFields = new ArrayList<>();
				if (externalSampleIdPos == null) missingFields.add(SamplesInfoFileField.EXTERNALSAMPLEID);
				if (sequencerPos == null) missingFields.add(SamplesInfoFileField.SEQUENCER);
				if (sequencingStartDatePos == null) missingFields.add(SamplesInfoFileField.SEQUENCINGSTARTDATE);
				if (runPos == null) missingFields.add(SamplesInfoFileField.RUN);
				if (flowcellPos == null) missingFields.add(SamplesInfoFileField.FLOWCELL);
				if (lanePos == null) missingFields.add(SamplesInfoFileField.LANE);

				// Throws an IOException if 1 or more missing fields were found.
				if (missingFields.size() > 0)
				{
					throw new IOException(generateMissingHeaderFieldsErrorMessage(missingFields));
				}
			}

			/**
			 * Generates a {@link String} that can be used as error message containing the specific fields that are
			 * missing.
			 * 
			 * @param missingFields
			 *            {@link List}{@code <}{@link SamplesInfoFileField}{@code >} Should not be empty!
			 * @return {@link String}
			 */
			private String generateMissingHeaderFieldsErrorMessage(List<SamplesInfoFileField> missingFields)
			{
				StringBuilder errorString = new StringBuilder();
				errorString.append("Header line missing fields: ");

				// Quick check that no empty list was given (method should not be called with an empty list).
				if (missingFields.size() == 0) return errorString.toString();

				// Adds name of first missing field.
				errorString.append(missingFields.get(0).getName());

				// Adds names of other missing fields, if more than 1 fields are missing.
				if (missingFields.size() > 1)
				{
					for (int i = 1; i < missingFields.size() - 2; i++)
					{
						errorString.append(", ");
						errorString.append(missingFields.get(i).getName());
					}
					errorString.append(" and ");
					errorString.append(missingFields.get(missingFields.size() - 1).getName());
				}

				// Returns a finished String with all missing fields.
				return errorString.toString();
			}
		};

		sink.handleInputStream(inputStream);

		return samples;
	}
}
