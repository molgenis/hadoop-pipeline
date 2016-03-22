package org.molgenis.hadoop.pipeline.application.cachedigestion;

import static java.util.Objects.requireNonNull;

import htsjdk.samtools.SAMReadGroupRecord;

/**
 * Information about the data that represents a single {@link Sample}. Can be used to find which data belongs to which
 * sample and to add identifying information to this data. IMPORTANT: {@link SAMReadGroupRecord#setPlatform(String)} is
 * defined hardcoded!!!
 */
public class Sample
{
	/**
	 * The platform used to generate the samples. Currently hardcoded as illumina sequence outputs are expected in
	 * current pipeline.
	 */
	private static final String platform = "illumina";

	/**
	 * The sample external sample ID.
	 */
	private String externalSampleId;

	/**
	 * The used sequencer of the sample.
	 */
	private String sequencer;

	/**
	 * The sequencing start date.
	 */
	private int sequencingStartDate;

	/**
	 * The run number.
	 */
	private int run;

	/**
	 * The sample flowcell.
	 */
	private String flowcell;

	/**
	 * The lane number.
	 */
	private int lane; // Is the @RG ID.

	public String getExternalSampleId()
	{
		return externalSampleId;
	}

	public String getSequencer()
	{
		return sequencer;
	}

	public int getSequencingStartDate()
	{
		return sequencingStartDate;
	}

	public int getRun()
	{
		return run;
	}

	public String getFlowcell()
	{
		return flowcell;
	}

	public int getLane()
	{
		return lane;
	}

	/**
	 * Returns a {@link String} that can be used for comparing whether this {@link Sample} matches a file/directory name
	 * containing read data belonging to a {@link Sample}.
	 * 
	 * @return {@link String}
	 */
	public String getComparisonName()
	{
		return String.format("%1$s_%2$s_%3$4s_%4$s_L%5$s", sequencingStartDate, sequencer, run, flowcell, lane)
				.replace(' ', '0');
	}

	/**
	 * A String that is conform to what is expected as {@code @RG} within a sam file.
	 * 
	 * @return {@link String}
	 */
	public String getReadGroupLine()
	{
		return String.format("@RG\tID:%6$s\tPL:illumina\tLB:%2$s_%3$s_%4$s_%5$s_L%6$s\tSM:%1$s", externalSampleId,
				sequencingStartDate, sequencer, run, flowcell, lane);
	}

	/**
	 * A wrapper of {@link #getReadGroupLine()} where an extra {@code \} is added to the {@code \t} turning it into
	 * {@code \\t}. This allows it to be used safely within the application as argument within a {@link ProcessBuilder}
	 * (for example with the {@code -R} argument for a bwa binary executable).
	 * 
	 * @return {@link String}
	 */
	public String getSafeReadGroupLine()
	{
		return getReadGroupLine().replace("\t", "\\t");
	}

	/**
	 * Returns the sample as a {@link SAMReadGroupRecord}.
	 * 
	 * @return {@link SAMReadGroupRecord}
	 */
	public SAMReadGroupRecord getAsReadGroupRecord()
	{
		SAMReadGroupRecord record = new SAMReadGroupRecord(Integer.toString(lane));
		record.setPlatform(platform);
		record.setLibrary(
				String.format("%1$s_%2$s_%3$s_%4$s_L%5$s", sequencingStartDate, sequencer, run, flowcell, lane));
		record.setSample(externalSampleId);
		return record;
	}

	/**
	 * Create a new {@link Sample} instance.
	 * 
	 * @param externalSampleId
	 *            {@link String}
	 * @param sequencer
	 *            {@link String}
	 * @param sequencingStartDate
	 *            {@code int}
	 * @param run
	 *            {@code int}
	 * @param flowcell
	 *            {@link String}
	 * @param lane
	 *            {@code int}
	 */
	public Sample(String externalSampleId, String sequencer, int sequencingStartDate, int run, String flowcell,
			int lane)
	{
		requireNonNull(externalSampleId);
		requireNonNull(sequencer);
		requireNonNull(flowcell);

		this.externalSampleId = externalSampleId;
		this.sequencer = sequencer;
		this.sequencingStartDate = sequencingStartDate;
		this.run = run;
		this.flowcell = flowcell;
		this.lane = lane;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((externalSampleId == null) ? 0 : externalSampleId.hashCode());
		result = prime * result + ((flowcell == null) ? 0 : flowcell.hashCode());
		result = prime * result + lane;
		result = prime * result + run;
		result = prime * result + ((sequencer == null) ? 0 : sequencer.hashCode());
		result = prime * result + sequencingStartDate;
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Sample other = (Sample) obj;
		if (externalSampleId == null)
		{
			if (other.externalSampleId != null) return false;
		}
		else if (!externalSampleId.equals(other.externalSampleId)) return false;
		if (flowcell == null)
		{
			if (other.flowcell != null) return false;
		}
		else if (!flowcell.equals(other.flowcell)) return false;
		if (lane != other.lane) return false;
		if (run != other.run) return false;
		if (sequencer == null)
		{
			if (other.sequencer != null) return false;
		}
		else if (!sequencer.equals(other.sequencer)) return false;
		if (sequencingStartDate != other.sequencingStartDate) return false;
		return true;
	}
}
