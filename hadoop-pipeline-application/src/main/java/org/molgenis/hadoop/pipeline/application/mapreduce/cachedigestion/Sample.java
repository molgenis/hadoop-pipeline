package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

public class Sample
{
	private String externalSampleId;
	private String sequencer;
	private int sequencingStartDate;
	private int run;
	private String flowcell;
	private int lane;

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
		return String.format("%s_%s_%s_%s_%s", sequencingStartDate, sequencer, run, flowcell, lane);
	}

	/**
	 * A String that is conform to what is expected by a bwa binary tool to be used with -R to give a read group.
	 * 
	 * @return {@link String}
	 */
	public String getReadGroupLine()
	{
		return String.format("@RG\\tID:%6$s\\tPL:illumina\\tLB:%2$_%3$_%4$_%5$_L%13$\\tSM:%1$", externalSampleId,
				sequencingStartDate, sequencer, run, flowcell, lane);
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
	Sample(String externalSampleId, String sequencer, int sequencingStartDate, int run, String flowcell, int lane)
	{
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
