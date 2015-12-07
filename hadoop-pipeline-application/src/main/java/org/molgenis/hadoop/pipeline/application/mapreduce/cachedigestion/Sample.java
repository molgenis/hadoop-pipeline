package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

public class Sample
{
	private String sequencer;
	private int sequencingStartDate;
	private int run;
	private String flowcell;
	private int lane;

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
	 * Returns how the start of the expected file name should look like for this sample (after this part more text could
	 * be present in the file name).
	 * 
	 * @return {@link String}
	 */
	public String getFileName()
	{
		return String.format("%s_%s_%s_%s_%s", sequencingStartDate, sequencer, run, flowcell, lane);
	}

	Sample(String sequencer, int sequencingStartDate, int run, String flowcell, int lane)
	{
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
