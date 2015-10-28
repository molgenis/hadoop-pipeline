package org.molgenis.hadoop.pipeline.application.processes;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

/**
 * Aligner to execute a {@code BWA mem -p -M} alignment with.
 */
public class BwaAligner extends PipelineProcess
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(BwaAligner.class);

	/**
	 * Creates {@link BwaAligner} instance and builds the command line to be executed.
	 * 
	 * @param bwaTool
	 * @param alignmentReferenceFastaFile
	 */
	public BwaAligner(String bwaTool, String alignmentReferenceFastaFile)
	{
		requireNonNull(bwaTool);
		requireNonNull(alignmentReferenceFastaFile);

		buildArgumentList(bwaTool, alignmentReferenceFastaFile);

		// Make sure the call method casts/returns the same format as defined in the PipelineInFactory enum!!!
		setPipelineInFactory(PipelineInFactory.SAM);
	}

	/**
	 * Builds the command line to be executed.
	 * 
	 * @param bwaTool
	 * @param alignmentReferenceFastaFile
	 */
	private void buildArgumentList(String bwaTool, String alignmentReferenceFastaFile)
	{
		ArrayList<String> commandLineArguments = new ArrayList<String>();
		commandLineArguments.add(bwaTool);
		commandLineArguments.add("mem");
		commandLineArguments.add("-p");
		commandLineArguments.add("-M");
		commandLineArguments.add(alignmentReferenceFastaFile);
		commandLineArguments.add("-");
		setCommandLineArguments(commandLineArguments);

		logger.debug("bwa process command line: " + commandLineArguments);
	}

	/**
	 * Executes bwa alignment.
	 * 
	 * @return {@link String}
	 */
	@Override
	public SamInContainer call() throws IOException, InterruptedException
	{
		logger.info("Executing bwa alignment");

		// Make sure the call method casts/returns the same format as defined in the PipelineInFactory enum!!!
		return (SamInContainer) super.call();
	}
}
