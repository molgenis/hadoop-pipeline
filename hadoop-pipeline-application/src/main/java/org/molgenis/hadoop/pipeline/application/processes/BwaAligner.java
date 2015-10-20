package org.molgenis.hadoop.pipeline.application.processes;

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
		buildArgumentList(bwaTool, alignmentReferenceFastaFile);
	}

	/**
	 * Builds the command line to be executed.
	 * 
	 * @param bwaTool
	 * @param alignmentReferenceFastaFile
	 */
	private void buildArgumentList(String bwaTool, String alignmentReferenceFastaFile)
	{
		commandLineArguments = new ArrayList<String>();
		commandLineArguments.add(bwaTool);
		commandLineArguments.add("mem");
		commandLineArguments.add("-p");
		commandLineArguments.add("-M");
		commandLineArguments.add(alignmentReferenceFastaFile);
		commandLineArguments.add("-");

		logger.debug("bwa process command line: " + commandLineArguments);
	}

	/**
	 * Executes bwa alignment.
	 * 
	 * @return {@link String}
	 */
	@Override
	public String call() throws IOException, InterruptedException
	{
		logger.info("executing bwa alignment");
		return super.call();
	}
}
