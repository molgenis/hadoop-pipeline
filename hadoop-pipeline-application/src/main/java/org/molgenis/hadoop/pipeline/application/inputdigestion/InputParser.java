package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Abstract parser for storing/validating information needed elsewhere in the application.
 */
public abstract class InputParser
{
	/**
	 * File system for validating existence files given as input.
	 */
	private FileSystem fileSys;

	/**
	 * Switch that only allows the MapReduce phase to start if all required files exist.
	 */
	private boolean continueApplication = false;

	/**
	 * Location of the .tar.gz archive containing the required tools.
	 */
	private Path toolsArchiveLocation;

	/**
	 * Location with input files.
	 */
	private Path inputDir;

	/**
	 * Location results can be written to.
	 */
	private Path outputDir;

	/**
	 * Location BWA index .fasta file for alignment.
	 */
	private Path alignmentReferenceFastaFile;

	/**
	 * Location BWA index .fasta.amb file for alignment.
	 */
	private Path alignmentReferenceFastaAmbFile;

	/**
	 * Location BWA index .fasta.ann file for alignment.
	 */
	private Path alignmentReferenceFastaAnnFile;

	/**
	 * Location BWA index .fasta.bwt file for alignment.
	 */
	private Path alignmentReferenceFastaBwtFile;

	/**
	 * Location BWA index .fasta.fai file for alignment.
	 */
	private Path alignmentReferenceFastaFaiFile;

	/**
	 * Location BWA index .fasta.pac file for alignment.
	 */
	private Path alignmentReferenceFastaPacFile;

	/**
	 * Location BWA index .fasta.sa file for alignment.
	 */
	private Path alignmentReferenceFastaSaFile;

	/**
	 * Location of the dictionary belonging to the BWA index files for alignment.
	 */
	private Path alignmentReferenceDictFile;

	/**
	 * Location to the BED file containing the grouping for the SAM records.
	 */
	private Path bedFile;

	/**
	 * The read group line that should be added to generated alignment SAM files.
	 */
	private Path samplesInfoFile;

	protected void setFileSys(FileSystem fileSys)
	{
		this.fileSys = fileSys;
	}

	public boolean isContinueApplication()
	{
		return continueApplication;
	}

	public Path getToolsArchiveLocation()
	{
		return toolsArchiveLocation;
	}

	protected void setToolsArchiveLocation(Path toolsArchiveLocation)
	{
		this.toolsArchiveLocation = toolsArchiveLocation;
	}

	protected void setToolsArchiveLocation(String toolsArchiveLocation)
	{
		this.toolsArchiveLocation = new Path(toolsArchiveLocation);
	}

	public Path getInputDir()
	{
		return inputDir;
	}

	protected void setInputDir(Path inputDir)
	{
		this.inputDir = inputDir;
	}

	protected void setInputDir(String inputDir)
	{
		this.inputDir = new Path(inputDir);
	}

	public Path getOutputDir()
	{
		return outputDir;
	}

	protected void setOutputDir(Path outputDir)
	{
		this.outputDir = outputDir;
	}

	protected void setOutputDir(String outputDir)
	{
		this.outputDir = new Path(outputDir);
	}

	public Path getAlignmentReferenceFastaFile()
	{
		return alignmentReferenceFastaFile;
	}

	/**
	 * Sets the {@code alignmentReferenceFastaFile} and associated files (.amb, .ann, .bwt, .fai, .pac & .sa). These
	 * associated files should be in the same directory as the {@code alignmentReferenceFastaFile}.
	 * 
	 * @param alignmentReferenceFastaFile
	 */
	protected void setAlignmentReferenceFastaFiles(Path alignmentReferenceFastaFile)
	{
		String mainFastaFile = alignmentReferenceFastaFile.toString();
		this.alignmentReferenceFastaFile = alignmentReferenceFastaFile;
		alignmentReferenceFastaAmbFile = new Path(mainFastaFile + ".amb");
		alignmentReferenceFastaAnnFile = new Path(mainFastaFile + ".ann");
		alignmentReferenceFastaBwtFile = new Path(mainFastaFile + ".bwt");
		alignmentReferenceFastaFaiFile = new Path(mainFastaFile + ".fai");
		alignmentReferenceFastaPacFile = new Path(mainFastaFile + ".pac");
		alignmentReferenceFastaSaFile = new Path(mainFastaFile + ".sa");
		alignmentReferenceDictFile = new Path(mainFastaFile.replaceAll("\\.fa(sta)?$", "") + ".dict");
	}

	/**
	 * Sets the {@code alignmentReferenceFastaFile} and associated files (.amb, .ann, .bwt, .fai, .pac & .sa). These
	 * associated files should be in the same directory as the {@code alignmentReferenceFastaFile}.
	 * 
	 * @param alignmentReferenceFastaFile
	 */
	protected void setAlignmentReferenceFastaFiles(String alignmentReferenceFastaFile)
	{
		this.alignmentReferenceFastaFile = new Path(alignmentReferenceFastaFile);
		alignmentReferenceFastaAmbFile = new Path(alignmentReferenceFastaFile + ".amb");
		alignmentReferenceFastaAnnFile = new Path(alignmentReferenceFastaFile + ".ann");
		alignmentReferenceFastaBwtFile = new Path(alignmentReferenceFastaFile + ".bwt");
		alignmentReferenceFastaFaiFile = new Path(alignmentReferenceFastaFile + ".fai");
		alignmentReferenceFastaPacFile = new Path(alignmentReferenceFastaFile + ".pac");
		alignmentReferenceFastaSaFile = new Path(alignmentReferenceFastaFile + ".sa");
		alignmentReferenceDictFile = new Path(alignmentReferenceFastaFile.replaceAll("\\.fa(sta)?$", "") + ".dict");
	}

	public Path getAlignmentReferenceFastaAmbFile()
	{
		return alignmentReferenceFastaAmbFile;
	}

	public Path getAlignmentReferenceFastaAnnFile()
	{
		return alignmentReferenceFastaAnnFile;
	}

	public Path getAlignmentReferenceFastaBwtFile()
	{
		return alignmentReferenceFastaBwtFile;
	}

	public Path getAlignmentReferenceFastaFaiFile()
	{
		return alignmentReferenceFastaFaiFile;
	}

	public Path getAlignmentReferenceFastaPacFile()
	{
		return alignmentReferenceFastaPacFile;
	}

	public Path getAlignmentReferenceFastaSaFile()
	{
		return alignmentReferenceFastaSaFile;
	}

	public Path getAlignmentReferenceDictFile()
	{
		return alignmentReferenceDictFile;
	}

	public Path getBedFile()
	{
		return bedFile;
	}

	protected void setBedFile(Path bedFile)
	{
		this.bedFile = bedFile;
	}

	protected void setBedFile(String bedFile)
	{
		this.bedFile = new Path(bedFile);
	}

	public Path getSamplesInfoFile()
	{
		return samplesInfoFile;
	}

	protected void setSamplesInfoFile(Path samplesInfoFile)
	{
		this.samplesInfoFile = samplesInfoFile;
	}

	protected void setSamplesInfoFile(String samplesInfoFile)
	{
		this.samplesInfoFile = new Path(samplesInfoFile);
	}

	/**
	 * Checks whether all required files exist. If one of the required files could not be found, a print statement to
	 * stderr is given regarding the missing file(s). If all required files are present, {@code isContinueApplication()}
	 * is set to {@code true}.
	 * 
	 * @throws IOException
	 */
	protected void checkValidityArguments() throws IOException
	{
		// Is set to true if one argument is invalid.
		boolean invalidInput = false;

		// Checks user input files whether they are present.
		if (!checkIfPathIsFile(toolsArchiveLocation))
		{
			invalidInput = true;
			System.err.println("Tools archive path is invalid.");
		}
		if (!checkIfPathIsDir(inputDir))
		{
			invalidInput = true;
			System.err.println("Input directory does not exist.");
		}
		if (!checkIfPathParentIsDir(outputDir))
		{
			invalidInput = true;
			System.err.println("Output directory parent does not exist.");
		}
		else if (checkIfPathIsDir(outputDir))
		{
			invalidInput = true;
			System.err.println("Output directory already exists.");
		}
		if (!checkIfPathIsFile(alignmentReferenceFastaFile))
		{
			invalidInput = true;
			System.err.println("BWA index fasta file is not present.");
		}
		// Only check for accompanying bwa index files if original fasta file is found.
		else if (!checkIfPathIsFile(alignmentReferenceFastaAmbFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaAnnFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaBwtFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaFaiFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaPacFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaSaFile) || !checkIfPathIsFile(alignmentReferenceDictFile))
		{
			invalidInput = true;
			System.err.println(
					"Additional BWA index/dict files are incomplete. Please be sure the following files are present as well: "
							+ System.lineSeparator() + getAlignmentReferenceFastaAmbFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaAnnFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaBwtFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaFaiFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaPacFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaSaFile() + System.lineSeparator()
							+ getAlignmentReferenceDictFile());
		}
		if (!checkIfPathIsFile(bedFile))
		{
			invalidInput = true;
			System.err.println("BED file describing the grouping of the SAM records does not exist.");
		}
		if (!checkIfPathIsFile(samplesInfoFile))
		{
			invalidInput = true;
			System.err.println("Samplesheet file containing information about the samples does not exist.");
		}

		// If no invalid input is found, continueApplication is set to true.
		if (!invalidInput)
		{
			continueApplication = true;
		}
	}

	/**
	 * Checks if a given location is an existing file.
	 * 
	 * @param fileLocation
	 * @return boolean
	 * @throws IOException
	 */
	protected boolean checkIfPathIsFile(Path fileLocation) throws IOException
	{
		return fileSys.exists(fileLocation) && fileSys.getFileStatus(fileLocation).isFile();
	}

	/**
	 * Checks if a given location is an existing directory.
	 * 
	 * @param fileLocation
	 * @return boolean
	 * @throws IOException
	 */
	protected boolean checkIfPathIsDir(Path fileLocation) throws IOException
	{
		return fileSys.exists(fileLocation) && fileSys.getFileStatus(fileLocation).isDirectory();
	}

	/**
	 * Checks if a given location's parent is an existing directory.
	 * 
	 * @param fileLocation
	 * @return boolean
	 * @throws IOException
	 */
	protected boolean checkIfPathParentIsDir(Path fileLocation) throws IOException
	{
		return fileSys.exists(fileLocation.getParent())
				&& fileSys.getFileStatus(fileLocation.getParent()).isDirectory();
	}

	/**
	 * Checks whether the given read group line {@link String} matches the expected pattern. This pattern is:
	 * 
	 * <pre>
	 * {@code @RG\\tID:[0-9]+\\tPL:(?i:(capillary|ls454|illumina|solid|helicos|iontorrent|ont|pacbio))\\tLB:[0-9]+_[A-Z]+[0-9]+_[0-9]+_[A-Z]+_L[0-9]+\\tSM:[a-zA-Z0-9]+}
	 * </pre>
	 * 
	 * (above is a Java compatible regex, so characters such as an extra exclusion backslash are present)
	 * 
	 * @param readGroupLine
	 * @return boolean
	 */
	// private boolean checkReadGroupLineFormat(String readGroupLine)
	// {
	// String pattern =
	// "@RG\\tID:[0-9]+\\tPL:(?i:(capillary|ls454|illumina|solid|helicos|iontorrent|ont|pacbio))\\tLB:[0-9]+_[A-Z]+[0-9]+_[0-9]+_[A-Z]+_L[0-9]+\\tSM:[a-zA-Z0-9]+";
	// return readGroupLine.matches(pattern);
	// }
}
