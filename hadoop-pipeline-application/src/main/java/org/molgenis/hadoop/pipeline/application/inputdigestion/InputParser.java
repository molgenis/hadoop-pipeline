package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class InputParser
{
	/**
	 * File system for validating existence files given as input.
	 */
	protected FileSystem fileSys;

	/**
	 * Switch that only allows the MapReduce phase to start if all required files exist.
	 */
	protected boolean continueApplication;

	/**
	 * Location of the .tar.gz archive containing the required tools.
	 */
	protected Path toolsArchiveLocation;

	/**
	 * Location with input files.
	 */
	protected Path inputDir;

	/**
	 * Location results can be written to.
	 */
	protected Path outputDir;

	/**
	 * Location BWA index .fasta file for alignment.
	 */
	protected Path alignmentReferenceFastaFile;

	/**
	 * Location BWA index .fasta.amb file for alignment.
	 */
	protected Path alignmentReferenceFastaAmbFile;

	/**
	 * Location BWA index .fasta.ann file for alignment.
	 */
	protected Path alignmentReferenceFastaAnnFile;

	/**
	 * Location BWA index .fasta.bwt file for alignment.
	 */
	protected Path alignmentReferenceFastaBwtFile;

	/**
	 * Location BWA index .fasta.fai file for alignment.
	 */
	protected Path alignmentReferenceFastaFaiFile;

	/**
	 * Location BWA index .fasta.pac file for alignment.
	 */
	protected Path alignmentReferenceFastaPacFile;

	/**
	 * Location BWA index .fasta.sa file for alignment.
	 */
	protected Path alignmentReferenceFastaSaFile;

	public boolean isContinueApplication()
	{
		return continueApplication;
	}

	public Path getToolsArchiveLocation()
	{
		return toolsArchiveLocation;
	}

	public Path getInputDir()
	{
		return inputDir;
	}

	public Path getOutputDir()
	{
		return outputDir;
	}

	public Path getAlignmentReferenceFastaFile()
	{
		return alignmentReferenceFastaFile;
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
				|| !checkIfPathIsFile(alignmentReferenceFastaSaFile))
		{
			invalidInput = true;
			System.err.println(
					"Additional BWA index files are incomplete. Please be sure the following files are present as well: "
							+ System.lineSeparator() + getAlignmentReferenceFastaAmbFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaAnnFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaBwtFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaFaiFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaPacFile() + System.lineSeparator()
							+ getAlignmentReferenceFastaSaFile());
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
}
