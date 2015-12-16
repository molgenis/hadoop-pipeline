package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;
import java.util.ArrayList;

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
	 * Location of the .tar.gz archive containing the required tools.
	 */
	private Path toolsArchiveLocation;

	/**
	 * Directories with input files.
	 */
	private ArrayList<Path> inputDirs = new ArrayList<Path>();

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
	 * The file containing information about the samples.
	 */
	private Path samplesInfoFile;

	protected void setFileSys(FileSystem fileSys)
	{
		this.fileSys = fileSys;
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

	public ArrayList<Path> getInputDirs()
	{
		return inputDirs;
	}

	protected void setInputDirs(ArrayList<Path> inputDirs)
	{
		this.inputDirs = inputDirs;
	}

	protected void setInputDirs(String[] inputDirs)
	{
		this.inputDirs.clear();

		for (String inputDir : inputDirs)
		{
			addInputDir(inputDir);
		}
	}

	protected void addInputDir(String inputDir)
	{
		addInputDir(new Path(inputDir));
	}

	protected void addInputDir(Path inputDir)
	{
		this.inputDirs.add(inputDir);
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
	 * Sets the {@code alignmentReferenceFastaFile} and associated files (.amb, .ann, .bwt, .fai, .pac, .sa & .dict).
	 * These associated files should be in the same directory as the {@code alignmentReferenceFastaFile} and should have
	 * the same file name prefix.
	 * 
	 * @param alignmentReferenceFastaFile
	 *            {@link Path} location of alignment reference fasta file.
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
	 * Sets the {@code alignmentReferenceFastaFile} and associated files (.amb, .ann, .bwt, .fai, .pac, .sa & .dict).
	 * These associated files should be in the same directory as the {@code alignmentReferenceFastaFile} and should have
	 * the same file name prefix.
	 * 
	 * @param alignmentReferenceFastaFile
	 *            {@link String} location of alignment reference fasta file.
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
	 * Checks whether all required input parameters are valid. For each invalid parameter, an error message is written
	 * (unless otherwise specified by in-line code comments).
	 * 
	 * @return {@code true} if all parameters are correct, otherwise {@code false}.
	 * @throws IOException
	 */
	protected boolean checkValidityArguments() throws IOException
	{
		// Is set to false if at least one argument is invalid.
		boolean validInput = true;

		// Validates tools archive.
		if (!checkIfPathIsFile(toolsArchiveLocation))
		{
			validInput = false;
			System.err.println("Tools archive path is invalid.");
		}

		// Checks if there are any input directories given.
		if (inputDirs.size() < 1)
		{
			validInput = false;
			System.err.println("No input directories were given.");
		}
		// If there are any input directories, validates each input directory.
		else
		{
			for (Path inputDir : inputDirs)
			{
				if (!checkIfPathIsDir(inputDir))
				{
					validInput = false;
					System.err.println("The following input directory does not exist: " + inputDir);
				}
			}
		}

		// Validates that the output directory does not exist, and if it doesn't, also checks that the parent directory
		// does exist.
		if (!checkIfPathParentIsDir(outputDir))
		{
			validInput = false;
			System.err.println("Output directory parent does not exist.");
		}
		else if (checkIfPathIsDir(outputDir))
		{
			validInput = false;
			System.err.println("Output directory already exists.");
		}

		// Checks whether the alignment reference fasta file exists.
		if (!checkIfPathIsFile(alignmentReferenceFastaFile))
		{
			validInput = false;
			System.err.println("BWA index fasta file is not present (skipping check on aditional index files).");
		}
		// Only check for accompanying bwa index files if reference fasta file is valid.
		else if (!checkIfPathIsFile(alignmentReferenceFastaAmbFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaAnnFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaBwtFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaFaiFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaPacFile)
				|| !checkIfPathIsFile(alignmentReferenceFastaSaFile) || !checkIfPathIsFile(alignmentReferenceDictFile))
		{
			validInput = false;
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

		// Checks validity bed file.
		if (!checkIfPathIsFile(bedFile))
		{
			validInput = false;
			System.err.println("BED file describing the grouping of the SAM records does not exist.");
		}

		// Checks validity samples info file.
		if (!checkIfPathIsFile(samplesInfoFile))
		{
			validInput = false;
			System.err.println("Samplesheet file containing information about the samples does not exist.");
		}

		// Returns whether input parameters were valid.
		return validInput;
	}

	/**
	 * Checks if a given location is an existing file.
	 * 
	 * @param fileLocation
	 *            {@link Path}
	 * @return boolean {@code true} if {@link Path} exists and is a file, otherwise {@code false}.
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
	 *            {@link Path}
	 * @return boolean {@code true} if {@link Path} exists and is a directory, otherwise {@code false}.
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
	 *            {@link Path}
	 * @return boolean {@code true} if {@link Path#getParent()} exists and is a directory, otherwise {@code false}.
	 * @throws IOException
	 */
	protected boolean checkIfPathParentIsDir(Path fileLocation) throws IOException
	{
		return fileSys.exists(fileLocation.getParent())
				&& fileSys.getFileStatus(fileLocation.getParent()).isDirectory();
	}
}
