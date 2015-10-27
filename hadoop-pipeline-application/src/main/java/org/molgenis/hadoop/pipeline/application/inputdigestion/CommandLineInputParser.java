package org.molgenis.hadoop.pipeline.application.inputdigestion;

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;

public class CommandLineInputParser extends InputParser
{
	/**
	 * Variable for generating & digesting the command line options.
	 */
	private Options options = new Options();

	/**
	 * Variable for digesting the command line.
	 */
	private CommandLine commandLine;

	/**
	 * Initiates parsing of the command line.
	 * 
	 * @param fileSys
	 * @param args
	 * @throws ParseException
	 * @throws IOException
	 */
	public CommandLineInputParser(FileSystem fileSys, String[] args) throws ParseException, IOException
	{
		setFileSys(requireNonNull(fileSys));
		requireNonNull(args);

		createOptions();
		retrieveParser(args);
		digestCommandLine();
		checkValidityArguments();
	}

	/**
	 * Prints the help message to stdout.
	 */
	public void printHelpMessage()
	{
		String helpHeader = "example: hadoop-pipeline-application -t /path/to/tools.tar.gz -i /path/to/inputDir/ -o /path/to/outputDir/ -bwa /path/to/bwa_reference_file.fasta";
		String helpFooter = "Molgenis-hadoop";

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80,
				"hadoop-pipeline-application -t <hdfs location tools tar.gz archive> -i <hdfs dir with interlieved pair-ended fasta files> -o <hdfs output dir> -bwa <hdfs location BWA alignment reference fasta file>",
				helpHeader, this.options, helpFooter, false);
	}

	/**
	 * Creates the command line option.
	 */
	@SuppressWarnings("static-access")
	private void createOptions()
	{
		options.addOption(OptionBuilder.withArgName("tools").hasArg().isRequired(true)
				.withDescription(
						"A tar.gz archive containing the required executables (see the readme for more information).")
				.create("t"));

		options.addOption(OptionBuilder.withArgName("input").hasArg().isRequired(true)
				.withDescription("Directory containing the input files.").create("i"));

		options.addOption(OptionBuilder.withArgName("output").hasArg().isRequired(true)
				.withDescription(
						"Directory in which the results should be stored. Note that the directory itself should not exist, though the parent directory should.")
				.create("o"));

		options.addOption(OptionBuilder.withArgName("burrows_wheeler_align").hasArg().isRequired(true)
				.withDescription(
						"BWA reference fasta file. Other BWA index file should be present as well using the same prefix.")
				.create("bwa"));
	}

	/**
	 * Creates a parser for command line parsing.
	 * 
	 * @param args
	 * @throws ParseException
	 */
	private void retrieveParser(String[] args) throws ParseException
	{
		// Creates parser.
		CommandLineParser parser = new BasicParser();

		// Execute the parsing.
		commandLine = parser.parse(options, args);
	}

	/**
	 * Digests the command line for arguments and appoints these to variables.
	 */
	private void digestCommandLine()
	{
		if (commandLine.hasOption("t"))
		{
			setToolsArchiveLocation(commandLine.getOptionValue("t"));
		}
		if (commandLine.hasOption("i"))
		{
			setInputDir(commandLine.getOptionValue("i"));
		}
		if (commandLine.hasOption("o"))
		{
			setOutputDir(commandLine.getOptionValue("o"));
		}
		if (commandLine.hasOption("bwa"))
		{
			setAlignmentReferenceFastaFiles(commandLine.getOptionValue("bwa"));
		}
	}
}
