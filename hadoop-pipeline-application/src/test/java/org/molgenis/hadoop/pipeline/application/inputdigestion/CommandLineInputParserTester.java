package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester for {@link CommandLineInputParser}.
 */
public class CommandLineInputParserTester extends Tester
{
	FileSystem fileSys;

	String tools;
	String inputDir;
	String outputDir;
	String bwaRefFasta;
	String bwaRefFastaAmb;
	String bwaRefFastaAnn;
	String bwaRefFastaBwt;
	String bwaRefFastaFai;
	String bwaRefFastaPac;
	String bwaRefFastaSa;
	String bwaRefDict;
	String bedFile;
	String samplesInfoFile;;

	Path toolsAsPath;
	ArrayList<Path> inputDirsAsPath;
	Path outputDirAsPath;
	Path bwaRefFastaAsPath;
	Path bwaRefFastaAmbAsPath;
	Path bwaRefFastaAnnAsPath;
	Path bwaRefFastaBwtAsPath;
	Path bwaRefFastaFaiAsPath;
	Path bwaRefFastaPacAsPath;
	Path bwaRefFastaSaAsPath;
	Path bwaRefDictAsPath;
	Path bedFileAsPath;
	Path samplesInfoFileAsPath;

	String[] args;
	String[] argsWithReadGroupLine;

	/**
	 * Executes basic code needed for the tests.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		Configuration conf = new Configuration();
		fileSys = FileSystem.get(conf);

		// String objects that function as input.
		tools = getClassLoader().getResource("tools.tar.gz").toString();
		inputDir = getClassLoader().getResource("").toString();
		outputDir = getClassLoader().getResource("").toString() + "output/";
		bwaRefFasta = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa").toString();
		bwaRefFastaAmb = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.amb").toString();
		bwaRefFastaAnn = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.ann").toString();
		bwaRefFastaBwt = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.bwt").toString();
		bwaRefFastaFai = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.fai").toString();
		bwaRefFastaPac = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.pac").toString();
		bwaRefFastaSa = getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.sa").toString();
		bwaRefDict = getClassLoader().getResource("reference_data/chr1_20000000-21000000.dict").toString();
		bedFile = getClassLoader().getResource("bed_files/chr1_20000000-21000000.bed").toString();
		samplesInfoFile = getClassLoader().getResource("samplesheets/samplesheet.csv").toString();

		// Path objects for comparison with expected output.
		toolsAsPath = new Path(tools);
		inputDirsAsPath = new ArrayList<Path>();
		inputDirsAsPath.add(new Path(inputDir));
		outputDirAsPath = new Path(outputDir);
		bwaRefFastaAsPath = new Path(bwaRefFasta);
		bwaRefFastaAmbAsPath = new Path(bwaRefFastaAmb);
		bwaRefFastaAnnAsPath = new Path(bwaRefFastaAnn);
		bwaRefFastaBwtAsPath = new Path(bwaRefFastaBwt);
		bwaRefFastaFaiAsPath = new Path(bwaRefFastaFai);
		bwaRefFastaPacAsPath = new Path(bwaRefFastaPac);
		bwaRefFastaSaAsPath = new Path(bwaRefFastaSa);
		bwaRefDictAsPath = new Path(bwaRefDict);
		bedFileAsPath = new Path(bedFile);
		samplesInfoFileAsPath = new Path(samplesInfoFile);

		// Create arguments string for the command line parser input.
		args = new String[12];
	}

	/**
	 * Resets command line string to default before each test.
	 */
	@BeforeMethod
	public void beforeMethod()
	{
		// Defines the default command line input.
		args[0] = "-t";
		args[1] = tools;
		args[2] = "-i";
		args[3] = inputDir;
		args[4] = "-o";
		args[5] = outputDir;
		args[6] = "-r";
		args[7] = bwaRefFasta;
		args[8] = "-b";
		args[9] = bedFile;
		args[10] = "-s";
		args[11] = samplesInfoFile;
	}

	/**
	 * Test: If all input is correct. If this test is valid, only the adjusted inputs need to be retested. The dict path
	 * is generated based upon the alignment reference fasta file. As this file can be both .fa and .fasta, regex was
	 * used to generate the .dict file path. This regex (java regex version: "\\.fa(sta)?$") was tested on
	 * http://www.regexplanet.com/advanced/java/index.html for validity with the following input file names: test.fa,
	 * test.fasta, test.fq, test.fastq, test.afa, test.afasta, test.faq, test.fastaq
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void allValidInput() throws ParseException, IOException
	{
		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);
		parser.printHelpMessage();

		Assert.assertEquals(parser.getToolsArchiveLocation(), toolsAsPath);
		Assert.assertEquals(parser.getInputDirs(), inputDirsAsPath);
		Assert.assertEquals(parser.getOutputDir(), outputDirAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaFile(), bwaRefFastaAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaAmbFile(), bwaRefFastaAmbAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaAnnFile(), bwaRefFastaAnnAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaBwtFile(), bwaRefFastaBwtAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaFaiFile(), bwaRefFastaFaiAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaPacFile(), bwaRefFastaPacAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceFastaSaFile(), bwaRefFastaSaAsPath);
		Assert.assertEquals(parser.getAlignmentReferenceDictFile(), bwaRefDictAsPath);
		Assert.assertEquals(parser.getBedFile(), bedFileAsPath);
		Assert.assertEquals(parser.getSamplesInfoFile(), samplesInfoFileAsPath);
	}

	/**
	 * Test: If bwa index command line argument is missing. Expects an {@code MissingOptionException}.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test(expectedExceptions = MissingOptionException.class)
	public void missingBwaIndexArgument() throws ParseException, IOException
	{
		String[] args = new String[10];
		args[0] = "-t";
		args[1] = tools;
		args[2] = "-i";
		args[3] = inputDir;
		args[4] = "-o";
		args[5] = outputDir;
		args[6] = "-b";
		args[7] = bedFile;
		args[8] = "-s";
		args[9] = samplesInfoFile;

		new CommandLineInputParser(fileSys, args);
	}

	/**
	 * Test: If tools.tar.gz file does not exist.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test(expectedExceptions = ParseException.class)
	public void nonExistingToolsArchive() throws ParseException, IOException
	{
		String invalidTools = getClassLoader().getResource("").toString() + "non_existing.tar.gz";
		args[1] = invalidTools;

		new CommandLineInputParser(fileSys, args);
	}

	/**
	 * Test: If input directory does not exist.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test(expectedExceptions = ParseException.class)
	public void nonExistingInputDir() throws ParseException, IOException
	{
		String invalidInputDir = getClassLoader().getResource("").toString() + "non_existing_input/";
		args[3] = invalidInputDir;

		new CommandLineInputParser(fileSys, args);
	}

	/**
	 * Test: If output directory parent does not exist.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test(expectedExceptions = ParseException.class)
	public void nonExistingOutputParentDir() throws ParseException, IOException
	{
		String invalidOutputDir = getClassLoader().getResource("").toString() + "output/another_output/";
		args[5] = invalidOutputDir;

		new CommandLineInputParser(fileSys, args);
	}

	/**
	 * Test: If output directory itself already exists.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test(expectedExceptions = ParseException.class)
	public void outputDirAlreadyExists() throws ParseException, IOException
	{
		String invalidOutputDir = getClassLoader().getResource("").toString();
		args[5] = invalidOutputDir;

		new CommandLineInputParser(fileSys, args);
	}
}
