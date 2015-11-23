package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
	String validReadGroup;

	Path toolsAsPath;
	Path inputDirAsPath;
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
		bedFile = getClassLoader().getResource("chr1_20000000-21000000.bed").toString();
		validReadGroup = "@RG\tID:5\tPL:illumina\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		// Path objects for comparison with expected output.
		toolsAsPath = new Path(tools);
		inputDirAsPath = new Path(inputDir);
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

		// Create arguments string for the command line parser input.
		args = new String[10];
		argsWithReadGroupLine = new String[12];
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
		args[6] = "-bwa";
		args[7] = bwaRefFasta;
		args[8] = "-bed";
		args[9] = bedFile;

		// Defines the default command line input when a read group line is also given.
		argsWithReadGroupLine = Arrays.copyOf(args, args.length + 2);
		argsWithReadGroupLine[10] = "-rg";
		argsWithReadGroupLine[11] = validReadGroup;
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

		Assert.assertEquals(toolsAsPath, parser.getToolsArchiveLocation());
		Assert.assertEquals(inputDirAsPath, parser.getInputDir());
		Assert.assertEquals(outputDirAsPath, parser.getOutputDir());
		Assert.assertEquals(bwaRefFastaAsPath, parser.getAlignmentReferenceFastaFile());
		Assert.assertEquals(bwaRefFastaAmbAsPath, parser.getAlignmentReferenceFastaAmbFile());
		Assert.assertEquals(bwaRefFastaAnnAsPath, parser.getAlignmentReferenceFastaAnnFile());
		Assert.assertEquals(bwaRefFastaBwtAsPath, parser.getAlignmentReferenceFastaBwtFile());
		Assert.assertEquals(bwaRefFastaFaiAsPath, parser.getAlignmentReferenceFastaFaiFile());
		Assert.assertEquals(bwaRefFastaPacAsPath, parser.getAlignmentReferenceFastaPacFile());
		Assert.assertEquals(bwaRefFastaSaAsPath, parser.getAlignmentReferenceFastaSaFile());
		Assert.assertEquals(bwaRefDictAsPath, parser.getAlignmentReferenceDictFile());
		Assert.assertEquals(bedFileAsPath, parser.getBedFile());
		Assert.assertEquals(null, parser.getReadGroupLine()); // Is null as parameter was not set.
		Assert.assertEquals(true, parser.isContinueApplication());
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
		String[] args = new String[8];
		args[0] = "-t";
		args[1] = tools;
		args[2] = "-i";
		args[3] = inputDir;
		args[4] = "-o";
		args[5] = outputDir;
		args[6] = "-bed";
		args[7] = bedFile;

		new CommandLineInputParser(fileSys, args);
	}

	/**
	 * Test: If tools.tar.gz file does not exist.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void nonExistingToolsArchive() throws ParseException, IOException
	{
		String invalidTools = getClassLoader().getResource("").toString() + "non_existing.tar.gz";
		args[1] = invalidTools;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(new Path(invalidTools), parser.getToolsArchiveLocation());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If input directory does not exist.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void nonExistingInputDir() throws ParseException, IOException
	{
		String invalidInputDir = getClassLoader().getResource("").toString() + "non_existing_input/";
		args[3] = invalidInputDir;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(new Path(invalidInputDir), parser.getInputDir());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If output directory parent does not exist.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void nonExistingOutputParentDir() throws ParseException, IOException
	{
		String invalidOutputDir = getClassLoader().getResource("").toString() + "output/another_output/";
		args[5] = invalidOutputDir;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(new Path(invalidOutputDir), parser.getOutputDir());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If output directory itself already exists.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void outputDirAlreadyExists() throws ParseException, IOException
	{
		String invalidOutputDir = getClassLoader().getResource("").toString();
		args[5] = invalidOutputDir;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(new Path(invalidOutputDir), parser.getOutputDir());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If a read group line is also given.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withValidReadGroupPgNameIlluminaLowerCase() throws ParseException, IOException
	{
		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(validReadGroup, parser.getReadGroupLine());
		System.out.println(parser.getReadGroupLine());
		Assert.assertEquals(true, parser.isContinueApplication());
	}

	/**
	 * Test:
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withValidReadGroupPgNameIlluminaCapitalCase() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "@RG\tID:5\tPL:Illumina\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(true, parser.isContinueApplication());
	}

	/**
	 * Test:
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withValidReadGroupPgNameIlluminaUpperCase() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "@RG\tID:5\tPL:ILLUMINA\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(true, parser.isContinueApplication());
	}

	/**
	 * Test:
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withValidReadGroupPgNameHelicos() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "@RG\tID:5\tPL:ILLUMINA\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(true, parser.isContinueApplication());
	}

	/**
	 * Test:
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withInValidReadGroupPgNameKobol() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "@RG\tID:5\tPL:kobol\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If a read group line is also given but misses the {@code @RG} tag.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withInvalidReadGroupNoStartTag() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "ID:5\tPL:illumina\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If a read group line is also given but misses a field.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withInvalidReadGroupMissingField() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "@RG\tID:5\tLB:150702_SN163_0649_BHJYNKADXX_L5\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(false, parser.isContinueApplication());
	}

	/**
	 * Test: If a read group line is also given but the field order differs.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void withInvalidReadGroupDifferentFieldOrder() throws ParseException, IOException
	{
		argsWithReadGroupLine[11] = "@RG\tID:5\tLB:150702_SN163_0649_BHJYNKADXX_L5\tPL:illumina\tSM:sample3";

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, argsWithReadGroupLine);

		Assert.assertEquals(argsWithReadGroupLine[11], parser.getReadGroupLine());
		Assert.assertEquals(false, parser.isContinueApplication());
	}
}
