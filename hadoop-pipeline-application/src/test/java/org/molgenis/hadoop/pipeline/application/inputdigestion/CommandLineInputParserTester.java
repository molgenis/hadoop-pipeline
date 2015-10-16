package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;

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

	String[] args;

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
		bwaRefFasta = getClassLoader().getResource("chr1_20000000-21000000.fa").toString();
		bwaRefFastaAmb = getClassLoader().getResource("chr1_20000000-21000000.fa.amb").toString();
		bwaRefFastaAnn = getClassLoader().getResource("chr1_20000000-21000000.fa.ann").toString();
		bwaRefFastaBwt = getClassLoader().getResource("chr1_20000000-21000000.fa.bwt").toString();
		bwaRefFastaFai = getClassLoader().getResource("chr1_20000000-21000000.fa.fai").toString();
		bwaRefFastaPac = getClassLoader().getResource("chr1_20000000-21000000.fa.pac").toString();
		bwaRefFastaSa = getClassLoader().getResource("chr1_20000000-21000000.fa.sa").toString();

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

		// Create arguments string for the command line parser input.
		args = new String[8];
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
	}

	/**
	 * Test: If all input is correct. If this test is valid, only the adjusted inputs need to be retested.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void allValidInput() throws ParseException, IOException
	{
		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

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
		String[] args = new String[6];
		args[0] = "-t";
		args[1] = tools;
		args[2] = "-i";
		args[3] = inputDir;
		args[4] = "-o";
		args[5] = outputDir;

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
	 * Test: if output directory itself already exists.
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
}
