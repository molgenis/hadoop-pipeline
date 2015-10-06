package org.molgenis.hadoop.pipeline.application.inputdigestion;

import java.io.IOException;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CommandLineInputParserTester
{
	ClassLoader classLoader;
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

	/**
	 * Executes basic code needed for the tests.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public void beforeClass() throws IOException
	{
		classLoader = Thread.currentThread().getContextClassLoader();
		// System.out.println(classLoader.getResource("").toString());
		// Output: <path/to/dir>/hadoop-pipeline/hadoop-pipeline-application/target/test-classes/

		Configuration conf = new Configuration();
		fileSys = FileSystem.get(conf);

		// String objects that function as input.
		tools = classLoader.getResource("tools.tar.gz").toString();
		inputDir = classLoader.getResource("").toString();
		outputDir = classLoader.getResource("").toString() + "output/";
		bwaRefFasta = classLoader.getResource("bwa_ref.fasta").toString();
		bwaRefFastaAmb = classLoader.getResource("bwa_ref.fasta.amb").toString();
		bwaRefFastaAnn = classLoader.getResource("bwa_ref.fasta.ann").toString();
		bwaRefFastaBwt = classLoader.getResource("bwa_ref.fasta.bwt").toString();
		bwaRefFastaFai = classLoader.getResource("bwa_ref.fasta.fai").toString();
		bwaRefFastaPac = classLoader.getResource("bwa_ref.fasta.pac").toString();
		bwaRefFastaSa = classLoader.getResource("bwa_ref.fasta.sa").toString();

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
	}

	/**
	 * Test: If all input is correct.
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	@Test
	public void allValidInput() throws ParseException, IOException
	{
		String[] args = new String[9];
		args[0] = ""; // main class
		args[1] = "-t";
		args[2] = tools;
		args[3] = "-i";
		args[4] = inputDir;
		args[5] = "-o";
		args[6] = outputDir;
		args[7] = "-bwa";
		args[8] = bwaRefFasta;

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
		String[] args = new String[7];
		args[0] = ""; // main class
		args[1] = "-t";
		args[2] = tools;
		args[3] = "-i";
		args[4] = inputDir;
		args[5] = "-o";
		args[6] = outputDir;

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
		String invalidTools = classLoader.getResource("").toString() + "non_existing.tar.gz";

		String[] args = new String[9];
		args[0] = ""; // main class
		args[1] = "-t";
		args[2] = invalidTools;
		args[3] = "-i";
		args[4] = inputDir;
		args[5] = "-o";
		args[6] = outputDir;
		args[7] = "-bwa";
		args[8] = bwaRefFasta;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(new Path(invalidTools), parser.getToolsArchiveLocation());
		Assert.assertEquals(inputDirAsPath, parser.getInputDir());
		Assert.assertEquals(outputDirAsPath, parser.getOutputDir());
		Assert.assertEquals(bwaRefFastaAsPath, parser.getAlignmentReferenceFastaFile());
		Assert.assertEquals(bwaRefFastaAmbAsPath, parser.getAlignmentReferenceFastaAmbFile());
		Assert.assertEquals(bwaRefFastaAnnAsPath, parser.getAlignmentReferenceFastaAnnFile());
		Assert.assertEquals(bwaRefFastaBwtAsPath, parser.getAlignmentReferenceFastaBwtFile());
		Assert.assertEquals(bwaRefFastaFaiAsPath, parser.getAlignmentReferenceFastaFaiFile());
		Assert.assertEquals(bwaRefFastaPacAsPath, parser.getAlignmentReferenceFastaPacFile());
		Assert.assertEquals(bwaRefFastaSaAsPath, parser.getAlignmentReferenceFastaSaFile());
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
		String invalidInputDir = classLoader.getResource("").toString() + "non_existing_input/";

		String[] args = new String[9];
		args[0] = ""; // main class
		args[1] = "-t";
		args[2] = tools;
		args[3] = "-i";
		args[4] = invalidInputDir;
		args[5] = "-o";
		args[6] = outputDir;
		args[7] = "-bwa";
		args[8] = bwaRefFasta;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(toolsAsPath, parser.getToolsArchiveLocation());
		Assert.assertEquals(new Path(invalidInputDir), parser.getInputDir());
		Assert.assertEquals(outputDirAsPath, parser.getOutputDir());
		Assert.assertEquals(bwaRefFastaAsPath, parser.getAlignmentReferenceFastaFile());
		Assert.assertEquals(bwaRefFastaAmbAsPath, parser.getAlignmentReferenceFastaAmbFile());
		Assert.assertEquals(bwaRefFastaAnnAsPath, parser.getAlignmentReferenceFastaAnnFile());
		Assert.assertEquals(bwaRefFastaBwtAsPath, parser.getAlignmentReferenceFastaBwtFile());
		Assert.assertEquals(bwaRefFastaFaiAsPath, parser.getAlignmentReferenceFastaFaiFile());
		Assert.assertEquals(bwaRefFastaPacAsPath, parser.getAlignmentReferenceFastaPacFile());
		Assert.assertEquals(bwaRefFastaSaAsPath, parser.getAlignmentReferenceFastaSaFile());
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
		String invalidOutputDir = classLoader.getResource("").toString() + "output/another_output/";

		String[] args = new String[9];
		args[0] = ""; // main class
		args[1] = "-t";
		args[2] = tools;
		args[3] = "-i";
		args[4] = inputDir;
		args[5] = "-o";
		args[6] = invalidOutputDir;
		args[7] = "-bwa";
		args[8] = bwaRefFasta;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(toolsAsPath, parser.getToolsArchiveLocation());
		Assert.assertEquals(inputDirAsPath, parser.getInputDir());
		Assert.assertEquals(new Path(invalidOutputDir), parser.getOutputDir());
		Assert.assertEquals(bwaRefFastaAsPath, parser.getAlignmentReferenceFastaFile());
		Assert.assertEquals(bwaRefFastaAmbAsPath, parser.getAlignmentReferenceFastaAmbFile());
		Assert.assertEquals(bwaRefFastaAnnAsPath, parser.getAlignmentReferenceFastaAnnFile());
		Assert.assertEquals(bwaRefFastaBwtAsPath, parser.getAlignmentReferenceFastaBwtFile());
		Assert.assertEquals(bwaRefFastaFaiAsPath, parser.getAlignmentReferenceFastaFaiFile());
		Assert.assertEquals(bwaRefFastaPacAsPath, parser.getAlignmentReferenceFastaPacFile());
		Assert.assertEquals(bwaRefFastaSaAsPath, parser.getAlignmentReferenceFastaSaFile());
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
		String invalidOutputDir = classLoader.getResource("").toString();

		String[] args = new String[9];
		args[0] = ""; // main class
		args[1] = "-t";
		args[2] = tools;
		args[3] = "-i";
		args[4] = inputDir;
		args[5] = "-o";
		args[6] = invalidOutputDir;
		args[7] = "-bwa";
		args[8] = bwaRefFasta;

		CommandLineInputParser parser = new CommandLineInputParser(fileSys, args);

		Assert.assertEquals(toolsAsPath, parser.getToolsArchiveLocation());
		Assert.assertEquals(inputDirAsPath, parser.getInputDir());
		Assert.assertEquals(new Path(invalidOutputDir), parser.getOutputDir());
		Assert.assertEquals(bwaRefFastaAsPath, parser.getAlignmentReferenceFastaFile());
		Assert.assertEquals(bwaRefFastaAmbAsPath, parser.getAlignmentReferenceFastaAmbFile());
		Assert.assertEquals(bwaRefFastaAnnAsPath, parser.getAlignmentReferenceFastaAnnFile());
		Assert.assertEquals(bwaRefFastaBwtAsPath, parser.getAlignmentReferenceFastaBwtFile());
		Assert.assertEquals(bwaRefFastaFaiAsPath, parser.getAlignmentReferenceFastaFaiFile());
		Assert.assertEquals(bwaRefFastaPacAsPath, parser.getAlignmentReferenceFastaPacFile());
		Assert.assertEquals(bwaRefFastaSaAsPath, parser.getAlignmentReferenceFastaSaFile());
		Assert.assertEquals(false, parser.isContinueApplication());
	}
}
