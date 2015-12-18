package org.molgenis.hadoop.pipeline.application.mapreduce.drivers;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.molgenis.hadoop.pipeline.application.Tester;
import org.molgenis.hadoop.pipeline.application.exceptions.SymlinkAlreadyExistsException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test to validate whether the {@link WorkDirSymlinkManager} works correctly.
 */
public class WorkDirSymlinkManagerTester extends Tester
{
	private FileSystem fileSys;
	private Path bwaRefFasta;
	private Path bwaRefFastaAmb;
	private Path bwaRefFastaAnn;

	@BeforeClass
	public void beforeClass() throws IOException
	{
		fileSys = FileSystem.get(new Configuration());
		bwaRefFasta = new Path(getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa").toString());
		bwaRefFastaAmb = new Path(
				getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.amb").toString());
		bwaRefFastaAnn = new Path(
				getClassLoader().getResource("reference_data/chr1_20000000-21000000.fa.ann").toString());
	}

	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
	}

	@AfterMethod
	public void afterMethod() throws URISyntaxException, IOException
	{
		WorkDirSymlinkManager.removeSymlinksInWorkDir();
	}

	/**
	 * Tests the creation of a single symlink.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testSingleSymlinkCreation() throws IOException
	{
		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirToPath(bwaRefFasta);

		if (!fileSys.exists(bwaRefFasta))
		{
			Assert.fail();
		}
	}

	/**
	 * Tests the creation of multiple symlinks.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testMultipleSymlinkCreation() throws IOException
	{
		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirForEachPath(new Path[]
		{ bwaRefFasta, bwaRefFastaAmb, bwaRefFastaAnn });

		if (!fileSys.exists(bwaRefFasta) || !fileSys.exists(bwaRefFastaAmb) || !fileSys.exists(bwaRefFastaAnn))
		{
			Assert.fail();
		}
	}

	/**
	 * Tests whether a {@link SymlinkAlreadyExistsException} is thrown when an attempt is made to create a symlink while
	 * a symlink with that name was already created before.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = SymlinkAlreadyExistsException.class)
	public void testSameSymlinkCreatedMultipleTimes() throws IOException
	{
		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirForEachPath(new Path[]
		{ bwaRefFasta, bwaRefFastaAmb, bwaRefFastaAnn });

		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirToPath(bwaRefFastaAmb);
	}

	/**
	 * Tests whether a {@link FileAlreadyExistsException} is thrown when an attempt is made to create a symlink when a
	 * file was already present with that name (but that file is not a symlink that was created using
	 * {@link WorkDirSymlinkManager#createSymlinkInCurrentWorkDirToPath(Path)}.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = FileAlreadyExistsException.class)
	public void testSameFileWithIDenticalNameAlreadyExists() throws IOException
	{
		String fileName = "you_really_should_not_have_a_file_with_this_name_as_it_might_cause_issues_with_this.test";
		File file = new File("./" + fileName);

		try
		{
			file.createNewFile();
			WorkDirSymlinkManager.createSymlinkInCurrentWorkDirToPath(new Path(fileName));
		}
		finally
		{
			file.delete();
		}
	}

}
