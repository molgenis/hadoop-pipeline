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
public class WorkDirSymLinkManagerTester extends Tester
{
	private FileSystem fileSys;
	private Path bwaRefFasta;
	private Path bwaRefFastaAmb;
	private Path bwaRefFastaAnn;

	// Test line to show a SymlinkAlreadyExistsException is thrown when a symlink is added twice (be sure to only
	// enable for testing the testing code and when using it on testing code that has at least 4 files added to the
	// cache).
	// WorkDirSymlinkManager.createSymlinkInCurrentWorkDirToPath(DistributedCache.getLocalCacheFiles(conf)[3]);

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

	@Test
	public void testSingleSymlinkCreation() throws IOException
	{
		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirToPath(bwaRefFasta);

		if (!fileSys.exists(bwaRefFasta))
		{
			Assert.fail();
		}
	}

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

	@Test(expectedExceptions = SymlinkAlreadyExistsException.class)
	public void testSameSymlinkCreatedMultipleTimes() throws IOException
	{
		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirForEachPath(new Path[]
		{ bwaRefFasta, bwaRefFastaAmb, bwaRefFastaAnn });

		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirToPath(bwaRefFastaAmb);
	}

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
