package org.molgenis.hadoop.pipeline.application.mapreduce.drivers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import com.google.common.collect.Lists;

/**
 * {@link MapDriver} that also creates symlinks in the current working directory to the tmp folder storing the MRUnit
 * testing distributed cache.
 */
@SuppressWarnings("deprecation")
public final class FileCacheSymlinkMapDriver<K1, V1, K2, V2> extends MapDriver<K1, V1, K2, V2>
{

	/**
	 * Stores the symlinks that were created (in case a symlink would be created while there is already a file present
	 * with that name in the working directory).
	 */
	private List<File> symlinksCreated;

	public FileCacheSymlinkMapDriver(Mapper<K1, V1, K2, V2> m)
	{
		super(m);
	}

	@Override
	protected void initDistributedCache() throws IOException
	{
		super.initDistributedCache();
		symlinksCreated = Lists.newArrayList();
		Configuration conf = getConfiguration();

		createSymlinkInCurrentWorkDirForEachPath(DistributedCache.getLocalCacheArchives(conf));
		createSymlinkInCurrentWorkDirForEachPath(DistributedCache.getLocalCacheFiles(conf));
	}

	@Override
	protected void cleanupDistributedCache() throws IOException
	{
		for (File symlink : symlinksCreated)
		{
			symlink.delete();
		}
		symlinksCreated.clear();
		super.cleanupDistributedCache();
	}

	/**
	 * Creates a symlink in the working directory to each {@link Path} present in the array. Calls
	 * {@link #createSymlinkInCurrentWorkDirToPath(Path)} for each {@link Path} in {@code tmpFiles}.
	 *
	 * @param tmpFiles
	 *            {@link Path}{@code []}
	 * @throws IOException
	 */
	private void createSymlinkInCurrentWorkDirForEachPath(Path[] tmpFiles) throws IOException
	{
		for (int i = 0; i < tmpFiles.length; i++)
		{
			createSymlinkInCurrentWorkDirToPath(tmpFiles[i]);
		}
	}

	/**
	 * Creates a symlink in the working directory to the {@link Path}. Also adds the created symlink to
	 * {@code symlinksCreated} for when {@link #cleanupDistributedCache()} is called.
	 *
	 * @param tmpFile
	 *            {@link Path}
	 * @throws IOException
	 */
	private void createSymlinkInCurrentWorkDirToPath(Path tmpFile) throws IOException
	{
		File symlinkFile = new File("./" + tmpFile.getName());
		Files.createSymbolicLink(symlinkFile.toPath(), Paths.get(tmpFile.toString()));
		symlinksCreated.add(symlinkFile);
	}
}