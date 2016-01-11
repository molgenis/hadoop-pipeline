package org.molgenis.hadoop.pipeline.application.mapreduce.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

/**
 * {@link MapDriver} that also creates symlinks in the current working directory to the tmp folder storing the MRUnit
 * testing distributed cache.
 */
@SuppressWarnings("deprecation")
public final class FileCacheSymlinkMapDriver<K1, V1, K2, V2> extends MapDriver<K1, V1, K2, V2>
{
	public FileCacheSymlinkMapDriver(Mapper<K1, V1, K2, V2> m)
	{
		super(m);
	}

	@Override
	protected void initDistributedCache() throws IOException
	{
		super.initDistributedCache();
		Configuration conf = getConfiguration();

		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirForEachPath(DistributedCache.getLocalCacheArchives(conf));
		WorkDirSymlinkManager.createSymlinkInCurrentWorkDirForEachPath(DistributedCache.getLocalCacheFiles(conf));
	}

	@Override
	protected void cleanupDistributedCache() throws IOException
	{
		WorkDirSymlinkManager.removeSymlinksInWorkDir();
		super.cleanupDistributedCache();
	}
}