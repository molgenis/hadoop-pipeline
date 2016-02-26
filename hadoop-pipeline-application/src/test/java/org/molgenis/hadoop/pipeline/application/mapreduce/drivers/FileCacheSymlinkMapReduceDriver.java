package org.molgenis.hadoop.pipeline.application.mapreduce.drivers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

/**
 * {@link MapReduceDriver} that also creates symlinks in the current working directory to the tmp folder storing the
 * MRUnit testing distributed cache.
 */
@SuppressWarnings("deprecation")
public class FileCacheSymlinkMapReduceDriver<K1, V1, K2, V2, K3, V3> extends MapReduceDriver<K1, V1, K2, V2, K3, V3>
{
	public FileCacheSymlinkMapReduceDriver(Mapper<K1, V1, K2, V2> mapper, Reducer<K2, V2, K3, V3> reducer)
	{
		super(mapper, reducer);
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
