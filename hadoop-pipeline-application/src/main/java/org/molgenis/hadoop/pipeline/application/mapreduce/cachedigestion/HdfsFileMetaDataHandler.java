package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.net.URI;

/**
 * Abstract class for basic HDFS file/directory information handling.
 */
public abstract class HdfsFileMetaDataHandler
{
	/**
	 * Retrieves the file name without the path before it.
	 * 
	 * @param filePath
	 * @return {@link String}
	 */
	public static String retrieveFileName(URI filePath)
	{
		String[] pathFragments = filePath.toString().split("/");
		return pathFragments[pathFragments.length - 1];
	}
}
