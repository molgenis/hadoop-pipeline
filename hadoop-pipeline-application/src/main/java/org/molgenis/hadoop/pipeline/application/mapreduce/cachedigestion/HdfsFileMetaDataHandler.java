package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.net.URI;

import org.apache.hadoop.fs.Path;

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
	public static String retrieveFileName(String filePath)
	{
		String[] pathFragments = filePath.split("/");
		return pathFragments[pathFragments.length - 1];
	}

	/**
	 * Retrieves the file name without the path before it.
	 * 
	 * @param filePath
	 * @return {@link String}
	 */
	public static String retrieveFileName(URI filePath)
	{
		return retrieveFileName(filePath.toString());
	}

	/**
	 * Retrieves the file name without the path before it.
	 * 
	 * @param filePath
	 * @return {@link String}
	 */
	public static String retrieveFileName(Path filePath)
	{
		return retrieveFileName(filePath.toString());
	}
}
