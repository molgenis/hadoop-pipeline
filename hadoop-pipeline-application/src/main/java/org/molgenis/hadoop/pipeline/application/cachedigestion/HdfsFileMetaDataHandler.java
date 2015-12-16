package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.net.URI;

import org.apache.hadoop.fs.Path;

/**
 * Abstract class for basic HDFS file/directory information handling.
 */
public abstract class HdfsFileMetaDataHandler
{
	/**
	 * Retrieves the file name (so without the complete path to the file).
	 * 
	 * @param filePath
	 *            {@link String} complete path to a file.
	 * @return {@link String} the file name.
	 */
	public static String retrieveFileName(String filePath)
	{
		String[] pathFragments = filePath.split("/");
		return pathFragments[pathFragments.length - 1];
	}

	/**
	 * Wrapper for {@link HdfsFileMetaDataHandler#retrieveFileName(String)}.
	 * 
	 * @param filePath
	 *            {@link String} complete path to a file.
	 * @return {@link String} the file name.
	 */
	public static String retrieveFileName(URI filePath)
	{
		return retrieveFileName(filePath.toString());
	}

	/**
	 * Wrapper for {@link HdfsFileMetaDataHandler#retrieveFileName(String)}.
	 * 
	 * @param filePath
	 *            {@link String} complete path to a file.
	 * @return {@link String} the file name.
	 */
	public static String retrieveFileName(Path filePath)
	{
		return retrieveFileName(filePath.toString());
	}
}
