package org.molgenis.hadoop.pipeline.application;

/**
 * Superclass for the TestNG tests containing general code.
 */
public abstract class Tester
{
	/**
	 * ClassLoader object to view files on disk for testing purposes.
	 */
	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	// System.out.println(classLoader.getResource("").toString());
	// Output: <path/to/dir>/hadoop-pipeline/hadoop-pipeline-application/target/test-classes/

	protected ClassLoader getClassLoader()
	{
		return classLoader;
	}

}
