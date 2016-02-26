package org.molgenis.hadoop.pipeline.application;

/**
 * Superclass for the TestNG tests containing general code. Furthermore, it contains some generic testing methods that
 * are used among multiple test packages.
 */
public abstract class Tester
{
	/**
	 * ClassLoader object to view test resource files. Test files can be retrieved using {@code getResource()}, where an
	 * empty {@link String} will refer to the folder {@code target/test-classes}.
	 */
	private static final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

	protected static ClassLoader getClassLoader()
	{
		return classLoader;
	}
}
