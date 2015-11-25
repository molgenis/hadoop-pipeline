package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import static java.util.Objects.requireNonNull;

/**
 * Stores information about a specific tool as stored in a tools XML file (see {@code src/main/resources} for the XML
 * Schema). This data can then be used within a {@link org.apache.hadoop.mapreduce.Mapper} or
 * {@link org.apache.hadoop.mapreduce.Reducer}.
 */
public class Tool
{
	/**
	 * The name of the executable as stored in the tools archive.
	 */
	String fileName;

	/**
	 * The value for {@code ID:} as should be used when writing an {@code @PG} tag.
	 */
	String id;

	/**
	 * The value for {@code PN:} as should be used when writing an {@code @PG} tag.
	 */
	String name;

	/**
	 * The value for {@code VN:} as should be used when writing an {@code @PG} tag.
	 */
	String version;

	String getFileName()
	{
		return fileName;
	}

	String getId()
	{
		return id;
	}

	String getName()
	{
		return name;
	}

	String getVersion()
	{
		return version;
	}

	/**
	 * Create a new {@link Tool} instance.
	 * 
	 * @param fileName
	 *            {@link String}
	 * @param id
	 *            {@link String}
	 * @param name
	 *            {@link String}
	 * @param version
	 *            {@link String}
	 */
	Tool(String fileName, String id, String name, String version)
	{
		this.fileName = requireNonNull(fileName);
		this.id = requireNonNull(id);
		this.name = requireNonNull(name);
		this.version = requireNonNull(version);
	}

	/**
	 * Retrieve a {@link String} that is formatted according to a {@code @PG} tag from the SAM-format.
	 * 
	 * @return {@link String}
	 * 
	 * @see <a href=
	 *      'https://samtools.github.io/hts-specs/SAMv1.pdf#page=3'>https://samtools.github.io/hts-specs/SAMv1.pdf#page=3</a>
	 */
	public String getSamString()
	{
		return "@PG\tID:" + id + "\tPN:" + name + "\tVN:" + version;
	}

	@Override
	public String toString()
	{
		return "Tool [fileName=" + fileName + ", id=" + id + ", name=" + name + ", version=" + version + "]";
	}
}
