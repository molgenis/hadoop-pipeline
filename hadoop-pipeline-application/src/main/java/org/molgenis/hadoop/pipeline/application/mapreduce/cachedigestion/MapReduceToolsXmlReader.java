package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;

import org.apache.hadoop.fs.FileSystem;
import org.apache.http.conn.scheme.Scheme;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import htsjdk.samtools.SAMProgramRecord;

/**
 * Digests an XML {@link InputStream} formatted according to the {@link Schema} from
 * {@code src/main/resources/tools_archive_info.xsd} (and within the jar after compiling).
 */
public class MapReduceToolsXmlReader extends MapReduceXmlReader<HashMap<String, SAMProgramRecord>>
{
	/**
	 * Stores the attribute that stores the file name in each tool node within the XML file.
	 */
	static final String FILE_NAME = "fileName";
	/**
	 * A {@link File} that stores the validation {@link Schema}. This file should be present within the created jar
	 * after compiling.
	 */
	final URL schemaFile = getClass().getClassLoader().getResource("tools_archive_info.xsd");

	/**
	 * Create a new {@link MapReduceToolsXmlReader} instance.
	 * 
	 * @param fileSys
	 *            {@link FileSystem}
	 */
	public MapReduceToolsXmlReader(FileSystem fileSys)
	{
		super(fileSys);
	}

	/**
	 * Reads and digests an XML-formatted {@link inputStream} that adheres to the format as defined in the
	 * {@link Scheme} found in {@code src/main/resources/tools_archive_info.xsd}.
	 * 
	 * @return {@link ArrayList}{@code <}{@link Tool}{@code >}
	 */
	@Override
	public HashMap<String, SAMProgramRecord> read(InputStream inputStream) throws IOException
	{
		try
		{
			Schema schema = retrieveSchema(schemaFile);
			Document doc = generateParsedXmlDocument(inputStream, schema, new XmlReaderStrictErrorHandler());
			return digestDomStructure(doc);
		}
		catch (ParserConfigurationException | SAXException e)
		{
			throw new IOException(e);
		}
	}

	/**
	 * Digestion of the XML-formatted {@link Document} that adheres to the {@link Scheme} defined in
	 * {@code src/main/resources/tools_archive_info.xsd}.
	 * 
	 * @param dom
	 *            {@link Document}
	 * @return {@link ArrayList}{@code <}{@link SAMProgramRecord}{@code >}
	 */
	private HashMap<String, SAMProgramRecord> digestDomStructure(Document dom)
	{
		// ArrayList to store digested data in.
		HashMap<String, SAMProgramRecord> tools = new HashMap<String, SAMProgramRecord>();

		// Retrieve root node and it's children.
		Element rootNode = dom.getDocumentElement();
		NodeList toolNodes = rootNode.getElementsByTagName("tool");

		// Goes through all tool nodes.
		for (int i = 0; i < toolNodes.getLength(); i++)
		{
			Node toolNode = toolNodes.item(i);

			// Resets the needed node data for each tool.
			String fileName = toolNode.getAttributes().getNamedItem(FILE_NAME).getNodeValue();
			String toolId = null;
			String toolName = null;
			String toolVersion = null;

			// Retrieves info nodes of a single tool node.
			NodeList toolInfoNodes = toolNode.getChildNodes();

			//  Goes through all tool node info nodes.
			for (int j = 0; j < toolInfoNodes.getLength(); j++)
			{
				Node toolInfoNode = toolInfoNodes.item(j);

				// Only digests child nodes that are actual elements (skip "#text" nodes).
				if (toolInfoNode.getNodeType() == Node.ELEMENT_NODE)
				{
					// Retrieves the tool child nodes. ToolField.getEnum(fieldString) should not be able to return null
					// if the xml file adheres to the given scheme.
					switch (ToolChildNode.getEnum(toolInfoNode.getNodeName()))
					{
						case ID:
							toolId = toolInfoNode.getTextContent();
							break;
						case NAME:
							toolName = toolInfoNode.getTextContent();
							break;
						case VERSION:
							toolVersion = toolInfoNode.getTextContent();
					}
				}
			}

			// Creates a new tool instance and adds it to the ArrayList for storing.
			SAMProgramRecord program = new SAMProgramRecord(toolId);
			program.setProgramName(toolName);
			program.setProgramVersion(toolVersion);
			tools.put(fileName, program);
		}

		return tools;
	}
}
