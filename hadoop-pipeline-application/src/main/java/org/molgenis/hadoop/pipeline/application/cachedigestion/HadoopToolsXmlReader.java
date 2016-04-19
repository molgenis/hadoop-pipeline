package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;

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
public class HadoopToolsXmlReader extends HadoopXmlReader<Map<String, SAMProgramRecord>>
{
	/**
	 * A {@link File} that stores the validation {@link Schema}. This file should be present within the created jar
	 * after compiling.
	 */
	private final URL schemaFile = getClass().getClassLoader().getResource("tools_archive_info.xsd");

	/**
	 * Reads and digests an XML-formatted {@link inputStream} that adheres to the format as defined in the
	 * {@link Scheme} found in {@code src/main/resources/tools_archive_info.xsd}.
	 * 
	 * @return {@link Map}{@code <}{@link String}{@code ,}{@link SAMProgramRecord}{@code >}
	 */
	@Override
	public Map<String, SAMProgramRecord> read(InputStream inputStream) throws IOException
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
	 * {@code src/main/resources/tools_archive_info.xsd}. IMPORTANT: If XML-formatted {@link Document} does not adhere
	 * to the scheme, this might cause unexpected behavior!
	 * 
	 * @param dom
	 *            {@link Document}
	 * @return {@link Map}{@code <}{@link String}{@code ,}{@link SAMProgramRecord}{@code >}
	 */
	private Map<String, SAMProgramRecord> digestDomStructure(Document dom)
	{
		// Map to store digested data in.
		Map<String, SAMProgramRecord> tools = new HashMap<String, SAMProgramRecord>();

		// Retrieve root node and it's children.
		Element rootNode = dom.getDocumentElement();
		NodeList toolNodes = rootNode.getElementsByTagName("tool");

		// Goes through all tool nodes.
		for (int i = 0; i < toolNodes.getLength(); i++)
		{
			// Retrieves the tool node.
			Node toolNode = toolNodes.item(i);

			// Retrieves information about the tool.
			String fileName = toolNode.getAttributes().getNamedItem("fileName").getNodeValue();
			Element toolElement = (Element) toolNode;
			String id = toolElement.getElementsByTagName("id").item(0).getTextContent();
			String name = toolElement.getElementsByTagName("name").item(0).getTextContent();
			String version = toolElement.getElementsByTagName("version").item(0).getTextContent();

			// Creates a new tool instance and adds it to the ArrayList for storing.
			SAMProgramRecord program = new SAMProgramRecord(id);
			program.setProgramName(name);
			program.setProgramVersion(version);
			tools.put(fileName, program);
		}

		return tools;
	}
}
