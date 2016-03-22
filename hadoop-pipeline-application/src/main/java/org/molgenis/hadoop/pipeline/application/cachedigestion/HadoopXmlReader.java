package org.molgenis.hadoop.pipeline.application.cachedigestion;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

/**
 * Stores some basic functionalities for XML-format digestion. Should be subclassed for creating a reader that digests a
 * specific XML-format.
 * 
 * @param <T>
 */
public abstract class HadoopXmlReader<T> extends HadoopFileReader<T>
{
	/**
	 * Retrieve a {@link Schema} to be used for XML file validation.
	 * 
	 * @param schemaFile
	 *            {@link URL}
	 * @return {@link Schema}
	 * @throws SAXException
	 */
	Schema retrieveSchema(URL schemaFile) throws SAXException
	{
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		return schemaFactory.newSchema(schemaFile);
	}

	/**
	 * Generates a parsed XML {@link Document} which can be used for retrieving node data. Uses the given {@link Schema}
	 * combined with the given {@link ErrorHandler} for digesting the XML formatted {@link InputStream}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @param schema
	 *            {@link Schema}
	 * @param errorHandler
	 *            {@link ErrorHandler}
	 * @return {@link Document}
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	Document generateParsedXmlDocument(InputStream inputStream, Schema schema, ErrorHandler errorHandler)
			throws ParserConfigurationException, SAXException, IOException
	{
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		docBuilderFactory.setSchema(schema);
		DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
		docBuilder.setErrorHandler(errorHandler);
		return docBuilder.parse(inputStream);
	}

	/**
	 * Generates a parsed XML {@link Document} which can be used for retrieving node data. Uses the given {@link Schema}
	 * for validating the XML-formatted {@link InputStream}. Is the same as
	 * {@link #generateParsedXmlDocument(InputStream, Schema, ErrorHandler)} with as {@link ErrorHandler} a new instance
	 * of {@link XmlReaderStrictErrorHandler}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @param schema
	 *            {@link Schema}
	 * @return {@link Document}
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @see {@link #generateParsedXmlDocument(InputStream, Schema, ErrorHandler)}
	 */
	Document generateParsedXmlDocument(InputStream inputStream, Schema schema)
			throws ParserConfigurationException, SAXException, IOException
	{
		return generateParsedXmlDocument(inputStream, schema, new XmlReaderStrictErrorHandler());
	}

	/**
	 * Generates a parsed XML {@link Document} which can be used for retrieving node data. Uses no {@link Schema} for
	 * validation and as {@link ErrorHandler} it uses the default behavior from {@link DocumentBuilder}. Is the same as
	 * {@link #generateParsedXmlDocument(InputStream, Schema, ErrorHandler)} with as {@link Schema} and
	 * {@link ErrorHandler} both {@code null}.
	 * 
	 * @param inputStream
	 *            {@link InputStream}
	 * @return {@link Document}
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @see {@link #generateParsedXmlDocument(InputStream, Schema, ErrorHandler)}
	 */
	Document generateParsedXmlDocument(InputStream inputStream)
			throws ParserConfigurationException, SAXException, IOException
	{
		return generateParsedXmlDocument(inputStream, null, null);
	}
}
