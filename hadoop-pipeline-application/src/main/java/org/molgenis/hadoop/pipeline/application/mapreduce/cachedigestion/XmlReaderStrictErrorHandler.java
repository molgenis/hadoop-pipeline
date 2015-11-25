/**
 * The Javadoc from Oracle's org.xml.saxErrorHandler was used to create the Javadoc present in this file.
 */
package org.molgenis.hadoop.pipeline.application.mapreduce.cachedigestion;

import org.apache.log4j.Logger;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * {@link ErrorHandler} implementation that only allows {@link #warning(SAXParseException)} to continue parsing (in
 * which case a message is written to a {@link Logger}). Any {@link #error(SAXParseException)} such as the XML file not
 * adhering to the given {@link Schema} will throw an {@link SAXParseException}. This is also the case for any
 * {@link #fatalError(SAXParseException)}.
 */
public class XmlReaderStrictErrorHandler implements ErrorHandler
{
	/**
	 * Logger to write information to.
	 */
	private static final Logger logger = Logger.getLogger(XmlReaderStrictErrorHandler.class);

	@Override
	public void warning(SAXParseException exception) throws SAXException
	{
		logger.error(exception.getMessage());
	}

	@Override
	public void error(SAXParseException exception) throws SAXException
	{
		throw exception;
	}

	@Override
	public void fatalError(SAXParseException exception) throws SAXException
	{
		throw exception;
	}

}
