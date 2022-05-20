/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.redis.foundation.io;

import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.IOXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DataDocXML
{
	private final String XML_DATADOC_NODE_NAME = "DataDoc";

	private DataDoc mDataDoc;
	private int mChildNumber = 0;
	private boolean mIsHeaderSaved;
	private final DataItemXML mDataItemXML;

	/**
	 * Default constructor.
	 */
	public DataDocXML()
	{
		mDataDoc = new DataDoc("Data Document");
		mDataItemXML = new DataItemXML();
	}

	/**
	 * Constructor accepts a data document instance as a parameter.
	 *
	 * @param aDataDoc Data document instance.
	 */
	public DataDocXML(DataDoc aDataDoc)
	{
		setDataDoc(aDataDoc);
		mDataItemXML = new DataItemXML();
	}

	/**
	 * Assigns the bag parameter to the internally managed data document instance.
	 *
	 * @param aDataDoc Data document instance.
	 */
	public void setDataDoc(DataDoc aDataDoc)
	{
		mDataDoc = aDataDoc;
	}

	/**
	 * Returns a reference to the internally managed bag instance.
	 *
	 * @return Data document instance.
	 */
	public DataDoc getDataDoc()
	{
		return mDataDoc;
	}

	/**
	 * Assign a flag to indicate if the header details should be saved to XML.
	 *
	 * @param anIsHeaderSaved <i>true</i> saves header details
	 */
	public void setHeaderSaveFlag(boolean anIsHeaderSaved)
	{
		mIsHeaderSaved = anIsHeaderSaved;
	}

	private void save(PrintWriter aPW, DataDoc aDataDoc, String aTagName, int anIndentAmount)
		throws IOException
	{
		IOXML.indentLine(aPW, anIndentAmount);
		String tagName = StringUtils.remove(aTagName, StrUtl.CHAR_SPACE);
		aPW.printf("<%s", tagName);
		if (mIsHeaderSaved)
		{
			IOXML.writeAttrNameValue(aPW, "type", IO.extractType(aDataDoc.getClass().getName()));
			IOXML.writeAttrNameValue(aPW, "name", aDataDoc.getName());
			IOXML.writeAttrNameValue(aPW, "title", aDataDoc.getTitle());
			IOXML.writeAttrNameValue(aPW, "count", aDataDoc.count());
			IOXML.writeAttrNameValue(aPW, "version", IO.DATADOC_XML_FORMAT_VERSION);
		}
		for (Map.Entry<String, String> featureEntry : aDataDoc.getFeatures().entrySet())
			IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
		aPW.printf(">%n");

		if (aDataDoc.count() > 0)
		{
			for (DataItem dataItem : aDataDoc.getItems())
			{
				if (dataItem.isValueAssigned())
					mDataItemXML.saveValue(aPW, dataItem, anIndentAmount + 1);
				else
				{
					mDataItemXML.saveNode(aPW, anIndentAmount + 1);
					mDataItemXML.saveAttr(aPW, dataItem);
					aPW.printf("/>%n");
				}
			}
		}
		else
			aPW.printf(">%n");

		if (mDataDoc.childrenCount() > 0)
		{
			aDataDoc.getChildDocs().entrySet().stream()
					.forEach(e -> {
						try
						{
							IOXML.indentLine(aPW, anIndentAmount + 1);
							aPW.printf("<%s", IO.XML_CHILD_NODE_NAME);
							IOXML.writeAttrNameValue(aPW, "name", e.getKey());
							aPW.printf(">%n");
							for (DataDoc dataDoc : e.getValue())
								save(aPW, dataDoc, XML_DATADOC_NODE_NAME, anIndentAmount+2);
							IOXML.indentLine(aPW, anIndentAmount + 1);
							aPW.printf("</%s>%n", IO.XML_CHILD_NODE_NAME);
						}
						catch (IOException ignored)
						{
						}
					});
		}

		IOXML.indentLine(aPW, anIndentAmount);
		aPW.printf("</%s>%n", tagName);
	}

	/**
	 * Saves the previous assigned data document (e.g. via constructor or set method)
	 * to the print writer stream wrapped in a tag name specified in the parameter.
	 *
	 * @param aPW PrintWriter stream instance.
	 * @param aTagName Tag name.
	 * @param anIndentAmount Indentation count.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
		throws IOException
	{
		save(aPW, mDataDoc, aTagName, anIndentAmount);
	}

	/**
	 * Saves the previous assigned data document instance (e.g. via constructor or set method)
	 * to the print writer stream specified as a parameter.
	 *
	 * @param aPW PrintWriter stream instance.
	 *
	 * @throws java.io.IOException I/O related exception.
	 */
	public void save(PrintWriter aPW)
		throws IOException
	{
		save(aPW, XML_DATADOC_NODE_NAME, 0);
	}

	/**
	 * Saves the previous assigned data document instance (e.g. via constructor or set method)
	 * to the path/file name specified as a parameter.
	 *
	 * @param aPathFileName Absolute file name.
	 * @param aTagName Tag name.
	 * @throws java.io.IOException I/O related exception.
	 */
	public void save(String aPathFileName, String aTagName)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			save(printWriter, aTagName, 0);
		}
	}

	/**
	 * Saves the previous assigned data document instance (e.g. via constructor or set method)
	 * to the path/file name specified as a parameter.
	 *
	 * @param aPathFileName Absolute file name.
	 *
	 * @throws java.io.IOException I/O related exception.
	 */
	public void save(String aPathFileName)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			save(printWriter);
		}
	}

	private void loadChild(DataDoc aParentDoc, Element anElement)
		throws IOException
	{
		Node nodeItem;
		String nodeName;
		DataDoc dataDoc;
		Element nodeElement;

		mChildNumber++;
		String attrValue = anElement.getAttribute("name");
		if (StringUtils.isEmpty(attrValue))
			attrValue = String.format("Child %d", mChildNumber);

		NodeList nodeList = anElement.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++)
		{
			nodeItem = nodeList.item(i);

			if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
				continue;

			nodeName = nodeItem.getNodeName();
			if (nodeName.equalsIgnoreCase(XML_DATADOC_NODE_NAME))
			{
				nodeElement = (Element) nodeItem;
				dataDoc = loadDataDoc(nodeElement);
				if (dataDoc.count() > 0)
					aParentDoc.addChild(attrValue, dataDoc);
			}
		}
	}

	private DataDoc loadDataDoc(Element anElement)
		throws IOException
	{
		Node nodeItem;
		Attr nodeAttr;
		DataItem dataItem;
		Element nodeElement;
		String nodeName, nodeValue;

		DataDoc dataDoc = new DataDoc("Data Document");
		String className = dataDoc.getClass().getName();
		String attrValue = anElement.getAttribute("type");
		if ((StringUtils.isNotEmpty(attrValue)) && (! IO.isTypesEqual(attrValue, className)))
			throw new IOException("Unsupported type: " + attrValue);

		attrValue = anElement.getAttribute("name");
		if (StringUtils.isNotEmpty(attrValue))
			dataDoc.setName(attrValue);
		attrValue = anElement.getAttribute("title");
		if (StringUtils.isNotEmpty(attrValue))
			dataDoc.setTitle(attrValue);

		NamedNodeMap namedNodeMap = anElement.getAttributes();
		int attrCount = namedNodeMap.getLength();
		for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
		{
			nodeAttr = (Attr) namedNodeMap.item(attrOffset);
			nodeName = nodeAttr.getNodeName();
			nodeValue = nodeAttr.getNodeValue();

			if (StringUtils.isNotEmpty(nodeValue))
			{
				if ((StringUtils.equalsIgnoreCase(nodeName, "name")) ||
					(StringUtils.equalsIgnoreCase(nodeName, "type")) ||
					(StringUtils.equalsIgnoreCase(nodeName, "count")) ||
					(StringUtils.equalsIgnoreCase(nodeName, "title")) ||
					(StringUtils.equalsIgnoreCase(nodeName, "version")))
					continue;
				else
					dataDoc.addFeature(nodeName, nodeValue);
			}
		}

		NodeList nodeList = anElement.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++)
		{
			nodeItem = nodeList.item(i);

			if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
				continue;

			nodeName = nodeItem.getNodeName();
			if (nodeName.equalsIgnoreCase(IO.XML_ITEM_NODE_NAME))
			{
				nodeElement = (Element) nodeItem;
				dataItem = mDataItemXML.load(nodeElement);
				if (dataItem != null)
					dataDoc.add(dataItem);
			}
			else if (nodeName.equalsIgnoreCase(IO.XML_CHILD_NODE_NAME))
			{
				nodeElement = (Element) nodeItem;
				loadChild(dataDoc, nodeElement);
			}
		}

		return dataDoc;
	}

	/**
	 * Parses an XML DOM element and loads it into a data document instance.
	 *
	 * @param anIS Input stream.
	 *
	 * @throws IOException I/O related exception.
	 * @throws ParserConfigurationException XML parser related exception.
	 * @throws SAXException XML parser related exception.
	 */
	public void load(InputStream anIS)
		throws ParserConfigurationException, IOException, SAXException
	{
		DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
		InputSource inputSource = new InputSource(anIS);
		Document xmlDocument = docBuilder.parse(inputSource);
		xmlDocument.getDocumentElement().normalize();

		mDataDoc = loadDataDoc(xmlDocument.getDocumentElement());
	}

	/**
	 * Parses an XML file identified by the path/file name parameter
	 * and loads it into a bag/table.
	 *
	 * @param aPathFileName Absolute file name.
	 *
	 * @throws IOException                  I/O related exception.
	 * @throws ParserConfigurationException XML parser related exception.
	 * @throws SAXException                 XML parser related exception.
	 */
	public void load(String aPathFileName)
		throws IOException, ParserConfigurationException, SAXException
	{
		File xmlFile = new File(aPathFileName);
		if (! xmlFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
		Document xmlDocument = docBuilder.parse(new File(aPathFileName));
		xmlDocument.getDocumentElement().normalize();

		mDataDoc = loadDataDoc(xmlDocument.getDocumentElement());
	}
}
