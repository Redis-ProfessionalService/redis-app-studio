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

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterion;
import com.redis.foundation.ds.DSCriterionEntry;
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
import java.util.ArrayList;
import java.util.Map;

/**
 * The DSCriteriaXML provides a collection of methods that can generate/load
 * an XML representation of a {@link DSCriteria} object.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DSCriteriaXML
{
	private DSCriteria mDSCriteria;
	private DataItemXML mDataItemXML;

	/**
	 * Default constructor.
	 */
	public DSCriteriaXML()
	{
		mDSCriteria = new DSCriteria();
		mDataItemXML = new DataItemXML();
	}

	/**
	 * Constructor that identifies a criteria prior to a save operation.
	 *
	 * @param aCriteria Data source criteria.
	 */
	public DSCriteriaXML(DSCriteria aCriteria)
	{
		mDSCriteria = aCriteria;
		mDataItemXML = new DataItemXML();
	}

	/**
	 * Returns a reference to the {@link DSCriteria} being managed by
	 * this class.
	 *
	 * @return Data source criteria.
	 */
	public DSCriteria getCriteria()
	{
		return mDSCriteria;
	}

	/**
	 * Saves the previous assigned criteria (e.g. via constructor or set method)
	 * to the print writer stream wrapped in a tag name specified in the parameter.
	 *
	 * @param aPW            PrintWriter stream instance.
	 * @param aTagName       Tag name.
	 * @param anIndentAmount Indentation count.
	 *
	 * @throws java.io.IOException I/O related exception.
	 */
	public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
		throws IOException
	{
		DataItem dataItem;
		DSCriterion dsCriterion;

		String tagName = StringUtils.remove(aTagName, StrUtl.CHAR_SPACE);
		ArrayList<DSCriterionEntry> dsCriterionEntries = mDSCriteria.getCriterionEntries();
		int ceCount = dsCriterionEntries.size();

		IOXML.indentLine(aPW, anIndentAmount);
		aPW.printf("<%s", tagName);
		IOXML.writeAttrNameValue(aPW, "type", IO.extractType(mDSCriteria.getClass().getName()));
		IOXML.writeAttrNameValue(aPW, "name", mDSCriteria.getName());
		IOXML.writeAttrNameValue(aPW, "version", IO.CRITERIA_XML_FORMAT_VERSION);
		IOXML.writeAttrNameValue(aPW, "operator", Data.operatorToString(Data.Operator.AND));
		IOXML.writeAttrNameValue(aPW, "count", ceCount);
		for (Map.Entry<String, String> featureEntry : mDSCriteria.getFeatures().entrySet())
			IOXML.writeAttrNameValue(aPW, featureEntry.getKey(), featureEntry.getValue());
		aPW.printf(">%n");

		if (ceCount > 0)
		{
			for (DSCriterionEntry ce : dsCriterionEntries)
			{
				dsCriterion = ce.getCriterion();

				dataItem = new DataItem(dsCriterion.getItem());
				dataItem.addFeature("operator", Data.operatorToString(ce.getLogicalOperator()));
				mDataItemXML.saveNode(aPW, anIndentAmount+1);
				mDataItemXML.saveAttr(aPW, dataItem);
				mDataItemXML.saveValue(aPW, dataItem, anIndentAmount+1);
			}
		}

		IOXML.indentLine(aPW, anIndentAmount);
		aPW.printf("</%s>%n", tagName);
	}

	/**
	 * Saves the previous assigned criteria (e.g. via constructor or set method)
	 * to the print writer stream specified as a parameter.
	 *
	 * @param aPW PrintWriter stream instance.
	 *
	 * @throws java.io.IOException I/O related exception.
	 */
	public void save(PrintWriter aPW)
		throws IOException
	{
		save(aPW, IO.XML_CRITERIA_NODE_NAME, 0);
	}

	/**
	 * Saves the previous assigned criteria (e.g. via constructor or set method)
	 * to the path/file name specified as a parameter.
	 *
	 * @param aPathFileName Absolute file name.
	 *
	 * @throws java.io.IOException I/O related exception.
	 */
	public void save(String aPathFileName)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
		{
			save(printWriter);
		}
	}

	/**
	 * Parses an XML DOM element and loads it into a criteria.
	 *
	 * @param anElement DOM element.
	 *
	 * @throws java.io.IOException I/O related exception.
	 */
	public void load(Element anElement)
		throws IOException
	{
		Node nodeItem;
		Attr nodeAttr;
		DataItem dataItem;
		Element nodeElement;
		String nodeName, nodeValue, logicalOperator;

		String className = mDSCriteria.getClass().getName();
		String attrValue = anElement.getAttribute("type");
		if ((StringUtils.isNotEmpty(attrValue)) &&
				(! IO.isTypesEqual(attrValue, className)))
			throw new IOException("Unsupported type: " + attrValue);

		attrValue = anElement.getAttribute("name");
		if (StringUtils.isNotEmpty(attrValue))
			mDSCriteria.setName(attrValue);

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
						(StringUtils.equalsIgnoreCase(nodeName, "type")))
					continue;
				else
					mDSCriteria.addFeature(nodeName, nodeValue);
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
				{
					logicalOperator = dataItem.getFeature("operator");
					if (StringUtils.isEmpty(logicalOperator))
						logicalOperator = Data.operatorToString(Data.Operator.EQUAL);
					mDSCriteria.add(dataItem, Data.stringToOperator(logicalOperator));
				}
			}
		}
	}

	/**
	 * Parses an XML DOM element and loads it into a bag/table.
	 *
	 * @param anIS Input stream.
	 *
	 * @throws java.io.IOException                            I/O related exception.
	 * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
	 * @throws org.xml.sax.SAXException                       XML parser related exception.
	 */
	public void load(InputStream anIS)
		throws ParserConfigurationException, IOException, SAXException
	{
		DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
		InputSource inputSource = new InputSource(anIS);
		Document xmlDocument = docBuilder.parse(inputSource);
		xmlDocument.getDocumentElement().normalize();

		load(xmlDocument.getDocumentElement());
	}

	/**
	 * Parses an XML file identified by the path/file name parameter
	 * and loads it into a bag/table.
	 *
	 * @param aPathFileName Absolute file name.
	 *
	 * @throws java.io.IOException                            I/O related exception.
	 * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
	 * @throws org.xml.sax.SAXException                       XML parser related exception.
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

		load(xmlDocument.getDocumentElement());
	}
}
