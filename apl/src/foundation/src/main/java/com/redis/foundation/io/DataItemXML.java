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
import com.redis.foundation.data.DataRange;
import com.redis.foundation.ds.DS;
import com.redis.foundation.std.IOXML;
import com.redis.foundation.std.XMLUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.w3c.dom.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Parses and saves a DataItem as an collection of XML entities.
 */
public class DataItemXML
{
	private DataRangeXML mDataRangeXML;

	public DataItemXML()
	{
		mDataRangeXML = new DataRangeXML();
	}

	public void saveNode(PrintWriter aPW, int anIndentAmount)
	{
		IOXML.indentLine(aPW, anIndentAmount);
		aPW.printf("<%s", IO.XML_ITEM_NODE_NAME);
	}

	public void saveAttr(PrintWriter aPW, DataItem aDataItem)
		throws IOException
	{
		String featureName;

		IOXML.writeAttrNameValue(aPW, "type", Data.typeToString(aDataItem.getType()));
		IOXML.writeAttrNameValue(aPW, "name", aDataItem.getName());
		IOXML.writeAttrNameValue(aPW, "title", aDataItem.getTitle());
		if (aDataItem.isMultiValue())
			IOXML.writeAttrNameValue(aPW, "isMultiValue", aDataItem.isMultiValue());
		if (aDataItem.getDisplaySize() > 0)
			IOXML.writeAttrNameValue(aPW, "displaySize", aDataItem.getDisplaySize());
		IOXML.writeAttrNameValue(aPW, "defaultValue", aDataItem.getDefaultValue());
		IOXML.writeAttrNameValue(aPW, "dataFormat", aDataItem.getDataFormat());
		IOXML.writeAttrNameValue(aPW, "uiFormat", aDataItem.getUIFormat());
		if (aDataItem.isSorted())
			IOXML.writeAttrNameValue(aPW, "sortOrder", aDataItem.getSortOrder().name());
		for (Map.Entry<String, String> featureEntry : aDataItem.getFeatures().entrySet())
		{
			featureName = featureEntry.getKey();
			if (! mDataRangeXML.isRangeFeature(featureName))
				IOXML.writeAttrNameValue(aPW, featureName, featureEntry.getValue());
		}
		if ((aDataItem.isRangeAssigned()) && (aDataItem.getRange().getType() != Data.Type.Undefined))
			mDataRangeXML.saveAttr(aPW, aDataItem.getRange());
	}

	public void saveValue(PrintWriter aPW, DataItem aDataItem, int anIndentAmount)
	{
		if (aDataItem.isMultiValue())
		{
			IOXML.indentLine(aPW, anIndentAmount);
			String mvDelimiter = aDataItem.getFeature(Data.FEATURE_MV_DELIMITER);
			if (StringUtils.isNotEmpty(mvDelimiter))
				aPW.printf("%s</%s>%n", StringEscapeUtils.escapeXml10(aDataItem.getValuesCollapsed(mvDelimiter.charAt(0))),
						   IO.XML_ITEM_NODE_NAME);
			else
				aPW.printf("%s</%s>%n", StringEscapeUtils.escapeXml10(aDataItem.getValuesCollapsed()),
						   IO.XML_ITEM_NODE_NAME);
		}
		else
		{
			IOXML.indentLine(aPW, anIndentAmount);
			if (aDataItem.isFeatureTrue(DS.FEATURE_IS_CONTENT))
				aPW.printf("%s</%s>%n", XMLUtl.escapeElemStrValue(aDataItem.getValue()),
						   IO.XML_ITEM_NODE_NAME);
			else
				aPW.printf("%s</%s>%n", StringEscapeUtils.escapeXml10(aDataItem.getValue()),
						   IO.XML_ITEM_NODE_NAME);
		}
	}

	public DataItem load(Element anElement)
		throws IOException
	{
		Attr nodeAttr;
		Node nodeItem;
		DataItem dataItem;
		Data.Type fieldType;
		String nodeName, nodeValue;

		String attrValue = anElement.getAttribute("name");
		if (StringUtils.isNotEmpty(attrValue))
		{
			String fieldName = attrValue;
			attrValue = anElement.getAttribute("type");
			if (StringUtils.isNotEmpty(attrValue))
				fieldType = Data.stringToType(attrValue);
			else
				fieldType = Data.Type.Text;
			dataItem = new DataItem.Builder().type(fieldType).name(fieldName).build();

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
					else if (StringUtils.equalsIgnoreCase(nodeName, "rangeType"))
					{
						DataRange dataRange = mDataRangeXML.load(anElement);
						if (dataRange != null)
							dataItem.setRange(dataRange);
					}
					else if (StringUtils.equalsIgnoreCase(nodeName, "title"))
						dataItem.setTitle(nodeValue);
					else if (StringUtils.equalsIgnoreCase(nodeName, "displaySize"))
						dataItem.setDisplaySize(Data.createInt(nodeValue));
					else if (StringUtils.equalsIgnoreCase(nodeName, "sortOrder"))
						dataItem.setSortOrder(Data.Order.valueOf(nodeValue));
					else if (StringUtils.equalsIgnoreCase(nodeName, "defaultValue"))
						dataItem.setDefaultValue(nodeValue);
					else if (StringUtils.equalsIgnoreCase(nodeName, "uiFormat"))
						dataItem.setUIFormat(nodeValue);
					else if (StringUtils.equalsIgnoreCase(nodeName, "dataFormat"))
						dataItem.setDataFormat(nodeValue);
					else
						dataItem.addFeature(nodeName, nodeValue);
				}
			}

// Process value content for the item - if it was defined.

			nodeItem = (Node) anElement;
			if (dataItem.isFeatureTrue(DS.FEATURE_IS_CONTENT))
				nodeValue = XMLUtl.getNodeCDATAValue(nodeItem);
			else
				nodeValue = XMLUtl.getNodeStrValue(nodeItem);
			if (StringUtils.isNotEmpty(nodeValue))
			{
				if (dataItem.isMultiValue())
				{
					String mvDelimiter = dataItem.getFeature(Data.FEATURE_MV_DELIMITER);
					if (StringUtils.isNotEmpty(mvDelimiter))
						dataItem.expandAndSetValues(nodeValue, mvDelimiter.charAt(0));
					else
						dataItem.expandAndSetValues(nodeValue);
				}
				else
					dataItem.setValue(nodeValue);
			}
		}
		else
			dataItem = null;

		return dataItem;
	}
}
