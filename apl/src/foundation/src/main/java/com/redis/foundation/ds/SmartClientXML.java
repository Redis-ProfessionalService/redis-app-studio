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

package com.redis.foundation.ds;

import com.redis.foundation.data.*;
import com.redis.foundation.std.IOXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * The SmartClientXML provides a collection of methods that can generate/load
 * an XML representation of a SmartClient Datasource object.  In general, the
 * developer should use the data source I/O methods instead of this helper
 * implementation.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SmartClientXML
{
	public SmartClientXML()
	{
	}

	private void saveRecord(PrintWriter aPW, DataDoc aDataDoc)
	{
		aPW.printf("<record>%n");
		for (DataItem dataItem : aDataDoc.getItems())
		{
			aPW.printf("<%s>%s</%s>", dataItem.getName(),
					   StringEscapeUtils.escapeXml10(dataItem.getValuesCollapsed()),
					   dataItem.getName());
		}
		aPW.printf("</record>%n");
	}

	public void saveDocResponse(PrintWriter aPW, DataDoc aDataDoc)
		throws IOException
	{
		if (aPW == null)
			throw new IOException("PrintWriter output stream is null.");

		aPW.printf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n");
		aPW.printf("<response>%n");
		aPW.printf("<status>0</status>%n");

		aPW.printf("<startRow>0</startRow>%n");
		aPW.printf("<endRow>1</endRow>%n");
		aPW.printf("<totalRows>1</totalRows>%n");
		aPW.printf("<data>%n");
		saveRecord(aPW, aDataDoc);
		aPW.printf("</data>%n");
	}

	public void saveDataGridResponse(PrintWriter aPW, DataGrid aGrid, int aStartRow, int anEndRow)
		throws IOException
	{
		if (aPW == null)
			throw new IOException("PrintWriter output stream is null.");

		aPW.printf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n");
		aPW.printf("<response>%n");
		aPW.printf("<status>0</status>%n");
		aPW.printf("<startRow>%d</startRow>%n", aStartRow);
		aPW.printf("<endRow>%d</endRow>%n", anEndRow);

		int rowCount = aGrid.rowCount();
		aPW.printf("<totalRows>%d</totalRows>%n", rowCount);
		aPW.printf("<data>%n");
		for (int row = 0; row < rowCount; row++)
			saveRecord(aPW, aGrid.getRowAsDoc(row));
		aPW.printf("</data>%n");

		aPW.printf("</response>%n");
	}

// http://www.smartclient.com/releases/SmartGWT_Quick_Start_Guide.pdf (Data Sources Page 22)

	private String dsTypeToSCString(DataItem anItem)
	{
		switch (anItem.getType())
		{
			case Integer:
			case Long:
				if (anItem.isFeatureEqual(DS.FEATURE_SEQUENCE_MANAGEMENT, DS.SQL_INDEX_MANAGEMENT_IMPLICIT))
					return "sequence";
				else
					return "integer";
			case Float:
			case Double:
				return "float";
			case Boolean:
				return "boolean";
			case Date:
				return "date";
			case DateTime:
				return "datetime";
			default:
				return "text";
		}
	}

	private Data.Type dsSCStringToType(String aTypeString)
	{
		Data.Type fieldType = Data.Type.Text;

		if (StringUtils.equalsIgnoreCase(aTypeString, "sequence"))
			fieldType = Data.Type.Integer;
		else if (StringUtils.equalsIgnoreCase(aTypeString, "integer"))
			fieldType = Data.Type.Integer;
		else if (StringUtils.equalsIgnoreCase(aTypeString, "float"))
			fieldType = Data.Type.Double;
		else if (StringUtils.equalsIgnoreCase(aTypeString, "boolean"))
			fieldType = Data.Type.Boolean;
		else if ((StringUtils.equalsIgnoreCase(aTypeString, "date")) || (StringUtils.equalsIgnoreCase(aTypeString, "datetime")))
			fieldType = Data.Type.DateTime;

		return fieldType;
	}

	public void saveRange(PrintWriter aPW, int anIndentAmount, DataRange aRange)
		throws IOException
	{
		if (aRange.getType() == Data.Type.Text)
		{
			IOXML.indentLine(aPW, anIndentAmount);
			aPW.printf("<valueMap>%n");
			ArrayList<String> rangeItems = aRange.getItems();
			for (String rangeItem : rangeItems)
				IOXML.writeNodeNameValue(aPW, anIndentAmount + 1, "value", rangeItem);
			IOXML.indentLine(aPW, anIndentAmount);
			aPW.printf("</valueMap>%n");
		}
	}

	private boolean isSpecialItem(DataItem aDataItem)
	{
		return aDataItem.getName().equals(Data.GRAPH_EDGE_DIRECTION_NAME);
	}

	private void writeSpecialItem(PrintWriter aPW, DataItem aDataItem)
	{
		if (aDataItem.getName().equals(Data.GRAPH_EDGE_DIRECTION_NAME))
		{
			if (aDataItem.isFeatureTrue(Data.FEATURE_IS_HIDDEN))
				aPW.printf("<field name=\"%s\" title=\"Direction\" type=\"text\" detail=\"true\" hidden=\"true\" />%n", Data.GRAPH_EDGE_DIRECTION_NAME);
			else
				aPW.printf("<field name=\"%s\" title=\"Direction\" type=\"image\" imageURLPrefix=\"direction/\" imageURLSuffix=\".png\" />%n", Data.GRAPH_EDGE_DIRECTION_NAME);
		}
	}

	public void save(PrintWriter aPW, int anIndentAmount, DataDoc aDataDoc)
		throws IOException
	{
		int fieldCount = aDataDoc.count();
		if (fieldCount > 0)
		{
			IOXML.indentLine(aPW, anIndentAmount);
			aPW.printf("<fields>%n");
			for (DataItem dataItem : aDataDoc.getItems())
			{
				IOXML.indentLine(aPW, anIndentAmount+1);
				if (isSpecialItem(dataItem))
					writeSpecialItem(aPW, dataItem);
				else
				{
					aPW.printf("<field");
					IOXML.writeAttrNameValue(aPW, "name", dataItem.getName());
					IOXML.writeAttrNameValue(aPW, "title", dataItem.getTitle());
					IOXML.writeAttrNameValue(aPW, "type", dsTypeToSCString(dataItem));
					if (dataItem.isFeatureTrue(Data.FEATURE_IS_REQUIRED))
						IOXML.writeAttrNameValue(aPW, "required", StrUtl.STRING_TRUE);
					if (dataItem.isFeatureTrue(Data.FEATURE_IS_HIDDEN))
						IOXML.writeAttrNameValue(aPW, "hidden", true);
					if (dataItem.isFeatureFalse(Data.FEATURE_IS_VISIBLE))
						IOXML.writeAttrNameValue(aPW, "detail", true);
					if (dataItem.isFeatureAssigned(Data.FEATURE_IS_EDITABLE))
						IOXML.writeAttrNameValue(aPW, "canEdit", dataItem.isFeatureTrue(Data.FEATURE_IS_EDITABLE));
					if (dataItem.isFeatureAssigned(DS.FEATURE_STORED_SIZE))
						IOXML.writeAttrNameValue(aPW, "length", dataItem.getFeatureAsInt(DS.FEATURE_STORED_SIZE));
					if (dataItem.isFeatureTrue(Data.FEATURE_IS_PRIMARY))
						IOXML.writeAttrNameValue(aPW, "primaryKey", StrUtl.STRING_TRUE);
					if (dataItem.isFeatureAssigned(Data.FEATURE_UI_FORMAT))
						IOXML.writeAttrNameValue(aPW, "format", dataItem.getFeature(Data.FEATURE_UI_FORMAT));
					else if (StringUtils.isNotEmpty(dataItem.getUIFormat()))
						IOXML.writeAttrNameValue(aPW, "format", dataItem.getUIFormat());
					if (dataItem.isFeatureAssigned(Data.FEATURE_UI_WIDTH))
						IOXML.writeAttrNameValue(aPW, "width", dataItem.getFeature(Data.FEATURE_UI_WIDTH));
					if ((dataItem.isFeatureAssigned(Data.FEATURE_UI_EDITOR)) && (! dataItem.isRangeAssigned()))
						IOXML.writeAttrNameValue(aPW, "editorType", dataItem.getFeature(Data.FEATURE_UI_EDITOR));
					if (dataItem.isFeatureTrue(Data.FEATURE_UI_HOVER))
						IOXML.writeAttrNameValue(aPW, "showHover", StrUtl.STRING_TRUE);
					if (dataItem.isRangeAssigned())
					{
						IOXML.writeAttrNameValue(aPW, "editorType", "SelectItem");
						aPW.printf(">%n");
						saveRange(aPW, anIndentAmount+2, dataItem.getRange());
					}
					else
						aPW.printf("/>%n");

					if (dataItem.isRangeAssigned())
					{
						IOXML.indentLine(aPW, anIndentAmount+1);
						aPW.printf("</field>%n");
					}
				}
			}
			IOXML.indentLine(aPW, anIndentAmount);
			aPW.printf("</fields>%n");
		}
	}

	public void save(String aPathFileName, DataDoc aDataDoc)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			save(printWriter, 0, aDataDoc);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}
}
