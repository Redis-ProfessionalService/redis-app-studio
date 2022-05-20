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
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintWriter;
import java.util.Optional;

/**
 * The DataGridConsole class provides a convenient way for an application
 * to generate a presentation for the console.  The presentation of the
 * columns are aligned based on display widths.
 * <p>
 * <b>Note:</b> The width of the column will be derived from the data
 * item display width or the item name length or the data value length.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataGridConsole
{
	private final String FEATURE_GRID_DISPLAY_WIDTH = "display_width";

	private boolean mIsFormatted;

	/**
	 * Default constructor.
	 */
	public DataGridConsole()
	{
	}

	/**
	 * Assign the value formatting flag.  If <i>true</i>, then numbers
	 * and dates will be generated based on the format mask.
	 *
	 * @param aIsFormatted True or false
	 */
	public void setFormattedFlag(boolean aIsFormatted)
	{
		mIsFormatted = aIsFormatted;
	}

	private int deriveItemWidth(DataItem aDataItem, int aMaxWidth)
	{
		int nameLength, titleLength, valueLength;

		int displayWidth = aDataItem.getDisplaySize();
		if (displayWidth == 0)
		{
			nameLength = aDataItem.getName().length();
			titleLength = aDataItem.getTitle().length();
			if (aDataItem.isMultiValue())
				valueLength = aDataItem.getValuesCollapsed().length();
			else if (mIsFormatted)
				valueLength = aDataItem.getValueFormatted().length();
			else
				valueLength = aDataItem.getValue().length();
			displayWidth = nameLength;
			displayWidth = Math.max(titleLength, displayWidth);
			displayWidth = Math.max(valueLength, displayWidth);
		}

		if (aMaxWidth == 0)
			return displayWidth;
		else
			return Math.min(displayWidth, aMaxWidth);
	}

	private int deriveRowColumnWidth(DataGrid aDataGrid, DataItem aDataItem, int aMaxWidth)
	{
		DataDoc dataDoc;
		DataItem dataItem;
		Optional<DataDoc> optDataDoc;
		Optional<DataItem> optDataItem;

		int rowCount = aDataGrid.rowCount();
		int maxColDisplayWidth = deriveItemWidth(aDataItem, aMaxWidth);
		for (int row = 0; row < rowCount; row++)
		{
			optDataDoc = aDataGrid.getRowAsDocOptional(row);
			if (optDataDoc.isPresent())
			{
				dataDoc = optDataDoc.get();
				optDataItem = dataDoc.getItemByNameOptional(aDataItem.getName());
				if (optDataItem.isPresent())
				{
					dataItem = optDataItem.get();
					maxColDisplayWidth = Math.max(maxColDisplayWidth, deriveItemWidth(dataItem, aMaxWidth));
				}
			}
		}

		return maxColDisplayWidth;
	}

	/**
	 * Writes the contents of the data grid to the console.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aPW Printer writer instance.
	 * @param aTitle An optional presentation title.
	 * @param aMaxWidth Maximum width of any column (zero means unlimited).
	 * @param aColSpace Number of spaces between columns.
	 */
	public void write(DataGrid aDataGrid, PrintWriter aPW, String aTitle, int aMaxWidth, int aColSpace)
	{
		String rowString;
		DataDoc schemaDoc, dataDoc;
		StringBuilder rowStrBuilder;
		Optional<DataDoc> optDataDoc;
		int j, k, colCount, rowCount;
		String labelString, valueString;
		int strLength, colWidth, displayWidth, totalWidth;

// Calculate our total output width.

		totalWidth = 0;
		schemaDoc = aDataGrid.getColumns();
		colCount = schemaDoc.count();
		for (DataItem dataItem : schemaDoc.getItems())
		{
			if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
				totalWidth += deriveRowColumnWidth(aDataGrid, dataItem, aMaxWidth);
		}
		totalWidth += (aColSpace * (colCount - 1));

// Account for our title string.

		if (StringUtils.isNotEmpty(aTitle))
		{
			totalWidth = Math.max(aTitle.length(), totalWidth);
			rowString = StrUtl.centerSpaces(aTitle, totalWidth);
			aPW.printf("%n%s%n%n", rowString);
		}

// Display our column header information.

		rowStrBuilder = new StringBuilder();
		for (DataItem dataItem : schemaDoc.getItems())
		{
			if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
			{
				displayWidth = deriveRowColumnWidth(aDataGrid, dataItem, aMaxWidth);
				dataItem.addFeature(FEATURE_GRID_DISPLAY_WIDTH, displayWidth);
				labelString = dataItem.getTitle();
				strLength = labelString.length();
				colWidth = displayWidth + aColSpace;
				strLength = Math.min(displayWidth, strLength);
				rowStrBuilder.append(labelString.substring(0, strLength));
				for (k = strLength; k < colWidth; k++)
					rowStrBuilder.append(StrUtl.CHAR_SPACE);
			}
		}
		aPW.printf("%s%n", rowStrBuilder.toString());

// Underline our column headers.

		rowStrBuilder.setLength(0);
		for (DataItem dataItem : schemaDoc.getItems())
		{
			if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
			{
				displayWidth = dataItem.getFeatureAsInt(FEATURE_GRID_DISPLAY_WIDTH);
				labelString = dataItem.getTitle();
				strLength = labelString.length();
				colWidth = displayWidth + aColSpace;
				strLength = Math.min(displayWidth, strLength);
				for (j = 0; j < strLength; j++)
					rowStrBuilder.append(StrUtl.CHAR_HYPHEN);
				for (k = strLength; k < colWidth; k++)
					rowStrBuilder.append(StrUtl.CHAR_SPACE);
			}
		}
		aPW.printf("%s%n", rowStrBuilder.toString());

// Display each row of cells.

		rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			rowStrBuilder.setLength(0);
			optDataDoc = aDataGrid.getRowAsDocOptional(row);
			if (optDataDoc.isPresent())
			{
				dataDoc = optDataDoc.get();
				for (DataItem dataItem : dataDoc.getItems())
				{
					if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
					{
						displayWidth = schemaDoc.getItemByName(dataItem.getName()).getFeatureAsInt(FEATURE_GRID_DISPLAY_WIDTH);
						if (dataItem.isValueAssigned())
						{
							if (dataItem.isMultiValue())
								valueString = dataItem.getValuesCollapsed();
							else if (mIsFormatted)
								valueString = dataItem.getValueFormatted();
							else
								valueString = dataItem.getValue();
						}
						else
							valueString = StringUtils.EMPTY;

						strLength = valueString.length();
						colWidth = displayWidth + aColSpace;
						strLength = Math.min(displayWidth, strLength);
						rowStrBuilder.append(valueString.substring(0, strLength));
						for (k = strLength; k < colWidth; k++)
							rowStrBuilder.append(StrUtl.CHAR_SPACE);
					}
				}
				aPW.printf("%s%n", rowStrBuilder.toString());
			}
		}
		aPW.printf("%n");

// Clean up after ourselves.

		for (DataItem dataItem : schemaDoc.getItems())
			dataItem.disableFeature(FEATURE_GRID_DISPLAY_WIDTH);
	}

	/**
	 * Writes the contents of the data grid to the console.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aPW Print writer instance.
	 * @param aTitle An optional presentation title.
	 */
	public void write(DataGrid aDataGrid, PrintWriter aPW, String aTitle)
	{
		write(aDataGrid, aPW, aTitle, 0, 2);
	}
}
