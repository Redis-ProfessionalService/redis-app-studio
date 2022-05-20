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
import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * This class offers convenience methods for logging data
 * grid information.
 */
public class DataGridLogger
{
	private final String FEATURE_GRID_DISPLAY_WIDTH = "display_width";

	private int mColumnSpace;
	private int mMaxColumnWidth;
	private boolean mIsFormatted;
	private final Logger mLogger;
	private DataItemLogger mDataItemLogger;

	/**
	 * Constructor that accepts a logger instance.
	 *
	 * @param aLogger Logger instance
	 */
	public DataGridLogger(Logger aLogger)
	{
		mLogger = aLogger;
		setColumnSpace(1);
		setMaxColumnWidth(40);
		mDataItemLogger = new DataItemLogger(aLogger);
	}

	/**
	 * Constructor that accepts logger, max column width and column space
	 * configuration parameters.
	 *
	 * @param aLogger Logger instance
	 * @param aMaxWidth Maximum column width
	 * @param aColSpace Column space
	 */
	public DataGridLogger(Logger aLogger, int aMaxWidth, int aColSpace)
	{
		mLogger = aLogger;
		setColumnSpace(aColSpace);
		setMaxColumnWidth(aMaxWidth);
		mDataItemLogger = new DataItemLogger(aLogger);
	}

	public void setColumnSpace(int aColumnSpace)
	{
		mColumnSpace = aColumnSpace;
	}

	public void setMaxColumnWidth(int aMaxColumnWidth)
	{
		mMaxColumnWidth = aMaxColumnWidth;
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

	public void writeCommon(DataGrid aDataGrid)
	{
		DataDoc schemaDoc, dataDoc;
		StringBuilder rowStrBuilder;
		Optional<DataDoc> optDataDoc;
		int j, k, colCount, rowCount;
		String labelString, valueString;
		int strLength, colWidth, displayWidth;

// Display our column header information.

		schemaDoc = aDataGrid.getColumns();
		rowStrBuilder = new StringBuilder();
		for (DataItem dataItem : schemaDoc.getItems())
		{
			if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
			{
				displayWidth = deriveRowColumnWidth(aDataGrid, dataItem, mMaxColumnWidth);
				dataItem.addFeature(FEATURE_GRID_DISPLAY_WIDTH, displayWidth);
				labelString = dataItem.getTitle();
				strLength = labelString.length();
				colWidth = displayWidth + mColumnSpace;
				strLength = Math.min(displayWidth, strLength);
				rowStrBuilder.append(labelString.substring(0, strLength));
				for (k = strLength; k < colWidth; k++)
					rowStrBuilder.append(StrUtl.CHAR_SPACE);
			}
		}
		mLogger.debug(rowStrBuilder.toString());

// Underline our column headers.

		rowStrBuilder.setLength(0);
		for (DataItem dataItem : schemaDoc.getItems())
		{
			if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
			{
				displayWidth = dataItem.getFeatureAsInt(FEATURE_GRID_DISPLAY_WIDTH);
				labelString = dataItem.getTitle();
				strLength = labelString.length();
				colWidth = displayWidth + mColumnSpace;
				strLength = Math.min(displayWidth, strLength);
				for (j = 0; j < strLength; j++)
					rowStrBuilder.append(StrUtl.CHAR_HYPHEN);
				for (k = strLength; k < colWidth; k++)
					rowStrBuilder.append(StrUtl.CHAR_SPACE);
			}
		}
		mLogger.debug(rowStrBuilder.toString());

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
						colWidth = displayWidth + mColumnSpace;
						strLength = Math.min(displayWidth, strLength);
						rowStrBuilder.append(valueString.substring(0, strLength));
						for (k = strLength; k < colWidth; k++)
							rowStrBuilder.append(StrUtl.CHAR_SPACE);
					}
				}
				mLogger.debug(rowStrBuilder.toString());
			}
		}

// Clean up after ourselves.

		for (DataItem dataItem : schemaDoc.getItems())
			dataItem.disableFeature(FEATURE_GRID_DISPLAY_WIDTH);
	}

	@SuppressWarnings("StringRepeatCanBeUsed")
	public void writeCommonOLD(DataGrid aDataGrid)
	{
		if (aDataGrid != null)
		{
			DataItem dataItem;
			int itemWidth, colWidth;

// Calculate our maximum column width based on value size

			int maxColWidth = 0;
			int rowCount = aDataGrid.rowCount();
			int colCount = aDataGrid.colCount();
			if ((rowCount > 0) && (colCount > 0))
			{
				for (int row = 0; row < rowCount; row++)
				{
					for (int col = 0; col < colCount; col++)
					{
						Optional<DataItem> optDataItem = aDataGrid.getItemByRowColOptional(row, col);
						if (optDataItem.isPresent())
						{
							dataItem = optDataItem.get();
							maxColWidth = Math.max(maxColWidth, dataItem.getName().length());
							maxColWidth = Math.max(maxColWidth, dataItem.getValuesCollapsed().length());
						}
					}
				}
			}

// Item Name

			DataDoc schemaDoc = aDataGrid.getColumns();
			StringBuilder rowStrBuilder = new StringBuilder();
			for (DataItem diCol : schemaDoc.getItems())
			{
				itemWidth = diCol.getName().length();
				colWidth = Math.min(maxColWidth, itemWidth);
				rowStrBuilder.append(diCol.getName().substring(0, colWidth));
				for (int k = itemWidth; k < maxColWidth; k++)
					rowStrBuilder.append(StrUtl.CHAR_SPACE);
				rowStrBuilder.append(StrUtl.CHAR_SPACE);
			}
			mLogger.debug(rowStrBuilder.toString());

// Underline it

			rowStrBuilder.setLength(0);
			for (DataItem diCol : schemaDoc.getItems())
			{
				itemWidth = diCol.getName().length();
				colWidth = Math.min(maxColWidth, itemWidth);
				for (int j = 0; j < colWidth; j++)
					rowStrBuilder.append(StrUtl.CHAR_HYPHEN);
				for (int k = itemWidth; k < maxColWidth; k++)
					rowStrBuilder.append(StrUtl.CHAR_SPACE);
				rowStrBuilder.append(StrUtl.CHAR_SPACE);
			}
			mLogger.debug(rowStrBuilder.toString());

// Row values

			if ((rowCount > 0) && (colCount > 0))
			{
				for (int row = 0; row < rowCount; row++)
				{
					rowStrBuilder.setLength(0);
					for (int col = 0; col < colCount; col++)
					{
						Optional<DataItem> optDataItem = aDataGrid.getItemByRowColOptional(row, col);
						if (optDataItem.isPresent())
						{
							dataItem = optDataItem.get();
							itemWidth = dataItem.getValue().length();
							colWidth = Math.min(maxColWidth, itemWidth);
							rowStrBuilder.append(dataItem.getValue().substring(0, colWidth));
							for (int k = itemWidth; k < maxColWidth; k++)
								rowStrBuilder.append(StrUtl.CHAR_SPACE);
						}
					}
					mLogger.debug(rowStrBuilder.toString());
				}
			}
		}
	}

	public void writeFull(DataGrid aDataGrid)
	{
		if (aDataGrid != null)
		{
			mDataItemLogger.writeNV("Name", aDataGrid.getName());

			String nameString;
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aDataGrid.getFeatures().entrySet())
			{
				nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
				mDataItemLogger.writeNV(nameString, featureEntry.getValue());
			}
			writeCommon(aDataGrid);
			PropertyLogger propertyLogger = new PropertyLogger(mLogger);
			propertyLogger.writeFull(aDataGrid.getProperties());
		}
	}

	public void writeSimple(DataGrid aDataGrid)
	{
		writeCommon(aDataGrid);
	}

	public void write(DataGrid aDataGrid)
	{
		writeFull(aDataGrid);
	}
}
