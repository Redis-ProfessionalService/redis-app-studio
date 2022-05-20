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

package com.redis.app.redis_app_studio.shared;

import com.isomorphic.log.Logger;
import com.isomorphic.datasource.DSRequest;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterion;
import com.redis.foundation.ds.DSCriterionEntry;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The SmartClient logger class leverages the SmartClient logger stream
 * for generating log messages related to data items, docs, grids and
 * DSCriteria.
 */
public class SCLogger
{
	private Logger mLogger;
	private String mPrefix = StringUtils.EMPTY;

	public SCLogger() { mLogger = new Logger(DSRequest.class.getName()); }

	public SCLogger(String aName)
	{
		mLogger = new Logger(aName);
	}

	public void trace(String aMethodName, String aMessage)
	{
		mLogger.debug(String.format("[T] (%s): %s", aMethodName, aMessage));
	}

	public void debug(String aMessage)
	{
		mLogger.debug(aMessage);
	}

	public void info(String aMessage)
	{
		mLogger.info(aMessage);
	}

	public void warn(String aMessage)
	{
		mLogger.warn(aMessage);
	}

	public void error(String aMessage)
	{
		mLogger.error(aMessage);
	}

// DataItemLogger.java

	public void writeNV(String aName, String aValue)
	{
		if (StringUtils.isNotEmpty(aValue))
			mLogger.debug(aName + ": " + aValue);
	}

	public void writeNV(String aName, int aValue)
	{
		if (aValue > 0)
			writeNV(aName, Integer.toString(aValue));
	}

	public void writeNV(String aName, long aValue)
	{
		if (aValue > 0)
			writeNV(aName, Long.toString(aValue));
	}

	public void writeFull(DataItem aDataItem)
	{
		if (aDataItem != null)
		{
			writeNV("Name", aDataItem.getName());
			writeNV("Type", Data.typeToString(aDataItem.getType()));
			writeNV("Title", aDataItem.getTitle());
			writeNV("Value", aDataItem.getValuesCollapsed());
			if (aDataItem.getSortOrder() != Data.Order.UNDEFINED)
				writeNV("Sort Order", aDataItem.getSortOrder().name());
			writeNV("Display Size", aDataItem.getDisplaySize());
			writeNV("Default Value", aDataItem.getDefaultValue());

			String nameString;
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aDataItem.getFeatures().entrySet())
			{
				nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
				writeNV(nameString, featureEntry.getValue());
			}
		}
	}

	public void writeSimple(DataItem aDataItem)
	{
		if (aDataItem != null)
			mLogger.debug(aDataItem.toString());
	}

	public void write(DataItem aDataItem)
	{
		writeFull(aDataItem);
	}

// PropertyLogger.java

	public void writeSimple(HashMap<String, Object> aProperties)
	{
		if (aProperties != null)
		{
			for (Map.Entry<String, Object> propertyEntry : aProperties.entrySet())
				writeNV(propertyEntry.getKey(), propertyEntry.getValue().toString());
		}
	}

	public void writeFull(HashMap<String, Object> aProperties)
	{
		writeSimple(aProperties);
	}

	public void write(HashMap<String, Object> aProperties)
	{
		writeSimple(aProperties);
	}

// DataDocLogger.java

	public void writeFull(DataDoc aDataDoc)
	{
		if (aDataDoc != null)
		{
			writeNV("Name", aDataDoc.getName());
			writeNV("Title", aDataDoc.getTitle());

			String nameString;
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aDataDoc.getFeatures().entrySet())
			{
				nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
				writeNV(nameString, featureEntry.getValue());
			}
			for (DataItem dataItem : aDataDoc.getItems())
				writeSimple(dataItem);
			writeFull(aDataDoc.getProperties());
		}
	}

	public void writeSimple(DataDoc aDataDoc)
	{
		if (aDataDoc != null)
		{
			writeNV("Name", aDataDoc.getName());
			writeNV("Title", aDataDoc.getTitle());
			for (DataItem dataItem : aDataDoc.getItems())
				writeSimple(dataItem);
		}
	}

	public void write(DataDoc aDataDoc)
	{
		writeFull(aDataDoc);
	}

// DataGridLogger.java

	public void writeCommon(DataGrid aDataGrid)
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
			writeNV("Name", aDataGrid.getName());

			String nameString;
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aDataGrid.getFeatures().entrySet())
			{
				nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
				writeNV(nameString, featureEntry.getValue());
			}
			writeCommon(aDataGrid);
			writeFull(aDataGrid.getProperties());
		}
	}

	public void writeSimple(DataGrid aTable)
	{
		writeCommon(aTable);
	}

	public void write(DataGrid aTable)
	{
		writeFull(aTable);
	}

// DSCriteriaLogger.java

	public void setPrefix(String aPrefix)
	{
		if (aPrefix != null)
			mPrefix = String.format("[%s] ", aPrefix);
	}

	public void writeFull(DSCriteria aCriteria)
	{
		if (aCriteria != null)
		{
			int ceIndex = 1;
			DataItem dataItem;
			DSCriterion dsCriterion;
			String nameString, logString;

			writeNV(mPrefix + "Name", aCriteria.getName());
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aCriteria.getFeatures().entrySet())
			{
				nameString = String.format("%s F(%02d) %s", mPrefix, featureOffset++, featureEntry.getKey());
				writeNV(nameString, featureEntry.getValue());
			}
			ArrayList<DSCriterionEntry> dsCriterionEntries = aCriteria.getCriterionEntries();
			int ceCount = dsCriterionEntries.size();
			if (ceCount > 0)
			{
				for (DSCriterionEntry ce : dsCriterionEntries)
				{
					dsCriterion = ce.getCriterion();

					dataItem = dsCriterion.getItem();
					logString = String.format("%s(%d/%d) %s %s %s", mPrefix, ceIndex++,
											  ceCount, dataItem.getName(),
											  Data.operatorToString(ce.getLogicalOperator()),
											  dataItem.getValuesCollapsed());
					mLogger.debug(logString);
				}
			}
			writeFull(aCriteria.getProperties());
		}
	}

	public void writeSimple(DSCriteria aCriteria)
	{
		if (aCriteria != null)
		{
			int ceIndex = 1;
			String logString;
			DataItem dataItem;
			DSCriterion dsCriterion;

			writeNV(mPrefix + "Name", aCriteria.getName());
			ArrayList<DSCriterionEntry> dsCriterionEntries = aCriteria.getCriterionEntries();
			int ceCount = dsCriterionEntries.size();
			if (ceCount > 0)
			{
				for (DSCriterionEntry ce : dsCriterionEntries)
				{
					dsCriterion = ce.getCriterion();

					dataItem = dsCriterion.getItem();
					logString = String.format("%s(%d/%d) %s %s %s", mPrefix, ceIndex++,
											  ceCount, dataItem.getName(),
											  Data.operatorToString(ce.getLogicalOperator()),
											  dataItem.getValuesCollapsed());
					mLogger.debug(logString);
				}
			}
		}
	}

	public void write(DSCriteria aCriteria)
	{
		writeFull(aCriteria);
	}
}
