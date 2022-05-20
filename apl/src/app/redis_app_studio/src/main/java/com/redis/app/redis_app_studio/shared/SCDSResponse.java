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

import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.io.DataGridLogger;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.*;

/**
 * Collection of utility methods focused on converting Foundation objects
 * into SmartClient response payloads.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SCDSResponse
{
	private boolean mIsDebug;
	private final AppCtx mAppCtx;
	private final DataSource mDataSource;

	/**
	 * Constructor accepting an application context and SmartClient data source.
	 *
	 * @param anAppCtx Application context instance
	 * @param aDataSource SmartClient data source instance
	 */
	public SCDSResponse(AppCtx anAppCtx, DataSource aDataSource)
	{
		mAppCtx = anAppCtx;
		mDataSource = aDataSource;
	}

	/**
	 * Assigns a debug log level
	 *
	 * @param anIsEnabled <i>true</i> or <i>false</i>
	 */
	public void setDebugFlag(boolean anIsEnabled)
	{
		mIsDebug = anIsEnabled;
	}

	/**
	 * Calculates the SmartClient auxiliary grid response row start.
	 *
	 * @param aFetchStart Fetch start offset
	 *
	 * @return Calculated row start
	 */
	public int auxGridRowStart(int aFetchStart)
	{
		return aFetchStart;
	}

	/**
	 * Calculates the SmartClient main grid response row start.
	 *
	 * @param aFetchStart Fetch start offset
	 *
	 * @return Calculated row start
	 */
	public int mainGridRowStart(int aFetchStart)
	{
		return aFetchStart;
	}

	/**
	 * Calculates the SmartClient auxiliary grid response row total value.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aFetchRowLimit Fetch row limit from criteria
	 *
	 * @return Calculated row total
	 */
	public int auxGridRowTotal(DataGrid aDataGrid, int aFetchRowLimit)
	{
		int responseRowTotal;

		int rowCount = aDataGrid.rowCount();
		if (rowCount == 0)
			return 0;
		else if (aDataGrid.isFeatureAssigned(DS.FEATURE_TOTAL_DOCUMENTS))
		{
			responseRowTotal = aDataGrid.getFeatureAsInt(DS.FEATURE_TOTAL_DOCUMENTS);
			if (responseRowTotal <= aFetchRowLimit)
				responseRowTotal = rowCount;
		}
		else
			responseRowTotal = Math.max(0, rowCount);

		return responseRowTotal;
	}

	/**
	 * Calculates the SmartClient main grid response row total value.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aFetchRowLimit Fetch row limit from criteria
	 *
	 * @return Calculated row total
	 */
	public int mainGridRowTotal(DataGrid aDataGrid, int aFetchRowLimit)
	{
		int responseRowTotal;

		int rowCount = aDataGrid.rowCount();
		if (rowCount == 0)
			return 0;
		else if (aDataGrid.isFeatureAssigned(DS.FEATURE_TOTAL_DOCUMENTS))
		{
			responseRowTotal = aDataGrid.getFeatureAsInt(DS.FEATURE_TOTAL_DOCUMENTS);
			if (responseRowTotal <= aFetchRowLimit)
				responseRowTotal = rowCount;
		}
		else
			responseRowTotal = Math.max(0, rowCount);

		return responseRowTotal + 1;
	}

	/**
	 * Calculates the SmartClient auxiliary grid response row total value.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aFetchStart Fetch row start from criteria
	 * @param aRowTotal Calculated row total
	 *
	 * @return Calculated row finish
	 */
	public int auxGridRowFinish(DataGrid aDataGrid, int aFetchStart, int aRowTotal)
	{
		if (aRowTotal == 0)
			return 0;
		else
			return Math.min(aFetchStart + aDataGrid.rowCount(), aRowTotal);
	}

	/**
	 * Calculates the SmartClient main grid response row total value.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aFetchStart Fetch row start from criteria
	 * @param aRowTotal Calculated row total
	 *
	 * @return Calculated row finish
	 */
	public int mainGridRowFinish(DataGrid aDataGrid, int aFetchStart, int aRowTotal)
	{
		if (aRowTotal == 0)
			return 0;
		else
		{
			int rowFinish = Math.min(aFetchStart + aDataGrid.rowCount(), aRowTotal);
			if (rowFinish < aRowTotal)
				rowFinish++;
			return rowFinish;
		}
	}

	/**
	 * Creates a SmartClient response payload object.
	 *
	 * @param aMap Map of names/values
	 * @param anIsOK Operation execution status (true/false)
	 *
	 * @return SmartClient data response instance
	 */
	@SuppressWarnings("WhileLoopReplaceableByForEach")
	public DSResponse create(Map aMap, boolean anIsOK)
	{
		Logger appLogger = mAppCtx.getLogger(this, "create(Map)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = new DSResponse(mDataSource);
		if (anIsOK)
			dsResponse.setSuccess();
		else
			dsResponse.setFailure();
		dsResponse.setData(aMap);

		if (mIsDebug)
		{
			int offset = 0;
			Iterator mapIterator = aMap.entrySet().iterator();
			while (mapIterator.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) mapIterator.next();
				appLogger.debug("[" + offset + "]" + ": " + mapEntry.getKey() + " = " + mapEntry.getValue());
				offset++;
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Creates a SmartClient response payload object.
	 *
	 * @param aDataDoc Data document instance
	 * @param anIsOK Operation execution status (true/false)
	 *
	 * @return SmartClient data response instance
	 */
	public DSResponse create(DataDoc aDataDoc, boolean anIsOK)
	{
		Logger appLogger = mAppCtx.getLogger(this, "create(DataDoc)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = new DSResponse(mDataSource);
		if (anIsOK)
			dsResponse.setSuccess();
		else
			dsResponse.setFailure();

		HashMap rowMap = new HashMap();
		for (DataItem dataItem : aDataDoc.getItems())
		{
			if (dataItem.isValueAssigned())
				rowMap.put(dataItem.getName(), dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
			else
				rowMap.put(dataItem.getName(), StringUtils.EMPTY);
		}

		dsResponse.setData(rowMap);

		if (mIsDebug)
		{
			int offset = 0;
			Iterator mapIterator = rowMap.entrySet().iterator();
			while (mapIterator.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) mapIterator.next();
				appLogger.debug("[" + offset + "]" + ": " + mapEntry.getKey() + " = " + mapEntry.getValue());
				offset++;
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Creates a SmartClient response payload object.
	 *
	 * @param aDataDoc Data document instance
	 * @param anAppResource Application resource instance
	 * @param anIsOK Operation execution status (true/false)
	 *
	 * @return SmartClient data response instance
	 */
	public DSResponse create(DataDoc aDataDoc, AppResource anAppResource, boolean anIsOK)
	{
		String rasContext;
		Logger appLogger = mAppCtx.getLogger(this, "create(DataDoc, AppResource)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = new DSResponse(mDataSource);
		if (anIsOK)
			dsResponse.setSuccess();
		else
			dsResponse.setFailure();

		if (anAppResource == null)
			rasContext = StringUtils.EMPTY;
		else
			rasContext = String.format("%s|%s|%s", anAppResource.getPrefix(), anAppResource.getStructure(), anAppResource.getTitle());

		HashMap rowMap = new HashMap();
		for (DataItem dataItem : aDataDoc.getItems())
		{
			if (dataItem.isValueAssigned())
				rowMap.put(dataItem.getName(), dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
			else
				rowMap.put(dataItem.getName(), StringUtils.EMPTY);
		}
		if ((StringUtils.isNotEmpty(rasContext)) && (rowMap.size() > 0))
			rowMap.put(Constants.RAS_CONTEXT_FIELD_NAME, rasContext);

		dsResponse.setData(rowMap);

		if (mIsDebug)
		{
			int offset = 0;
			Iterator mapIterator = rowMap.entrySet().iterator();
			while (mapIterator.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) mapIterator.next();
				appLogger.debug("[" + offset + "]" + ": " + mapEntry.getKey() + " = " + mapEntry.getValue());
				offset++;
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Creates a SmartClient response payload object.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aSuggestItem Suggest data item instance
	 * @param aReplyItemName Reply item name
	 *
	 * @return SmartClient data response instance
	 */
	public DSResponse create(DataGrid aDataGrid, DataItem aSuggestItem, String aReplyItemName)
	{
		HashMap rowMap;
		DataItem dataItem;
		Optional<DataItem> optDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "create(DataGrid for DataItem)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();
		DSResponse dsResponse = new DSResponse(mDataSource);
		dsResponse.setSuccess();
		dsResponse.setStartRow(0);
		dsResponse.setEndRow(rowCount);
		dsResponse.setTotalRows(rowCount);

		ArrayList replyList = new ArrayList();
		if (mIsDebug)
		{
			appLogger.debug(String.format("[%s] '%s' column and %d rows", aDataGrid.getName(), aSuggestItem.getName(), rowCount));
			appLogger.debug(String.format("'%s': startRow = %d, endRow = %d, totalRows = %d", aDataGrid.getName(),
										  0, rowCount, rowCount));
		}
		if ((rowCount > 0) && (colCount > 0))
		{
			for (int row = 0; row < rowCount; row++)
			{
				rowMap = new HashMap();
				optDataItem = aDataGrid.getItemByRowNameOptional(row, aSuggestItem.getName());
				if (optDataItem.isPresent())
				{
					dataItem = optDataItem.get();
					if (dataItem.isValueAssigned())
						rowMap.put(aReplyItemName, dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
					else
						rowMap.put(aReplyItemName, StringUtils.EMPTY);
					replyList.add(rowMap);
				}
			}
			if (mIsDebug)
			{
				DataGridLogger dataGridLogger = new DataGridLogger(appLogger);
				dataGridLogger.writeSimple(aDataGrid);
			}
		}
		dsResponse.setData(replyList);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Creates a SmartClient data source response instance based on the data
	 * grid and row information.  In addition, if the application resource
	 * is non-null, then it will be used to assign an App Studio context
	 * column for each row in the data source response.
	 *
	 * @param aDataGrid Data grid instance
	 * @param anAppResource Application resource instance (can be null)
	 * @param aStartRow Starting row offset
	 * @param anEndRow Ending row offset
	 * @param aTotalRows Total rows
	 *
	 * @return SmartClient data source instance
	 */
	public DSResponse create(DataGrid aDataGrid, AppResource anAppResource,
							 int aStartRow, int anEndRow, int aTotalRows)
	{
		HashMap rowMap;
		DataDoc dataDoc;
		String rasContext;
		Logger appLogger = mAppCtx.getLogger(this, "create(DataGrid,AppResource)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = new DSResponse(mDataSource);
		dsResponse.setSuccess();
		dsResponse.setStartRow(aStartRow);
		dsResponse.setEndRow(anEndRow);
		dsResponse.setTotalRows(aTotalRows);

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();
		ArrayList replyList = new ArrayList();
		if (mIsDebug)
		{
			appLogger.debug(String.format("[%s] %d columns and %d rows", aDataGrid.getName(), colCount, rowCount));
			appLogger.debug(String.format("'%s': startRow = %d, endRow = %d, totalRows = %d", aDataGrid.getName(),
										  aStartRow, anEndRow, aTotalRows));
		}
		if (anAppResource == null)
			rasContext = StringUtils.EMPTY;
		else
			rasContext = String.format("%s|%s|%s", anAppResource.getPrefix(), anAppResource.getStructure(), anAppResource.getTitle());
		if ((rowCount > 0) && (colCount > 0))
		{
			for (int row = 0; row < rowCount; row++)
			{
				rowMap = new HashMap();
				dataDoc = aDataGrid.getRowAsDoc(row);
				for (DataItem dataItem : dataDoc.getItems())
				{
					if (dataItem.isValueAssigned())
						rowMap.put(dataItem.getName(), dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
					else
						rowMap.put(dataItem.getName(), StringUtils.EMPTY);
				}
				if ((StringUtils.isNotEmpty(rasContext)) && (rowMap.size() > 0))
					rowMap.put(Constants.RAS_CONTEXT_FIELD_NAME, rasContext);
				replyList.add(rowMap);
			}
			if (mIsDebug)
			{
				DataGridLogger dataGridLogger = new DataGridLogger(appLogger);
				dataGridLogger.setFormattedFlag(true);
				dataGridLogger.writeSimple(aDataGrid);
			}
		}
		dsResponse.setData(replyList);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Creates a SmartClient data source response instance based on the data
	 * grid and row information.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aStartRow Starting row offset
	 * @param anEndRow Ending row offset
	 * @param aTotalRows Total rows
	 *
	 * @return SmartClient data source instance
	 */
	public DSResponse create(DataGrid aDataGrid, int aStartRow, int anEndRow, int aTotalRows)
	{
		Logger appLogger = mAppCtx.getLogger(this, "create(DataGrid)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = create(aDataGrid, null, aStartRow, anEndRow, aTotalRows);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Generate a SmartClient response instance that enables cell highlighting
	 * when RedisSearch highlighting tokens are recognized.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aStartRow Starting row offset
	 * @param anEndRow Ending row offset
	 * @param aTotalRows Total rows
	 *
	 * @return SmartClient data source instance
	 */
	public DSResponse createWithHighlights(DataGrid aDataGrid, int aStartRow, int anEndRow, int aTotalRows)
	{
		HashMap rowMap;
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "createWithHighlights(DataGrid with Highlights)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = new DSResponse(mDataSource);
		dsResponse.setSuccess();
		dsResponse.setStartRow(aStartRow);
		dsResponse.setEndRow(anEndRow);
		dsResponse.setTotalRows(aTotalRows);

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();
		ArrayList replyList = new ArrayList();
		if (mIsDebug)
		{
			appLogger.debug(String.format("[%s] %d columns and %d rows", aDataGrid.getName(), colCount, rowCount));
			appLogger.debug(String.format("'%s': startRow = %d, endRow = %d, totalRows = %d", aDataGrid.getName(),
										  aStartRow, anEndRow, aTotalRows));
		}
		if ((rowCount > 0) && (colCount > 0))
		{
			String dataValue;
			String hlOpenTag = Redis.HIGHLIGHT_TAG_OPEN;
			String hlCloseTag = Redis.HIGHLIGHT_TAG_CLOSE;

			for (int row = 0; row < rowCount; row++)
			{
				int colOffset = 0;
				rowMap = new HashMap();
				dataDoc = aDataGrid.getRowAsDoc(row);
				for (DataItem dataItem : dataDoc.getItems())
				{
					dataValue = dataItem.getValue();
					if ((StringUtils.contains(dataValue, hlOpenTag)) && (StringUtils.contains(dataValue, hlCloseTag)))
					{
						dataValue = StringUtils.remove(dataValue, hlOpenTag);
						dataValue = StringUtils.remove(dataValue, hlCloseTag);
						dataItem.setValue(dataValue);
						rowMap.put("_hilite", colOffset);
					}
					rowMap.put(dataItem.getName(), dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
					colOffset++;
				}
				replyList.add(rowMap);
			}
			if (mIsDebug)
			{
				DataGridLogger dataGridLogger = new DataGridLogger(appLogger);
				dataGridLogger.writeSimple(aDataGrid);
			}
		}
		dsResponse.setData(replyList);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	private boolean isAlphaNumeric(char aCh)
	{
		return ((aCh >= 'A' && aCh <= 'Z') || (aCh >= 'a' && aCh <= 'z') || (aCh >= '0' && aCh <= '9'));
	}

	/**
	 * Generate a SmartClient response instance that enables cell highlighting
	 * when RedisSearch highlighting tokens are recognized.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aSearchTerms Search terms
	 * @param aSchemaDoc Schema data document instance
	 * @param aStartRow Starting row offset
	 * @param anEndRow Ending row offset
	 * @param aTotalRows Total rows
	 *
	 * @return SmartClient data source instance
	 */
	public DSResponse createWithHighlights(DataGrid aDataGrid, String aSearchTerms, DataDoc aSchemaDoc,
										   int aStartRow, int anEndRow, int aTotalRows)
	{
		HashMap rowMap;
		DataDoc dataDoc;
		String[] termArray;
		DataItem schemaItem;
		Logger appLogger = mAppCtx.getLogger(this, "createWithHighlights(DataGrid with Highlights - Search Terms)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSResponse dsResponse = new DSResponse(mDataSource);
		dsResponse.setSuccess();
		dsResponse.setStartRow(aStartRow);
		dsResponse.setEndRow(anEndRow);
		dsResponse.setTotalRows(aTotalRows);

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();
		ArrayList replyList = new ArrayList();
		if (mIsDebug)
		{
			appLogger.debug(String.format("[%s] %d columns and %d rows", aDataGrid.getName(), colCount, rowCount));
			appLogger.debug(String.format("'%s': startRow = %d, endRow = %d, totalRows = %d", aDataGrid.getName(),
										  aStartRow, anEndRow, aTotalRows));
		}

		termArray = aSearchTerms.split(" ");
		if ((rowCount > 0) && (colCount > 0))
		{
			String dataValue;
			int termOffset, termLength, termEndOffset, dataValueLength;

			int highlightCount = 0;
			for (int row = 0; row < rowCount; row++)
			{
				int colOffset = 0;
				rowMap = new HashMap();
				dataDoc = aDataGrid.getRowAsDoc(row);
				for (DataItem dataItem : dataDoc.getItems())
				{
					schemaItem = aSchemaDoc.getItemByName(dataItem.getName());
					if ((schemaItem != null) && (schemaItem.isFeatureTrue(Redis.FEATURE_IS_HIGHLIGHTED)))
					{
						dataValue = dataItem.getValue();
						if (StringUtils.isNotEmpty(dataValue))
						{
							dataValueLength = dataValue.length();
							for (String searchTerm : termArray)
							{
								termOffset = dataValue.toLowerCase().indexOf(searchTerm);
								if (termOffset >= 0)
								{
									termLength = searchTerm.length();
									if (dataValueLength > termLength)
									{
										termEndOffset = Math.min(dataValueLength-1, termOffset+termLength);
										if ((termEndOffset == dataValueLength-1) || (! isAlphaNumeric(dataValue.charAt(termEndOffset))))
										{
											highlightCount++;
											rowMap.put("_hilite", colOffset);
											break;
										}
									}
									else
									{
										highlightCount++;
										rowMap.put("_hilite", colOffset);
										break;
									}
								}
							}
						}
					}
					rowMap.put(dataItem.getName(), dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
					colOffset++;
				}
				replyList.add(rowMap);
			}
			if (mIsDebug)
			{
				DataGridLogger dataGridLogger = new DataGridLogger(appLogger);
				dataGridLogger.writeSimple(aDataGrid);
			}
			appLogger.debug(String.format("%s: %d cells were highlighted in the data grid.", aSearchTerms, highlightCount));
		}
		dsResponse.setData(replyList);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Creates a SmartClient response payload object.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return SmartClient data response instance
	 */
	public DSResponse create(DataGrid aDataGrid)
	{
		Logger appLogger = mAppCtx.getLogger(this, "create(DataGrid)");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int rowCount = aDataGrid.rowCount();
		DSResponse dsResponse = create(aDataGrid, 0, rowCount, rowCount);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
