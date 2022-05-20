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

package com.redis.app.redis_app_studio.rsh;

import com.isomorphic.datasource.BasicDataSource;
import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.*;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked", "rawtypes", "FieldCanBeLocal"})
public class AppViewGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "AppViewGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "rs";
	private final boolean REDIS_STACK_62_HIGHLIGHT_ISSUE = true;		// likely due to a field_name AS alias_field_name issue

	/**
	 * This method is responsible for transforming data formats from the SmartClient UI
	 * to the internal Redis App Studio framework.  Currently, dates are the priority,
	 * but others could emerge as testing continues.
	 *
	 * @param anAppCtx Application context
	 * @param aSchemaDoc Schema data document instance
	 * @param aDSCriteria Data source criteria instance
	 */
	private void assignDataFormats(AppCtx anAppCtx, DataDoc aSchemaDoc, DSCriteria aDSCriteria)
	{
		Date uiDate;
		DataItem ceDataItem, schemaDataItem;
		String itemName, dataFormat, uiFormat;
		Logger appLogger = anAppCtx.getLogger(this, "assignDataFormats");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			Optional<DataItem> optSchemaDataItem = aSchemaDoc.getItemByNameOptional(itemName);
			if (optSchemaDataItem.isPresent())
			{
				schemaDataItem = optSchemaDataItem.get();
				uiFormat = schemaDataItem.getUIFormat();
				dataFormat = schemaDataItem.getDataFormat();
				if (StringUtils.isNotEmpty(dataFormat))
				{
					ceDataItem.setDataFormat(dataFormat);
					if (Data.isDateOrTime(schemaDataItem.getType()))
					{
						if ((StringUtils.isNotEmpty(uiFormat)) && (! uiFormat.equals(dataFormat)))
						{
							uiDate = Data.createDate(ceDataItem.getValue(), uiFormat);
							ceDataItem.setValue(Data.dateValueFormatted(uiDate, dataFormat));
						}
					}
				}
			}
		}

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void enableHighlighting(AppCtx anAppCtx, DataDoc aSchemaDoc, DSCriteria aDSCriteria)
	{
		Logger appLogger = anAppCtx.getLogger(this, "enableHighlighting");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		for (DataItem dataItem : aSchemaDoc.getItems())
		{
			if (dataItem.isFeatureTrue(Redis.FEATURE_IS_HIGHLIGHTED))
				aDSCriteria.add(dataItem.getName(), Data.Operator.HIGHLIGHT, true);
		}

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Executes a SmartClient data source fetch operation based on a criteria.
	 *
	 * @param aRequest SmartClient data source request instance
	 *
	 * @return SmartClient data source response instance
	 *
	 * @throws Exception Signifying operation failure
	 */
	@SuppressWarnings("WrapperTypeMayBePrimitive")
	public DSResponse executeFetch(DSRequest aRequest)
		throws Exception
	{
		String responseType;
		DSResponse dsResponse;

		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		DSCriteria dsCriteria = scDSRequest.convertDSCriteria(aRequest);

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeFetch");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
		String criteriaName = dsCriteria.getName();
		dsCriteria.setName(criteriaName + " - SmartClient");
		dsCriteriaLogger.writeFull(dsCriteria);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRediSearchHash().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();
		appSession.save();

// Calculate our fetch values.

		Long scReqestStartRow = aRequest.getStartRow();
		Long scReqestEndRow = aRequest.getEndRow();
		int fetchRowStart = scReqestStartRow.intValue();
		int fetchRowFinish = scReqestEndRow.intValue();
		int fetchRowLimit = fetchRowFinish - fetchRowStart;
		appLogger.debug(String.format("'%s' (request): fetchRowStart = %d, fetchRowFinish = %d, fetchRowLimit = %d", dsCriteria.getName(),
									  fetchRowStart, fetchRowFinish, fetchRowLimit));
		String fetchPolicy = DS.fetchPolicyFromCriteria(dsCriteria);
		if (StringUtils.equals(fetchPolicy, DS.FETCH_POLICY_PAGING))
		{
			fetchRowStart = DS.offsetFromCriteria(dsCriteria);
			fetchRowLimit = DS.limitFromCriteria(dsCriteria);
			appLogger.debug(String.format("'%s' (criteria) - [%s]: fetchRowStart = %d, fetchRowLimit = %d", dsCriteria.getName(),
										  fetchPolicy, fetchRowStart, fetchRowLimit));
		}
		else
			appLogger.debug(String.format("'%s' (criteria) - [%s]: fetchRowStart = %d, fetchRowFinish = %d, fetchRowLimit = %d", dsCriteria.getName(),
										  fetchPolicy, fetchRowStart, fetchRowFinish, fetchRowLimit));

// Execute the operation.

		RedisDS redisDS = appResource.getRedisDS();
		GridDS gridDS = appResource.getGridDS();
		RedisKey redisKey = redisDS.getRedisKey();
		RedisSearch redisSearch = appResource.getRedisSearch();
		DataDoc searchSchemaDoc = redisSearch.getSearchSchema();
		boolean isHighlightEnabled = DS.highlightFromCriteria(dsCriteria);
		String searchTerms = DS.searchFromCriteria(dsCriteria);
		if (StringUtils.isEmpty(searchTerms))
			dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
		else
		{
			DSCriterion dsCriterion = new DSCriterion(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, searchTerms);
			if (isHighlightEnabled)
				enableHighlighting(appCtx, searchSchemaDoc, dsCriteria);
			dsCriteria.add(dsCriterion);
		}
		String facetFieldNameValue = DS.facetNameValueFromCriteria(dsCriteria);
		if (StringUtils.isNotEmpty(facetFieldNameValue))
		{
			int offset;
			String fieldName, fieldValue;

			ArrayList<String> facetNameValueList = StrUtl.expandToList(facetFieldNameValue, StrUtl.CHAR_PIPE);
			for (String fieldNameValue : facetNameValueList)
			{
				offset = fieldNameValue.indexOf(StrUtl.CHAR_COLON);
				if (offset > 0)
				{
					fieldName = fieldNameValue.substring(0, offset);
					fieldValue = fieldNameValue.substring(offset + 1);
					if ((StringUtils.isNotEmpty(fieldName)) && (StringUtils.isNotEmpty(fieldValue)))
						dsCriteria.add(fieldName, Data.Operator.EQUAL, fieldValue);
				}
			}
		}
		assignDataFormats(appCtx, searchSchemaDoc, dsCriteria);
		dsCriteria.setName(criteriaName + " - Redis App Studio");
		dsCriteriaLogger.writeFull(dsCriteria);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataGrid dataGrid = redisSearch.query(dsCriteria, fetchRowStart, fetchRowLimit);
		dataGrid.setName(appResource.getTitle());
		String gridKeyName = redisKey.moduleCore().redisSortedSet().dataObject(gridDS.getDataGrid()).name();
		dataGrid.addFeature(Redis.FEATURE_KEY_NAME, gridKeyName);
		appResource.setResultDS(dataGrid);
		stopWatch.stop();

		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dsCriteria.getName(), rowCount, stopWatch.getTime()));

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		int responseRowStart = scdsResponse.mainGridRowStart(fetchRowStart);
		int responseRowTotal = scdsResponse.mainGridRowTotal(dataGrid, fetchRowLimit);
		int responseRowFinish = scdsResponse.mainGridRowFinish(dataGrid, fetchRowStart, responseRowTotal);
		if (isHighlightEnabled)
		{
			if ((REDIS_STACK_62_HIGHLIGHT_ISSUE) && (StringUtils.isNotEmpty(searchTerms)))
			{
				responseType = "Highlight(Memory)";
				dsResponse = scdsResponse.createWithHighlights(dataGrid, searchTerms, searchSchemaDoc, responseRowStart, responseRowFinish, responseRowTotal);
			}
			else
			{
				responseType = "Highlight(RediSearch)";
				dsResponse = scdsResponse.createWithHighlights(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);
			}
		}
		else
		{
			responseType = "Highlight(Disabled)";
			dsResponse = scdsResponse.create(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);
		}
		appLogger.debug(String.format("'%s': %s - responseRowStart = %d, responseRowFinish = %d, responseRowTotal = %d", dsCriteria.getName(),
									  responseType, responseRowStart, responseRowFinish, responseRowTotal));

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	// Handles an edge cases where the UI grid is out of sync with the result data source
	private void refreshResultDS(AppCtx anAppCtx, AppResource anAppResource, DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = anAppCtx.getLogger(this, "refreshResultDS");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		RedisDS redisDS = anAppResource.getRedisDS();
		RedisGrid redisGrid = redisDS.createGrid();
		redisDS.setCommandStreamActiveFlag(false);
		redisGrid.loadGridPipeline(aDataGrid, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
		redisDS.setCommandStreamActiveFlag(true);
		anAppResource.setResultDS(aDataGrid);

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Executes a SmartClient data source add operation based on a document.
	 *
	 * @param aRequest SmartClient data source request instance
	 *
	 * @return SmartClient data source response instance
	 *
	 * @throws Exception Signifying operation failure
	 */
	public DSResponse executeAdd(DSRequest aRequest)
		throws Exception
	{
		DSResponse dsResponse;

		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeAdd");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRediSearchHash().document(scDocument).build();
		AppResource appResource = appSession.establish();

// Execute the operation.

		DataGrid dataGrid = appResource.getResultDS().getDataGrid();
		DataDoc dataDoc = scDSRequest.convertDocument(scDocument, dataGrid.getColumns());
		RedisDS redisDS = appResource.getRedisDS();
		RedisGrid redisGrid = redisDS.createGrid();
		RedisSearch redisSearch = appResource.getRedisSearch();

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		redisGrid.add(dataGrid, dataDoc);
		redisSearch.addSuggestion(dataDoc);
		refreshResultDS(appCtx, appResource, dataGrid);
		stopWatch.stop();
		appLogger.debug(String.format("'%s': %d items added in %d milliseconds.", scDS.getName(), dataDoc.count(), stopWatch.getTime()));

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		dsResponse = scdsResponse.create(scDocument, true);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	private DataDoc locateDocPrepareForUpdate(AppCtx anAppCtx, DataGrid aDataGrid, DataDoc aDataDoc)
		throws Exception
	{
		DataDoc dataDoc, updateDoc;
		Logger appLogger = anAppCtx.getLogger(this, "locateDocPrepareForUpdate");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		boolean isFound = false;
		int rowNumber = aDataGrid.getFeatureAsInt(DS.FEATURE_CUR_OFFSET);
		DataDoc schemaDoc = aDataGrid.getColumns();
		Optional<DataItem> optDataItem = schemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			dataDoc = null;
			DataItem primaryItem = optDataItem.get();
			String primaryIdValue = aDataDoc.getValueByName(primaryItem.getName());
			if (StringUtils.isEmpty(primaryIdValue))
				throw new DSException("Data source document is missing a primary id value - cannot update");
			int rowCount = aDataGrid.rowCount();
			for (int rowOffset = 0; rowOffset < rowCount; rowOffset++)
			{
				dataDoc = aDataGrid.getRowAsDoc(rowOffset);
				if (dataDoc.getValueByName(primaryItem.getName()).equals(primaryIdValue))
				{
					isFound = true;
					break;
				}
				else
					rowNumber++;
			}
			if (isFound)
			{
				updateDoc = new DataDoc(dataDoc);
				updateDoc.addFeature(DS.FEATURE_ROW_NUMBER, rowNumber);
				for (DataItem dataItem : aDataDoc.getItems())
					updateDoc.setValueByName(dataItem.getName(), dataItem.getValue());
				appLogger.debug(String.format("Matched primary key value '%s' at row %d.", primaryIdValue, rowNumber));
			}
			else
				throw new DSException(String.format("Unable to match primary id value '%s' with cached data grid - cannot update", primaryIdValue));
		}
		else
			throw new DSException("Data source is missing a primary id - cannot update");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return updateDoc;
	}

	/**
	 * Executes a SmartClient data source update operation based on a document.
	 *
	 * @param aRequest SmartClient data source request instance
	 *
	 * @return SmartClient data source response instance
	 *
	 * @throws Exception Signifying operation failure
	 */
	public DSResponse executeUpdate(DSRequest aRequest)
		throws Exception
	{
		DSResponse dsResponse;

		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeUpdate");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRediSearchHash().document(scDocument).build();
		AppResource appResource = appSession.establish();

// Execute the operation.

		DataGrid dataGrid = appResource.getResultDS().getDataGrid();
		refreshResultDS(appCtx, appResource, dataGrid);
		RedisDS redisDS = appResource.getRedisDS();
		RedisGrid redisGrid = redisDS.createGrid();
		RedisSearch redisSearch = appResource.getRedisSearch();

// The SmartClient grid widget will only send us a subset of changed items,
// so we need to load the current version of the document and apply the
// changes.

		DataDoc scDataDoc = scDSRequest.convertDocument(scDocument, dataGrid.getColumns());
		DataDoc updateDoc = locateDocPrepareForUpdate(appCtx, dataGrid, scDataDoc);
		int docRowNumber = updateDoc.getFeatureAsInt(DS.FEATURE_ROW_NUMBER);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		redisGrid.update(dataGrid, docRowNumber, scDataDoc);
		redisSearch.addSuggestion(scDataDoc);
		refreshResultDS(appCtx, appResource, dataGrid);
		boolean isOK = true;
		stopWatch.stop();
		appLogger.debug(String.format("'%s': Updated row %d (%d items) in %d milliseconds.", scDS.getName(), docRowNumber,
									  scDataDoc.count(), stopWatch.getTime()));

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	private int locatePrimaryRowNumber(AppCtx anAppCtx, DataGrid aDataGrid, DataDoc aDataDoc)
		throws Exception
	{
		DataDoc dataDoc;
		Logger appLogger = anAppCtx.getLogger(this, "locatePrimaryRowNumber");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		boolean isFound = false;
		int rowNumber = aDataGrid.getFeatureAsInt(DS.FEATURE_CUR_OFFSET);
		DataDoc schemaDoc = aDataGrid.getColumns();
		Optional<DataItem> optDataItem = schemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			String primaryItemName = dataItem.getName();
			String primaryIdValue = aDataDoc.getValueByName(primaryItemName);
			if (StringUtils.isEmpty(primaryIdValue))
				throw new DSException("Data source document is missing a primary id value - cannot update");
			int rowCount = aDataGrid.rowCount();
			for (int rowOffset = 0; rowOffset < rowCount; rowOffset++)
			{
				dataDoc = aDataGrid.getRowAsDoc(rowOffset);
				appLogger.debug(String.format("[Row %d] Cache '%s' value '%s' vs primary id '%s'", rowOffset, primaryItemName,
											  dataDoc.getValueByName(primaryItemName).equals(primaryIdValue), primaryIdValue));
				if (dataDoc.getValueByName(primaryItemName).equals(primaryIdValue))
				{
					isFound = true;
					appLogger.debug(String.format("Matched primary key value '%s' at row %d.", primaryIdValue, rowNumber));
					break;
				}
				else
					rowNumber++;
			}
			if (! isFound)
				throw new DSException(String.format("Unable to match primary id value '%s' with cached data grid - cannot remove", primaryIdValue));
		}
		else
			throw new DSException("Data source is missing a primary id - cannot remove");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return rowNumber;
	}

	/**
	 * Executes a SmartClient data source delete operation based on a document.
	 *
	 * @param aRequest SmartClient data source request instance
	 *
	 * @return SmartClient data source response instance
	 *
	 * @throws Exception Signifying operation failure
	 */
	public DSResponse executeRemove(DSRequest aRequest)
		throws Exception
	{
		DSResponse dsResponse;

		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeRemove");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

/// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRediSearchHash().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Execute the operation.

		DataGrid dataGrid = appResource.getResultDS().getDataGrid();
		refreshResultDS(appCtx, appResource, dataGrid);
		RedisDS redisDS = appResource.getRedisDS();
		RedisGrid redisGrid = redisDS.createGrid();
		DataDoc scDataDoc = scDSRequest.convertDocument(scDocument, dataGrid.getColumns());
		int docRowNumber = locatePrimaryRowNumber(appCtx, dataGrid, scDataDoc);

// The SmartClient grid widget will only send us the primary key value,
// so we need to load the current version of the document.

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		redisGrid.delete(dataGrid, docRowNumber);
		refreshResultDS(appCtx, appResource, dataGrid);
		stopWatch.stop();
		boolean isOK = true;
		appLogger.debug(String.format("Row number %d deleted in %d milliseconds.", docRowNumber, stopWatch.getTime()));
		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}