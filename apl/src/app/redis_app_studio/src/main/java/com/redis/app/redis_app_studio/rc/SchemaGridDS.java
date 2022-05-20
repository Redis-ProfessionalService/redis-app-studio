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

package com.redis.app.redis_app_studio.rc;

import com.isomorphic.datasource.BasicDataSource;
import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class SchemaGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "SchemaGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "rc";

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
		dsCriteriaLogger.writeFull(dsCriteria);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisCore().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();
		appSession.save();

// Calculate our fetch values.

		Long scReqestStartRow = aRequest.getStartRow();
		Long scReqestEndRow = aRequest.getEndRow();
		int fetchRowStart = scReqestStartRow.intValue();
		int fetchRowFinish = scReqestEndRow.intValue();
		int fetchRowLimit = fetchRowFinish - fetchRowStart;
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

		GridDS gridDS = appResource.getGridDS();
		RedisDS redisDS = appResource.getRedisDS();
		RedisCore redisCore = redisDS.createCore();
		RedisGrid redisGrid = redisDS.createGrid();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		RedisKey redisKey = redisDS.getRedisKey();
		String gridKeyName = redisKey.moduleCore().redisSortedSet().dataObject(gridDS.getDataGrid()).name();
		Optional<DataGrid> optDataGrid = redisGrid.getGridSchema(gridKeyName);
		if (optDataGrid.isPresent())
		{
			DataGrid schemaDataGrid = optDataGrid.get();
			DataDoc dbSchemaDoc = schemaDataGrid.getColumns();
			DataGrid schemaGrid = redisCore.schemaDocToDataGrid(dbSchemaDoc, false);
			stopWatch.stop();
			int rowCount = schemaGrid.rowCount();
			appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dsCriteria.getName(),
										  rowCount, stopWatch.getTime()));

// Create our data source response.

			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			int responseRowStart = scdsResponse.auxGridRowStart(fetchRowStart);
			int responseRowTotal = scdsResponse.auxGridRowTotal(schemaGrid, fetchRowLimit);
			int responseRowFinish = scdsResponse.auxGridRowFinish(schemaGrid, fetchRowStart, responseRowTotal);
			dsResponse = scdsResponse.create(schemaGrid, responseRowStart, responseRowFinish, responseRowTotal);
			appLogger.debug(String.format("'%s': responseRowStart = %d, responseRowFinish = %d, responseRowTotal = %d", dsCriteria.getName(),
										  responseRowStart, responseRowFinish, responseRowTotal));
		}
		else
			throw new DSException(String.format("%s: The RedisCore schema for this data grid was not found in database.", gridKeyName));

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisCore().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Execute the operation.

		RedisDS redisDS = appResource.getRedisDS();
		RedisCore redisCore = redisDS.createCore();
		RedisGrid redisGrid = redisDS.createGrid();
		GridDS gridDS = appResource.getGridDS();

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String gridKeyName = redisDS.createKey().moduleCore().redisSortedSet().dataName(appResource.getTitle()).name();
		Optional<DataGrid> optDataGrid = redisGrid.getGridSchema(gridKeyName);
		if (optDataGrid.isPresent())
		{
			DataGrid schemaDataGrid = optDataGrid.get();
			DataDoc dbSchemaDoc = schemaDataGrid.getColumns();
			DataGrid schemaGrid = redisCore.schemaDocToDataGrid(dbSchemaDoc, false);

// The SmartClient grid widget will only send us a subset of changed items,
// so we need to load the current version of the document and apply the
// changes.

			DataDoc schemaDoc = scDSRequest.convertDocument(scDocument, dbSchemaDoc);
			DataDoc rowDoc = null;
			int rowCount = schemaGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				rowDoc = schemaGrid.getRowAsDoc(row);
				if (rowDoc.getValueByName("item_name").equals(schemaDoc.getValueByName("item_name")))
					break;
			}
			if (rowDoc == null)
				throw new DSException(String.format("Unable to match item name '%s' in Redis schema.", schemaDoc.getValueByName("item_name")));

			schemaDoc = scDSRequest.convertDocument(scDocument, rowDoc);

			boolean rebuildDB = redisGrid.updateSchema(schemaDataGrid, schemaDoc);
			schemaDataGrid.setColumns(schemaDoc);
			gridDS.setDatGrid(schemaDataGrid);
			stopWatch.stop();

			appLogger.debug(String.format("'%s': Updated Redis schema definition in %d milliseconds.  Index database is: '%s'.",
										  rowDoc.getValueByName("item_name"), stopWatch.getTime(),
										  StrUtl.booleanToString(rebuildDB)));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(true);
			dsResponse = scdsResponse.create(schemaDoc, true);
		}
		else
			throw new DSException(String.format("%s: The RedisCore schema for this data grid was not found in database.", gridKeyName));

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Executes a SmartClient data source delete operation based on a document.
	 *
	 * Note: We are using the remove operation to trigger an index rebuild
	 *       required in the UI.
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
		boolean isOK;
		DSResponse dsResponse;

		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeRemove");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisCore().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Execute the operation.

		RedisDS redisDS = appResource.getRedisDS();
		RedisGrid redisGrid = redisDS.createGrid();

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String gridKeyName = redisDS.createKey().moduleCore().redisSortedSet().dataName(appResource.getTitle()).name();
		Optional<DataGrid> optDataGrid = redisGrid.getGridSchema(gridKeyName);
		if (optDataGrid.isPresent())
		{
			DataGrid dataGrid = optDataGrid.get();
			redisGrid.loadGrid(dataGrid, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
			appLogger.debug(String.format("[%s] dataGrid has %d rows.", dataGrid.getName(), dataGrid.rowCount()));

// Delete the data grid and then completely rebuild the database.

			DataGrid deleteGrid = new DataGrid(dataGrid);
			appLogger.debug(String.format("[%s] Deleting grid with %d rows.", gridKeyName, deleteGrid.rowCount()));
			redisGrid.delete(deleteGrid);
			appLogger.debug(String.format("[%s] Successfully deleted grid with %d rows.", gridKeyName, deleteGrid.rowCount()));

// Add all of the grid documents into Redis.

			dataGrid.setName(appResource.getTitle());
			redisGrid.add(dataGrid);
			appLogger.debug(String.format("[%s] Added grid with %d rows in %d milliseconds.", gridKeyName, dataGrid.rowCount(), stopWatch.getTime()));
			stopWatch.stop();
			isOK = dataGrid.rowCount() > 0;
			if (isOK)
				appResource.setGridDS(dataGrid);
		}
		else
			isOK = false;

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
