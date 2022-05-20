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

package com.redis.app.redis_app_studio.dm;

import com.isomorphic.datasource.BasicDataSource;
import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_grid.GridDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.Map;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class RelSchemaGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "RelSchemaGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "dm";

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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetMemory().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();
		appSession.save();

// Execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		GridDS gridDS = graphDS.getEdgeGridDS();

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
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataGrid dataGrid = graphDS.edgeSchemaDocToDataGrid(gridDS.getSchema(), false);
		stopWatch.stop();

		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dsCriteria.getName(),
									  rowCount, stopWatch.getTime()));

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		int responseRowStart = scdsResponse.auxGridRowStart(fetchRowStart);
		int responseRowTotal = scdsResponse.auxGridRowTotal(dataGrid, fetchRowLimit);
		int responseRowFinish = scdsResponse.auxGridRowFinish(dataGrid, fetchRowStart, responseRowTotal);
		DSResponse dsResponse = scdsResponse.create(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);
		appLogger.debug(String.format("'%s': responseRowStart = %d, responseRowFinish = %d, responseRowTotal = %d", dsCriteria.getName(),
									  responseRowStart, responseRowFinish, responseRowTotal));

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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetMemory().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		GridDS gridDS = graphDS.getEdgeGridDS();

// The SmartClient grid widget will only send us a subset of changed items,
// so we need to load the current version of the document and apply the
// changes.

		DataGrid schemaGrid = graphDS.edgeSchemaDocToDataGrid(gridDS.getSchema(), false);
		DataDoc schemaDoc = scDSRequest.convertDocument(scDocument, schemaGrid.getColumns());
		DataDoc rowDoc = null;
		int rowCount = schemaGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			rowDoc = schemaGrid.getRowAsDoc(row);
			if (rowDoc.getValueByName("item_name").equals(schemaDoc.getValueByName("item_name")))
				break;
		}
		if (rowDoc == null)
			throw new DSException(String.format("Unable to match item name '%s' in schema.", schemaDoc.getValueByName("item_name")));

		schemaDoc = scDSRequest.convertDocument(scDocument, rowDoc);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		boolean isOK = graphDS.updateEdgeSchema(schemaDoc);
		stopWatch.stop();
		appLogger.debug(String.format("'%s': Updated graph edge schema definition in %d milliseconds with an OK status of %s",
									  rowDoc.getValueByName("item_name"), stopWatch.getTime(),
									  StrUtl.booleanToString(isOK)));

//		DataDocLogger dataDocLogger = new DataDocLogger(appLogger);
//		dataDocLogger.writeFull(gridDS.getSchema());

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		dsResponse = scdsResponse.create(schemaDoc, true);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
