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
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterionEntry;
import com.redis.foundation.io.DSCriteriaLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class AppViewGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "AppViewGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "dm";

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
						if ((org.apache.commons.lang3.StringUtils.isNotEmpty(uiFormat)) && (! uiFormat.equals(dataFormat)))
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
		GridDS gridDS;
		DataGrid dataGrid;
		String criteriaName;

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
		criteriaName = dsCriteria.getName();
		dsCriteria.setName(criteriaName + " - SmartClient");
		dsCriteriaLogger.writeFull(dsCriteria);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetMemory().criteria(dsCriteria).build();
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

// Execute the operation with time tracking.

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String searchTerms = DS.searchFromCriteria(dsCriteria);
		if (appResource.isGraphDataType())
		{
			GraphDS graphDS = appResource.getGraphDS();
			gridDS = graphDS.getVertexGridDS();
		}
		else if (appResource.isJsonDataType())
			gridDS = appResource.getJsonDS();
		else
			gridDS = appResource.getGridDS();

// Adjust fetch request parameters if they exceed data source totals - edge case scenario seen with graph testing.

		if (fetchRowStart >= gridDS.count())
		{
			fetchRowStart = 0;
			fetchRowFinish = fetchRowStart + fetchRowLimit;
			appLogger.debug(String.format("'%s' (criteria) - [%s]: Adjusted fetchRowStart = %d, fetchRowFinish = %d, fetchRowLimit = %d", dsCriteria.getName(),
										  fetchPolicy, fetchRowStart, fetchRowFinish, fetchRowLimit));
		}

		assignDataFormats(appCtx, gridDS.getSchema(), dsCriteria);
		dsCriteria.setName(criteriaName + " - Redis App Studio");
		dsCriteriaLogger.writeFull(dsCriteria);
		if (StringUtils.isNotEmpty(searchTerms))
		{
			dataGrid = gridDS.search(searchTerms, fetchRowStart, fetchRowLimit);
			if (dataGrid.rowCount() > 0)
			{
				dataGrid.clearFeatures();
				gridDS = new GridDS(appCtx);
				gridDS.setDatGrid(dataGrid);
				dataGrid = gridDS.fetch(dsCriteria);
			}
		}
		else
			dataGrid = gridDS.fetch(dsCriteria, fetchRowStart, fetchRowLimit);
		int rowCount = dataGrid.rowCount();
		if (appResource.isGraphDataType())
		{
			if (rowCount == 0)
				appResource.setResultDataGraph(null);
			else if ((dsCriteria.count() == 0) && (StringUtils.isEmpty(searchTerms)))
				appResource.setResultDataGraph(null);
			else
			{
				DataGraph dataGraph = new DataGraph(dataGrid);
				appResource.setResultDataGraph(dataGraph);
			}
		}
		stopWatch.stop();

		appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dsCriteria.getName(),
									  rowCount, stopWatch.getTime()));

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		int responseRowStart = scdsResponse.mainGridRowStart(fetchRowStart);
		int responseRowTotal = scdsResponse.mainGridRowTotal(dataGrid, fetchRowLimit);
		int responseRowFinish = scdsResponse.mainGridRowFinish(dataGrid, fetchRowStart, responseRowTotal);
		DSResponse dsResponse = scdsResponse.create(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);
		appLogger.debug(String.format("'%s': responseRowStart = %d, responseRowFinish = %d, responseRowTotal = %d", dsCriteria.getName(),
									  responseRowStart, responseRowFinish, responseRowTotal));

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetMemory().document(scDocument).build();
		AppResource appResource = appSession.establish();

// Identify our data type and execute the operation.

		if (appResource.isGraphDataType())
		{
			GraphDS graphDS = appResource.getGraphDS();
			DataDoc dataDoc = scDSRequest.convertDocument(scDocument, graphDS.getVertexGridDS().getSchema());
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			boolean isOK = graphDS.addVertex(dataDoc);
			if (isOK)
			{
				appResource.setResultDataGraph(null);
				appResource.setGraphNodeDocument(dataDoc);
			}
			stopWatch.stop();
			appLogger.debug(String.format("'%s': %d vertex items added in %d milliseconds.", scDS.getName(),
										  dataDoc.count(), stopWatch.getTime()));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			dsResponse = scdsResponse.create(scDocument, isOK);
		}
		else
		{
			GridDS gridDS;

			if (appResource.isJsonDataType())
				gridDS = appResource.getJsonDS();
			else
				gridDS = appResource.getGridDS();
			DataDoc dataDoc = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			boolean isOK = gridDS.add(dataDoc);
			stopWatch.stop();
			appLogger.debug(String.format("'%s': %d document items added in %d milliseconds.", scDS.getName(),
										  dataDoc.count(), stopWatch.getTime()));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			dsResponse = scdsResponse.create(scDocument, isOK);
		}

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
		AppResource appResource = appSession.establish();

// Identify our data type and execute the operation.

		if (appResource.isGraphDataType())
		{
			GraphDS graphDS = appResource.getGraphDS();
			appResource.setGraphNodeDocument(null);
			DataDoc dataDoc1 = scDSRequest.convertDocument(scDocument, graphDS.getVertexGridDS().getSchema());

// The SmartClient grid widget will only send us a subset of changed items, so we need to load the current version of
// the document and apply the changes.

			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			DataDoc dataDoc2 = graphDS.loadVertexApplyUpdate(dataDoc1);
			appCtx.addProperty(Constants.APPCTX_PROPERTY_DS_GRAPH_NODE, dataDoc2);
			stopWatch.stop();
			appLogger.debug(String.format("'%s': %d vertex items updated in %d milliseconds.", scDS.getName(),
										  dataDoc1.count(), stopWatch.getTime()));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			dsResponse = scdsResponse.create(dataDoc2, true);
		}
		else
		{
			GridDS gridDS;

			if (appResource.isJsonDataType())
				gridDS = appResource.getJsonDS();
			else
				gridDS = appResource.getGridDS();
			DataDoc dataDoc1 = scDSRequest.convertDocument(scDocument, gridDS.getSchema());

// The SmartClient grid widget will only send us a subset of changed items, so we need to load the current version of
// the document and apply the changes.

			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			DataDoc dataDoc2 = gridDS.loadApplyUpdate(dataDoc1);
			stopWatch.stop();
			appLogger.debug(String.format("'%s': %d document items updated in %d milliseconds.", scDS.getName(),
										  dataDoc1.count(), stopWatch.getTime()));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			dsResponse = scdsResponse.create(dataDoc2, true);
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
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

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetMemory().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Identify our data type and execute the operation.

		if (appResource.isGraphDataType())
		{
			GraphDS graphDS = appResource.getGraphDS();
			appResource.setGraphNodeDocument(null);
			DataDoc dataDoc = scDSRequest.convertDocument(scDocument, graphDS.getVertexGridDS().getSchema());
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			boolean isOK = graphDS.deleteVertex(dataDoc);
			stopWatch.stop();
			appLogger.debug(String.format("'%s': 1 vertex deleted in %d milliseconds.", scDS.getName(),
										  stopWatch.getTime()));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			dsResponse = scdsResponse.create(scDocument, isOK);
		}
		else
		{
			GridDS gridDS;

			if (appResource.isJsonDataType())
				gridDS = appResource.getJsonDS();
			else
				gridDS = appResource.getGridDS();
			DataDoc dataDoc = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			boolean isOK = gridDS.delete(dataDoc);
			stopWatch.stop();
			appLogger.debug(String.format("'%s': 1 document deleted in %d milliseconds.", scDS.getName(),
										  stopWatch.getTime()));
			SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
			scdsResponse.setDebugFlag(false);
			dsResponse = scdsResponse.create(scDocument, isOK);
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
