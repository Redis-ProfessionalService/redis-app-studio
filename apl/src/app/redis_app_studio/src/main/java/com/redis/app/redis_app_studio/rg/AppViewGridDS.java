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

package com.redis.app.redis_app_studio.rg;

import com.isomorphic.datasource.BasicDataSource;
import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.graph.RedisGraphs;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.*;
import com.redis.foundation.io.DGCriteriaLogger;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.io.DataDocLogger;
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
@SuppressWarnings({"unchecked", "rawtypes", "FieldCanBeLocal"})
public class AppViewGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "AppViewGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "rg";
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
		DataGrid dataGrid;
		int responseRowTotal;
		DataSource scDS = aRequest.getDataSource();
		SCDSRequest scDSRequest = new SCDSRequest();
		DGCriteria dgCriteria = scDSRequest.convertDGCriteria(aRequest);

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeFetch");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisGraph().criteria(dgCriteria).build();
		AppResource appResource = appSession.establish();
		appSession.save();

// Ensure data graph criteria is assigned the data source title.

		DGCriteriaLogger dgCriteriaLogger = new DGCriteriaLogger(appLogger);
		dgCriteria.setName(appResource.getTitle());
		dgCriteriaLogger.writeFull(dgCriteria);

// Calculate our fetch values.

		Long scReqestStartRow = aRequest.getStartRow();
		Long scReqestEndRow = aRequest.getEndRow();
		int fetchRowStart = scReqestStartRow.intValue();
		int fetchRowFinish = scReqestEndRow.intValue();
		int fetchRowLimit = fetchRowFinish - fetchRowStart;
		appLogger.debug(String.format("'%s' (request): fetchRowStart = %d, fetchRowFinish = %d, fetchRowLimit = %d", dgCriteria.getName(),
									  fetchRowStart, fetchRowFinish, fetchRowLimit));
		String fetchPolicy = DS.fetchPolicyFromCriteria(dgCriteria);
		if (StringUtils.equals(fetchPolicy, DS.FETCH_POLICY_PAGING))
		{
			fetchRowStart = DS.offsetFromCriteria(dgCriteria);
			fetchRowLimit = DS.limitFromCriteria(dgCriteria);
			appLogger.debug(String.format("'%s' (criteria) - [%s]: fetchRowStart = %d, fetchRowLimit = %d", dgCriteria.getName(),
										  fetchPolicy, fetchRowStart, fetchRowLimit));
		}
		else
			appLogger.debug(String.format("'%s' (criteria) - [%s]: fetchRowStart = %d, fetchRowFinish = %d, fetchRowLimit = %d", dgCriteria.getName(),
										  fetchPolicy, fetchRowStart, fetchRowFinish, fetchRowLimit));

// Execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		GridDS gridDS = graphDS.getVertexGridDS();
		DataDoc vertexSchemaDoc = gridDS.getSchema();
		RedisGraphs redisGraph = appResource.getRedisGraph();

/* This logic is going to deviate from all other grid fetch methods because a DataGraph
   uses a completely different DGCriteria class to define its search criteria. */

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String searchTerms = DS.searchFromCriteria(dgCriteria);
		if (StringUtils.isNotEmpty(searchTerms))
		{
			dataGrid = redisGraph.queryNodeText(appResource.getTitle(), vertexSchemaDoc, searchTerms, fetchRowStart, fetchRowLimit);
			if (dataGrid.rowCount() == 0)
				appResource.setResultDataGraph(null);
			else
			{
				DataGraph dataGraph = new DataGraph(dataGrid);
				appResource.setResultDataGraph(dataGraph);
			}
		}
		else if (dgCriteria.count() > 0)
		{
			DataGraph dataGraph = redisGraph.queryPattern(dgCriteria);
			appLogger.debug(String.format("[%s] (%d v, %d e): %s%n%n", dgCriteria.getName(), dataGraph.getVertexDocSet().size(),
										  dataGraph.getEdgeSet().size(), dataGraph.getFeature(Redis.FEATURE_REDISGRAPH_QUERY)));
			appResource.setResultDataGraph(dataGraph);
			dataGrid = dataGraph.getVertexDataGrid();
		}
		else
		{
			DataGraph dataGraph = redisGraph.queryAll(appResource.getTitle());
			appResource.setResultDataGraph(null);
			dataGrid = dataGraph.getVertexDataGrid();
		}
		stopWatch.stop();
		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dgCriteria.getName(), rowCount, stopWatch.getTime()));

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		int responseRowStart = scdsResponse.mainGridRowStart(fetchRowStart);
		if (rowCount == 0)
			responseRowTotal = 0;
		else
			responseRowTotal = rowCount + 1;
		int responseRowFinish = scdsResponse.mainGridRowFinish(dataGrid, fetchRowStart, responseRowTotal);
		DSResponse dsResponse = scdsResponse.create(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);
		appLogger.debug(String.format("'%s': responseRowStart = %d, responseRowFinish = %d, responseRowTotal = %d", dgCriteria.getName(),
									  responseRowStart, responseRowFinish, responseRowTotal));

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	// Handles an edge cases where the UI grid is out of sync with the result data source
	private void refreshResultDS(AppCtx anAppCtx, AppResource anAppResource, DataGraph aDataGraph)
		throws RedisDSException
	{
		Logger appLogger = anAppCtx.getLogger(this, "refreshResultDS");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		RedisDS redisDS = anAppResource.getRedisDS();

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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisGraph().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Identify our data type and execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		DataDoc dataDoc = scDSRequest.convertDocument(scDocument, graphDS.getVertexGridDS().getSchema());
		RedisGraphs redisGraph = appResource.getRedisGraph();
		DataGraph dataGraph = appResource.getResultDataGraph();
		if (dataGraph == null)
		{
			appLogger.warn("The result data graph was 'null' and rebuilt from the graph data source.");
			dataGraph = graphDS.createGraph();
		}

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		boolean isOK = graphDS.addVertex(dataDoc);
		if (isOK)
		{
			appResource.setResultDataGraph(null);
			appResource.setGraphNodeDocument(dataDoc);
			String vertexLabel = dataDoc.getValueByName(Data.GRAPH_VERTEX_LABEL_NAME);
			if (StringUtils.isNotEmpty(vertexLabel))
			{
				dataDoc.setName(vertexLabel);
				dataDoc.setTitle(vertexLabel);
				dataDoc.remove(Data.GRAPH_VERTEX_LABEL_NAME);
				appLogger.debug(String.format("Assigned '%s' as the vertex label.", vertexLabel));
			}
//			DataDocLogger dataDocLogger = new DataDocLogger(appLogger);
//			dataDocLogger.writeSimple(dataDoc);
			isOK = redisGraph.addNode(dataGraph, dataDoc);
		}
		stopWatch.stop();
		appLogger.debug(String.format("'%s': %d vertex items added in %d milliseconds.", scDS.getName(),
									  dataDoc.count(), stopWatch.getTime()));
		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(scDocument, isOK);

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
		boolean isOK;
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisGraph().document(scDocument).build();
		AppResource appResource = appSession.establish();

// Identify our data type and execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		RedisGraphs redisGraph = appResource.getRedisGraph();
		DataGraph dataGraph = appResource.getResultDataGraph();
		if (dataGraph == null)
		{
			appLogger.warn("The result data graph was 'null' and rebuilt from the graph data source.");
			dataGraph = graphDS.createGraph();
		}
		DataDoc dataDoc1 = scDSRequest.convertDocument(scDocument, graphDS.getVertexGridDS().getSchema());

// The SmartClient grid widget will only send us a subset of changed items, so we need to load the current version of
// the document and apply the changes.

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataDoc dataDoc2 = graphDS.loadVertexApplyUpdate(dataDoc1);
		appResource.setResultDataGraph(null);
		appResource.setGraphNodeDocument(dataDoc2);
		String vertexLabel = dataDoc2.getValueByName(Data.GRAPH_VERTEX_LABEL_NAME);
		if (StringUtils.isNotEmpty(vertexLabel))
		{
			dataDoc2.setName(vertexLabel);
			dataDoc2.setTitle(vertexLabel);
			DataItem dataItem = dataDoc2.getItemByName(Data.GRAPH_VERTEX_LABEL_NAME);
			dataDoc2.remove(Data.GRAPH_VERTEX_LABEL_NAME);
			appLogger.debug(String.format("Assigned '%s' as the vertex label.", vertexLabel));
			int propertiesSet = redisGraph.updateNode(dataGraph, dataDoc2);
			isOK = propertiesSet > 0;
			dataDoc2.add(dataItem);
		}
		else
		{
			isOK = false;
			appLogger.error("Vertex label for data document is empty.");
		}
		stopWatch.stop();
		if (isOK)
			appLogger.debug(String.format("'%s': %d vertex items updated in %d milliseconds.", scDS.getName(),
										  dataDoc1.count(), stopWatch.getTime()));
		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(dataDoc2, isOK);

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
		boolean isOK;
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisGraph().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Identify our data type and execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		RedisGraphs redisGraph = appResource.getRedisGraph();
		DataGraph dataGraph = appResource.getResultDataGraph();
		if (dataGraph == null)
		{
			appLogger.warn("The result data graph was 'null' and rebuilt from the graph data source.");
			dataGraph = graphDS.createGraph();
		}
		GridDS gridDS = graphDS.getVertexGridDS();
		DataDoc dataDoc1 = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
		Optional<DataDoc> optDataDoc = gridDS.findDataDocByPrimaryId(dataDoc1);
		if (optDataDoc.isPresent())
		{
			DataDoc dataDoc2 = optDataDoc.get();
			isOK = graphDS.deleteVertex(dataDoc2);
			if (isOK)
			{
				String vertexLabel = dataDoc2.getValueByName(Data.GRAPH_VERTEX_LABEL_NAME);
				if (StringUtils.isNotEmpty(vertexLabel))
				{
					dataDoc2.setName(vertexLabel);
					dataDoc2.setTitle(vertexLabel);
					dataDoc2.remove(Data.GRAPH_VERTEX_LABEL_NAME);
					appLogger.debug(String.format("Assigned '%s' as the vertex label.", vertexLabel));
					StopWatch stopWatch = new StopWatch();
					stopWatch.start();
					int nodesDeleted = redisGraph.deleteNode(dataGraph, dataDoc2);
					isOK = nodesDeleted > 0;
					stopWatch.stop();
					if (isOK)
						appLogger.debug(String.format("'%s': 1 vertex deleted in %d milliseconds.", scDS.getName(),
													  stopWatch.getTime()));
				}
				else
				{
					isOK = false;
					appLogger.error("Vertex label for data document is empty.");
				}
			}
		}
		else
		{
			isOK = false;
			appLogger.error("Unable to delete vertex node by id.");
		}

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
