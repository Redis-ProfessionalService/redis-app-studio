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
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.io.DataDocLogger;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.Date;
import java.util.Map;
import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class AppViewGridRelDS extends BasicDataSource
{
	private final String CLASS_NAME = "AppViewGridRelDS";
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

	private DSCriteria convertToEdgeCriteria(AppCtx anAppCtx, DSCriteria aDSCriteria)
	{
		DataItem ceDataItem;
		Logger appLogger = anAppCtx.getLogger(this, "convertToEdgeCriteria");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(aDSCriteria.getName() + " - Edge Criteria");
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			dsCriteria.add(Data.GRAPH_DST_VERTEX_ID_NAME, ceDataItem.getValue());
		}

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return dsCriteria;
	}

	private String extractPrimaryKeyIdFromEdgeCriteria(AppCtx anAppCtx, DSCriteria aDSCriteria)
	{
		Logger appLogger = anAppCtx.getLogger(this, "extractPrimaryKeyIdFromEdgeCriteria");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
			return ce.getItem().getValue();

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return StringUtils.EMPTY;
	}

	// Determine if 'src_vertex_id' assigned - if yes, then use.  Otherwise, use the property.
	private DataDoc locateSrcVertexDataDoc(AppCtx anAppCtx, GraphDS aGraphDS, DataDoc aSCDataDoc)
		throws FCException
	{
		DataDoc srcDataDoc;
		DataItem diPrimaryKey;
		Logger appLogger = anAppCtx.getLogger(this, "locateSrcVertexDataDoc");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		String srcVertexId = aSCDataDoc.getValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME);
		if (StringUtils.isNotEmpty(srcVertexId))
		{
			GridDS gridDS = aGraphDS.getVertexGridDS();
			Optional<DataItem> optDataItem = gridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
			if (optDataItem.isPresent())
				diPrimaryKey = optDataItem.get();
			else
				throw new DSException("Vertex grid data source missing primary key item.");
			DSCriteria dsCriteria = new DSCriteria("Criteria - Primary Key");
			dsCriteria.addFeature(Data.FEATURE_DS_OFFSET, 0);
			dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1);
			dsCriteria.add(diPrimaryKey.getName(), Data.Operator.EQUAL, srcVertexId);
			DataGrid dataGrid = gridDS.fetch(dsCriteria);
			if (dataGrid.rowCount() != 1)
			{
				String msgStr = String.format("Unable to isolate data document by primary id '%s'.", srcVertexId);
				appLogger.error(msgStr);
				throw new DSException(msgStr);
			}
			srcDataDoc = dataGrid.getRowAsDoc(0);
		}
		else
		{
			Object propertyObject = anAppCtx.getProperty(Constants.APPCTX_PROPERTY_DS_GRAPH_NODE);
			if (propertyObject == null)
			{
				String msgStr = "The session managing the graph node has expired - please reload the application";
				appLogger.error(msgStr);
				throw new FCException(msgStr);
			}
			srcDataDoc = (DataDoc) propertyObject;
		}

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return srcDataDoc;
	}

	private DataDoc locateDstVertexDataDoc(AppCtx anAppCtx, GraphDS aGraphDS, DataDoc aSCDataDoc)
		throws FCException
	{
		DataDoc dstDataDoc;
		Logger appLogger = anAppCtx.getLogger(this, "locateDstVertexDataDoc");

		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		DataDocLogger dataDocLogger = new DataDocLogger(appLogger);
		dataDocLogger.writeSimple(aSCDataDoc);
		String dstVertexName = aSCDataDoc.getValueByName(Data.GRAPH_VERTEX_NAME);
		if (StringUtils.isEmpty(dstVertexName))
		{
			String msgStr = String.format("'%s' is not assigned in SC data document", Data.GRAPH_VERTEX_NAME);
			appLogger.error(msgStr);
			throw new FCException(msgStr);
		}
		else
		{
			GridDS gridDS = aGraphDS.getVertexGridDS();
			DSCriteria dsCriteria = new DSCriteria("Criteria - " + Data.GRAPH_COMMON_NAME);
			dsCriteria.addFeature(Data.FEATURE_DS_OFFSET, 0);
			dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1);
			dsCriteria.add(Data.GRAPH_COMMON_NAME, Data.Operator.EQUAL, dstVertexName);
			DataGrid dataGrid = gridDS.fetch(dsCriteria);
			if (dataGrid.rowCount() != 1)
			{
				String msgStr = String.format("Unable to isolate data document by %s '%s'.", Data.GRAPH_VERTEX_NAME, dstVertexName);
				appLogger.error(msgStr);
				throw new DSException(msgStr);
			}
			dstDataDoc = dataGrid.getRowAsDoc(0);
		}

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return dstDataDoc;
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).graphCSV().targetMemory().criteria(dsCriteria).build();
		AppResource appResource = appSession.restore();

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

		GraphDS graphDS = appResource.getGraphDS();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		GridDS gridDS = graphDS.getEdgeGridDS();
//		DataGridLogger dataGridLogger = new DataGridLogger(appLogger);
//		dataGridLogger.write(gridDS.getDataGrid());
		assignDataFormats(appCtx, gridDS.getSchema(), dsCriteria);
		dsCriteria.setName(criteriaName + " - Redis App Studio");
		dsCriteriaLogger.writeFull(dsCriteria);
		DSCriteria dsEdgeCriteria = convertToEdgeCriteria(appCtx, dsCriteria);
		dsCriteriaLogger.writeFull(dsEdgeCriteria);
		String primaryKeyId = extractPrimaryKeyIdFromEdgeCriteria(appCtx, dsCriteria);
		dataGrid = graphDS.queryEdgesByPrimaryId(primaryKeyId, fetchRowLimit);
		stopWatch.stop();

		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dsEdgeCriteria.getName(),
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).graphCSV().targetMemory().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		GridDS gridDS = graphDS.getEdgeGridDS();

// The SmartClient grid widget will only send us a subset of changed items,
// so we need to load the current version of the document and apply the
// changes.

		DataDoc scDataDoc = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
		DataDoc srcDataDoc = locateSrcVertexDataDoc(appCtx, graphDS, scDataDoc);
		DataDoc dstDataDoc = locateDstVertexDataDoc(appCtx, graphDS, scDataDoc);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		boolean isOK = graphDS.addEdge(srcDataDoc, dstDataDoc, scDataDoc);
		stopWatch.stop();
		appLogger.debug(String.format("'%s' - %s: %d edge items added to graph in %d milliseconds.", scDS.getName(),
									  StrUtl.booleanToString(isOK), scDataDoc.count(), stopWatch.getTime()));

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Executes a SmartClient data source updated operation based on a document.
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).graphCSV().targetMemory().document(scDocument).build();
		AppResource appResource = appSession.restore();

// Execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		GridDS gridDS = graphDS.getEdgeGridDS();

// The SmartClient grid widget will only send us a subset of changed items,
// so we need to load the current version of the document and apply the
// changes.

		DataDoc scDataDoc = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
		DataDoc srcDataDoc = locateSrcVertexDataDoc(appCtx, graphDS, scDataDoc);
		DataDoc dstDataDoc = locateDstVertexDataDoc(appCtx, graphDS, scDataDoc);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		boolean isOK = graphDS.updateVertexEdge(srcDataDoc, dstDataDoc, scDataDoc);
		stopWatch.stop();
		appLogger.debug(String.format("'%s': %d edge items updated in graph within %d milliseconds.", scDS.getName(),
									  scDataDoc.count(), stopWatch.getTime()));

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
