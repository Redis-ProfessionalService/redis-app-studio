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
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DSCriteriaLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked", "rawtypes", "FieldCanBeLocal"})
public class SuggestListDS extends BasicDataSource
{
	private final String CLASS_NAME = "SuggestListDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "rg";

	/**
	 * Executes a SmartClient data source fetch operation based on a criteria.
	 *
	 * @param aRequest SmartClient data source request instance
	 *
	 * @return SmartClient data source response instance
	 *
	 * @throws Exception Signifying operation failure
	 */
	@SuppressWarnings({"WrapperTypeMayBePrimitive", "UnnecessaryLocalVariable"})
	public DSResponse executeFetch(DSRequest aRequest)
		throws Exception
	{
		DataGrid dataGrid;
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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisGraph().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();

// Execute the operation.

		GraphDS graphDS = appResource.getGraphDS();
		GridDS gridDS = graphDS.getVertexGridDS();

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
		String suggestTerms = DS.suggestFromCriteria(dsCriteria);
		Optional<DataItem> optSuggestDataItem = gridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_SUGGEST);
		if (optSuggestDataItem.isEmpty())
			throw new DSException(String.format("Grid is missing a data item with a '%s' feature.", Data.FEATURE_IS_SUGGEST));
		DataItem suggestDataItem = optSuggestDataItem.get();

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		if (StringUtils.isNotEmpty(suggestTerms))
			dataGrid = gridDS.suggest(suggestTerms, fetchRowLimit);
		else
		{
			suggestTerms = "EMPTY STRING";
			dataGrid = new DataGrid(gridDS.getSchema());
		}
		stopWatch.stop();

		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d/%d rows fetched for item '%s' in %d milliseconds.", suggestTerms,
									  rowCount, gridDS.count(), suggestDataItem.getName(), stopWatch.getTime()));

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		int responseRowStart = scdsResponse.auxGridRowStart(fetchRowStart);
		int responseRowTotal = scdsResponse.auxGridRowTotal(dataGrid, fetchRowLimit);
		int responseRowFinish = scdsResponse.auxGridRowFinish(dataGrid, fetchRowStart, responseRowTotal);
		if (rowCount > 0)
		{
			Optional<DataItem> optDataItem = dataGrid.getColumns().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_SUGGEST);
			if (optDataItem.isEmpty())
				dsResponse = scdsResponse.create(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);
			else
			{
				DataItem dataItem = optDataItem.get();
				dsResponse = scdsResponse.create(dataGrid, dataItem, Data.FEATURE_DS_SUGGEST);
			}
		}
		else
			dsResponse = scdsResponse.create(dataGrid, responseRowStart, responseRowFinish, responseRowTotal);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
