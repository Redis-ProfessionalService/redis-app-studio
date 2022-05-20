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

package com.redis.app.redis_app_studio.rsj;

import com.isomorphic.datasource.BasicDataSource;
import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
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
	private final String APPLICATION_PROPERTIES_PREFIX = "rs";
	private final boolean JEDIS_4X_SUGGESTION_REGRESSION = true;

	private DataGrid convertToUIDataGrid(DataGrid aDataGrid)
	{
		DataDoc dataDoc;

		DataGrid dataGrid = new DataGrid("Search Suggestions");
		dataGrid.addCol(new DataItem.Builder().name("_suggest").title("Suggestion").build());
		dataGrid.addCol(new DataItem.Builder().name("payload").title("Payload").build());
		dataGrid.addCol(new DataItem.Builder().type(Data.Type.Double).name("score").title("Score").build());

		DataDoc schemaDoc = aDataGrid.getColumns();
		Optional<DataItem> optPrimaryItem = schemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		Optional<DataItem> optSuggestItem = schemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_SUGGEST);
		if ((optPrimaryItem.isPresent()) && (optSuggestItem.isPresent()))
		{
			DataItem primaryItem = optSuggestItem.get();
			DataItem suggestItem = optSuggestItem.get();

			int rowCount = aDataGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = aDataGrid.getRowAsDoc(row);
				dataGrid.newRow();
				dataGrid.setValueByName("_suggest", dataDoc.getValueByName(suggestItem.getName()));
				dataGrid.setValueByName("payload", dataDoc.getValueByName(primaryItem.getName()));
				dataGrid.setValueByName("score", 1.0);
				dataGrid.addRow();
			}
		}

		return dataGrid;
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
	@SuppressWarnings({"WrapperTypeMayBePrimitive", "UnnecessaryLocalVariable"})
	public DSResponse executeFetch(DSRequest aRequest)
		throws Exception
	{
		DataGrid dataGrid;

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

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRediSearchJson().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();

// Calculate the fetch values.

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

		RedisSearch redisSearch = appResource.getRedisDS().createSearch(Redis.Document.JSON);
		String suggestTerms = DS.suggestFromCriteria(dsCriteria);
		StopWatch stopWatch = new StopWatch();
		if (JEDIS_4X_SUGGESTION_REGRESSION)
		{
			GridDS gridDS = appResource.getJsonDS();
			stopWatch.start();
			if (StringUtils.isNotEmpty(suggestTerms))
				dataGrid = convertToUIDataGrid(gridDS.suggest(suggestTerms, fetchRowLimit));
			else
				dataGrid = convertToUIDataGrid(new DataGrid(redisSearch.getDataSchema()));
			stopWatch.stop();
		}
		else
		{
			stopWatch.start();
			if (StringUtils.isNotEmpty(suggestTerms))
				dataGrid = redisSearch.getSuggestions(suggestTerms, fetchRowLimit, false);
			else
				dataGrid = new DataGrid(redisSearch.getDataSchema());
			stopWatch.stop();
		}
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

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
