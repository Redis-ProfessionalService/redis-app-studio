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
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterionEntry;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class FacetGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "FacetGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "rs";

	/**
	 * This method is responsible for transforming data formats from the SmartClient UI
	 * to the internal Redis Workbench framework.  Currently, dates are the priority,
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

// Calculate our fetch values.

		Long scReqestStartRow = aRequest.getStartRow();
		Long scReqestEndRow = aRequest.getEndRow();
		int fetchRowStart = scReqestStartRow.intValue();
		int fetchRowFinish = scReqestEndRow.intValue();
		int fetchRowLimit = fetchRowFinish - fetchRowStart;
		appLogger.debug(String.format("'%s' (request): fetchRowStart = %d, fetchRowFinish = %d, fetchRowLimit = %d", dsCriteria.getName(),
									  fetchRowStart, fetchRowFinish, fetchRowLimit));
		String fetchPolicy = "virtual";
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

		RedisSearch redisSearch = appResource.getRedisSearch();
		String searchTerms = DS.searchFromCriteria(dsCriteria);
		if (StringUtils.isEmpty(searchTerms))
			dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
		else
			dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, searchTerms);

// Make sure we are working with the latest schema definition.

		int facetCount = 0;
		Optional<DataDoc> optDBSearchSchemaDoc = redisSearch.loadSchema();
		if (optDBSearchSchemaDoc.isPresent())
		{
			DataDoc dbSearchSchemaDoc = optDBSearchSchemaDoc.get();
			redisSearch.setSearchSchema(dbSearchSchemaDoc);
			for (DataItem searchItem : dbSearchSchemaDoc.getItems())
			{
				if (searchItem.isFeatureTrue(Redis.FEATURE_IS_FACET_FIELD))
				{
					facetCount++;
					dsCriteria.add(searchItem.getName(), Data.Operator.FACET, true);
				}
			}
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
		assignDataFormats(appCtx, redisSearch.getSearchSchema(), dsCriteria);
		dsCriteria.setName(criteriaName + " - Redis App Studio");
		dsCriteriaLogger.writeFull(dsCriteria);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		if (facetCount > 0)
		{
			int facetValueCount = DS.facetValueCountFromCriteria(dsCriteria);
			dsCriteria.deleteByNameOperator(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL);
			dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, facetValueCount);
			dataGrid = redisSearch.calculateUIFacets(dsCriteria);
		}
		else
			dataGrid = new DataGrid("Schema Without Facets Enabled");
		stopWatch.stop();

		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d facets - %d rows fetched in %d milliseconds.",
									  dsCriteria.getName(), facetCount, rowCount, stopWatch.getTime()));

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
}
