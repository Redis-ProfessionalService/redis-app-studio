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
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;

import java.util.Map;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class DatabaseDS extends BasicDataSource
{
	private final String CLASS_NAME = "DatabaseDS";
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
		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		DSCriteria dsCriteria = scDSRequest.convertDSCriteria(aRequest);

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeFetch");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetRedisCore().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();
		appSession.save();

// Execute the operation.

		RedisDS redisDS = appResource.getRedisDS();
		RedisCore redisCore = redisDS.createCore();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataDoc infoDoc = redisCore.info();
		boolean isOK = infoDoc.count() > 0;
		stopWatch.stop();
		appLogger.debug(String.format("Redis DB information retrieved in %d milliseconds.", stopWatch.getTime()));

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(infoDoc, appResource, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}

	/**
	 * Executes a SmartClient data source update operation based on a document.
	 *
	 * Note: We are using the update operation to trigger a database flush
	 *       operation in the UI.
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
		AppResource appResource = appSession.establish();

// Execute the operation.

		RedisDS redisDS = appResource.getRedisDS();
		RedisCore redisCore = redisDS.createCore();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		redisCore.flushDatabase();
		stopWatch.stop();
		appLogger.debug(String.format("Redis DB flushed in %d milliseconds.", stopWatch.getTime()));

// Create our data source response.

		DataDoc dataDoc = new DataDoc("Database");
		dataDoc.add(new DataItem.Builder().name("id").value(1).build());
		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(dataDoc, true);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
