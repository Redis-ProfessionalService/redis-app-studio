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
import com.redis.ds.ds_json.JsonDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DataDocLogger;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Map;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked", "rawtypes", "FieldCanBeLocal"})
public class GenAppFormDS extends BasicDataSource
{
	private final String CLASS_NAME = "GenAppFormDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "dm";

	// DM-GenAppForm.ds.xml
	private DataDoc createGenAppSchema()
	{
		DataDoc genAppSchema = new DataDoc("Generate Application");
		genAppSchema.add(new DataItem.Builder().name("app_group").title("App Group").build());
		genAppSchema.add(new DataItem.Builder().name("app_name").title("App Name").build());
		genAppSchema.add(new DataItem.Builder().name("app_prefix").title("App Prefix").build());
		genAppSchema.add(new DataItem.Builder().name("app_type").title("App Type").build());
		genAppSchema.add(new DataItem.Builder().name("ds_structure").title("DS Structure").build());
		genAppSchema.add(new DataItem.Builder().name("ds_title").title("DS Title").build());
		genAppSchema.add(new DataItem.Builder().type(Data.Type.Integer).name("grid_height").title("Grid Height").build());
		genAppSchema.add(new DataItem.Builder().name("rc_storage_type").title("Storage Type").build());
		genAppSchema.add(new DataItem.Builder().name("ui_facets").title("UI Facets").build());
		genAppSchema.add(new DataItem.Builder().name("skin_name").title("UI Theme").build());
		genAppSchema.add(new DataItem.Builder().name("gen_ds_1").title("Gen DS 1").build());
		genAppSchema.add(new DataItem.Builder().name("gen_ds_2").title("Gen DS 2").build());
		genAppSchema.add(new DataItem.Builder().name("gen_ds_3").title("Gen DS 3").build());
		genAppSchema.add(new DataItem.Builder().name("gen_ds_4").title("Gen DS 4").build());
		genAppSchema.add(new DataItem.Builder().name("gen_ds_5").title("Gen DS 5").build());
		genAppSchema.add(new DataItem.Builder().name("gen_html").title("Gen HTML").build());
		genAppSchema.add(new DataItem.Builder().name("gen_link").title("Gen Link").build());

		return genAppSchema;
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
		GridDS gridDS;
		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeAdd");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Preparing to upload the file to local document storage.

		DataDoc genAppSchema = createGenAppSchema();
		DataDoc dataDoc = scDSRequest.convertDocument(scDocument, genAppSchema);
		DataDocLogger dataDocLogger = new DataDocLogger(appLogger);
		dataDocLogger.writeSimple(dataDoc);

// Perform the I/O operation.

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataLoader dataLoader = new DataLoader(appCtx);
		String dsTitle = dataDoc.getValueByName("ds_title");
		String dataStructure = dataDoc.getValueByName("ds_structure");
		ApplicationGenerator appGenerator = new ApplicationGenerator(sessionContext, dataDoc);
		if (StringUtils.equals(dataStructure, Constants.DS_DATA_STRUCTURE_HIERARCHY_NAME))
		{
			if (dataLoader.isDataHierarchyAGraph(dataStructure, dsTitle))
			{
				GraphDS graphDS = dataLoader.getGraphDSFromStorageFile(dataStructure, dsTitle);
				appGenerator.saveDataGraph(graphDS);
			}
			else if (dataLoader.isDataHierarchyJSON(dataStructure, dsTitle))
			{
				JsonDS jsonDS = dataLoader.getJsonDSFromStorageFile(dataStructure, dsTitle);
				appGenerator.saveDataJson(jsonDS);
			}
			else
				throw new DSException(String.format("Unsupported data structure: '%s' and '%s'", dataStructure, dsTitle));
		}
		else
		{
			gridDS = dataLoader.getGridDSFromStorageFile(dataStructure, dsTitle);
			appGenerator.saveDataFlat(gridDS.getSchema());
		}
		stopWatch.stop();

		boolean isOK = StringUtils.isNoneEmpty(dataDoc.getValueByName("gen_link"));
		appLogger.debug(String.format("'%s': Generated %s '%s' [%s] in %d milliseconds.", scDS.getName(),
									  dataDoc.getValueByName("app_type"), dataDoc.getValueByName("app_name"),
									  dataDoc.getValueByName("ds_title"), stopWatch.getTime()));
		dataDocLogger.writeSimple(dataDoc);

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(dataDoc, isOK);

// Reset our session to avoid conflicts with newly created application.

		sessionContext.resetSession();;

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
		return executeAdd(aRequest);
	}
}
