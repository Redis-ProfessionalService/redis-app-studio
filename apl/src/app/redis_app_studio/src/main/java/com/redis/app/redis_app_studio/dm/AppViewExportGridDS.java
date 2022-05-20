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

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DSResponse;
import com.isomorphic.datasource.DataSource;
import com.isomorphic.rpc.RPCManager;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_grid.GridDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterionEntry;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.io.DataDocJSON;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridCSV;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings("FieldCanBeLocal")
public class AppViewExportGridDS
{
	private final String CLASS_NAME = "AppViewExportGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "dm";

	private int writeCSV(AppCtx anAppCtx, DataGrid aDataGrid, String aFormat, ServletOutputStream anOS)
		throws IOException
	{
		Logger appLogger = anAppCtx.getLogger(this, "writeCSV");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS);
		DataGridCSV dataGridCSV = new DataGridCSV();
		dataGridCSV.save(aDataGrid, outputStreamWriter, true, aFormat.toLowerCase().equals("title"));

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return DSResponse.STATUS_SUCCESS;
	}

	private int writeJSON(AppCtx anAppCtx, DataGrid aDataGrid, ServletOutputStream anOS)
		throws IOException
	{
		Logger appLogger = anAppCtx.getLogger(this, "writeJSON");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		List<DataDoc> dataDocList = new ArrayList<>();
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
			dataDocList.add(aDataGrid.getRowAsDoc(row));
		DataDocJSON dataDocJSON = new DataDocJSON();
		dataDocJSON.save(anOS, dataDocList);

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return DSResponse.STATUS_SUCCESS;
	}

	private int writeSchema(AppCtx anAppCtx, DataGrid aDataGrid, ServletOutputStream anOS)
		throws IOException
	{
		Logger appLogger = anAppCtx.getLogger(this, "writeSchema");
		appLogger.trace(anAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = aDataGrid.getColumns();
		appLogger.debug(String.format("'%s': %d columns in schema document.", aDataGrid.getName(), schemaDoc.count()));
		DataDocXML dataDocXML = new DataDocXML(schemaDoc);
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);
		dataDocXML.save(printWriter);
		anOS.write(stringWriter.toString().getBytes(StandardCharsets.UTF_8));

		appLogger.trace(anAppCtx.LOGMSG_TRACE_DEPART);

		return DSResponse.STATUS_SUCCESS;
	}

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
	 * SmartClient invoked endpoint responsible for the exporting of data
	 * from the grid.
	 *
	 * @param aRequest SmartClient data source request
	 * @param aRPCManager Smartclient RPC manager instance
	 *
	 * @throws Exception Processing exception
	 */
	public void exportData(DSRequest aRequest, RPCManager aRPCManager)
		throws Exception
	{
		GridDS gridDS;
		int statusCode;
		DataGrid dataGrid;
		String criteriaName;

		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		DSCriteria dsCriteria = scDSRequest.convertDSCriteria(aRequest);

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "exportData");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
		criteriaName = dsCriteria.getName();
		dsCriteria.setName(criteriaName + " - SmartClient");
		dsCriteriaLogger.writeFull(dsCriteria);

// Create/establish our application session.

		AppSession appSession = new AppSession.Builder().context(sessionContext).targetMemory().criteria(dsCriteria).build();
		AppResource appResource = appSession.establish();

// Calculate our fetch values.

		String fetchPolicy = DS.fetchPolicyFromCriteria(dsCriteria);
		int fetchRowStart = DS.offsetFromCriteria(dsCriteria);
		int fetchRowLimit = DS.limitFromCriteria(dsCriteria);
		appLogger.debug(String.format("'%s' (criteria) - [%s]: fetchRowStart = %d, fetchRowLimit = %d", dsCriteria.getName(),
									  fetchPolicy, fetchRowStart, fetchRowLimit));
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

		assignDataFormats(appCtx, gridDS.getSchema(), dsCriteria);
		dsCriteria.setName(criteriaName + " - Redis App Studio");
		dsCriteriaLogger.writeFull(dsCriteria);

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		if (StringUtils.isNotEmpty(searchTerms))
		{
			dataGrid = gridDS.search(searchTerms, fetchRowStart, gridDS.count());
			if (dataGrid.rowCount() > 0)
			{
				dataGrid.clearFeatures();
				gridDS = new GridDS(appCtx);
				gridDS.setDatGrid(dataGrid);
				dataGrid = gridDS.fetch(dsCriteria);
			}
		}
		else
			dataGrid = gridDS.fetch(dsCriteria, fetchRowStart, gridDS.count());
		stopWatch.stop();

		int rowCount = dataGrid.rowCount();
		appLogger.debug(String.format("'%s': %d rows fetched in %d milliseconds.", dsCriteria.getName(),
									  rowCount, stopWatch.getTime()));

		try
		{
			aRPCManager.doCustomResponse();
			HttpServletResponse response = aRPCManager.getContext().response;

			ServletOutputStream servletOutputStream = response.getOutputStream();
			String exportAction = DS.actionFromCriteria(dsCriteria);
			if (StringUtils.equals(exportAction, "grid_export_by_criteria_csv"))
			{
				response.setHeader("content-disposition", "attachment; filename=RAS-Grid.csv");
				response.setContentType("text/csv");
				String csvFormat = DS.formatFromCriteria(dsCriteria);
				statusCode = writeCSV(appCtx, dataGrid, csvFormat, servletOutputStream);
			}
			else if (StringUtils.equals(exportAction, "grid_export_by_criteria_json"))
			{
				response.setHeader("content-disposition", "attachment; filename=RAS-Grid.json");
				response.setContentType("application/json");
				statusCode = writeJSON(appCtx, dataGrid, servletOutputStream);
			}
			else if (StringUtils.equals(exportAction, "schema_export_xml"))
			{
				response.setHeader("content-disposition", "attachment; filename=RAS-Schema.xml");
				response.setContentType("text/xml");
				statusCode = writeSchema(appCtx, dataGrid, servletOutputStream);
			}
			else
				statusCode = DSResponse.STATUS_FAILURE;
			DSResponse dsResponse = new DSResponse();
			dsResponse.setStatus(statusCode);
			aRPCManager.send(aRequest, dsResponse);
		}
		catch (Exception e)	// Logic from SC sample
		{
			try
			{
				aRPCManager.sendFailure(aRequest, e.getMessage());
			}
			catch(Exception r)
			{
				appLogger.error(r.getMessage());
			}
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}
}
