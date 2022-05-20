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
import com.isomorphic.servlet.ISCFileItem;
import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_content.ContentType;
import com.redis.ds.ds_grid.GridDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.io.DataGridCSV;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.util.Date;
import java.util.Map;

/**
 * SmartClient data source class focused on managing fetch, add, update
 * and delete operations for the App Studio client application.
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Server DataSource Integration</a>
 */
@SuppressWarnings({"unchecked", "rawtypes", "FieldCanBeLocal"})
public class DocumentGridDS extends BasicDataSource
{
	private final String CLASS_NAME = "DocumentGridDS";
	private final String APPLICATION_PROPERTIES_PREFIX = "rs";

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

// We use a CSV file to manage the document list in a folder.

		DataLoader dataLoader = new DataLoader(appCtx, Constants.DS_DOCUMENTS_PATH_NAME);
		String storageCSVPathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_DOCUMENTS_PATH_NAME, Constants.DS_STORAGE_DETAILS_NAME);
		GridDS gridDS = new GridDS(appCtx);
		gridDS.loadData(storageCSVPathFileName, true);
		gridDS.getSchema().getItemByName(Constants.DS_STORAGE_DOCUMENT_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);

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

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataGrid dataGrid = gridDS.fetch(dsCriteria, fetchRowStart, fetchRowLimit);
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
	 * Executes a SmartClient data source add operation based on a document.
	 *
	 * @param aRequest SmartClient data source request instance
	 *
	 * @return SmartClient data source response instance
	 *
	 * @throws Exception Signifying operation failure
	 */
	@SuppressWarnings({"WrapperTypeMayBePrimitive", "UnnecessaryLocalVariable"})
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

// We use a CSV file to manage the document list in a folder.

		DataLoader dataLoader = new DataLoader(appCtx, Constants.DS_DOCUMENTS_PATH_NAME);
		String storageCSVPathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_DOCUMENTS_PATH_NAME, Constants.DS_STORAGE_DETAILS_NAME);
		GridDS gridDS = new GridDS(appCtx);
		gridDS.loadData(storageCSVPathFileName, true);
		gridDS.getSchema().getItemByName(Constants.DS_STORAGE_DOCUMENT_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);
		DataGrid dataGrid = gridDS.getDataGrid();
		ContentType contentType = sessionContext.getContentType();

// Preparing to upload the file to local document storage.

		DataDoc dataDoc = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
		final ISCFileItem fileItem = aRequest.getUploadedFile(Constants.DS_STORAGE_DOCUMENT_FILE);
		String fileName = fileItem.getShortFileName();

		String pathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_DOCUMENTS_PATH_NAME, fileName);
		appLogger.debug(String.format("Upload destination: %s", pathFileName));
		long fileSize = fileItem.getSize();

// Perform the I/O operation.

		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		File docFile = new File(pathFileName);
		fileItem.write(docFile);
		stopWatch.stop();

		boolean isOK = docFile.exists();
		appLogger.debug(String.format("'%s': Saved '%s' (%d bytes) to 'documents' folder in %d milliseconds.", scDS.getName(),
									  fileName, fileSize, stopWatch.getTime()));

// Populate our data document, add it to the folder tracking file and save it.

		if (isOK)
		{
			dataDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, fileName);
			if (contentType != null)
				dataDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE, contentType.nameByFileExtension(fileName));
			dataDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_DATE, new Date());
			dataDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_SIZE, fileSize);
			isOK = dataGrid.addRow(dataDoc);
			DataGridCSV dataGridCSV = new DataGridCSV();
			dataGridCSV.save(dataGrid, storageCSVPathFileName, true);
		}

// Create our data source response.

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(dataDoc, isOK);

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
		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeUpdate");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// We use a CSV file to manage the document list in a folder.

		DataLoader dataLoader = new DataLoader(appCtx, Constants.DS_DOCUMENTS_PATH_NAME);
		String storageCSVPathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_DOCUMENTS_PATH_NAME, Constants.DS_STORAGE_DETAILS_NAME);
		GridDS gridDS = new GridDS(appCtx);
		gridDS.loadData(storageCSVPathFileName, true);
		gridDS.getSchema().getItemByName(Constants.DS_STORAGE_DOCUMENT_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);

// The SmartClient grid widget will only send us a subset of changed items,
// so we need to load the current version of the document and apply the
// changes.

		DataDoc dataDoc1 = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		DataDoc dataDoc2 = gridDS.loadApplyUpdate(dataDoc1);
		stopWatch.stop();
		appLogger.debug(String.format("'%s': %d items updated in %d milliseconds.", scDS.getName(),
									  dataDoc1.count(), stopWatch.getTime()));

// Save our in-memory grid to local storage.

		DataGrid dataGrid = gridDS.getDataGrid();
		DataGridCSV dataGridCSV = new DataGridCSV();
		dataGridCSV.save(dataGrid, storageCSVPathFileName, true);

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(true);
		DSResponse dsResponse = scdsResponse.create(dataDoc2, true);

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
		SCDSRequest scDSRequest = new SCDSRequest();
		DataSource scDS = aRequest.getDataSource();
		Map scDocument = aRequest.getValues();

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest.getHttpServletRequest());
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "executeRemove");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// We use a CSV file to manage the document list in a folder.

		DataLoader dataLoader = new DataLoader(appCtx, Constants.DS_DOCUMENTS_PATH_NAME);
		String storageCSVPathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_DOCUMENTS_PATH_NAME, Constants.DS_STORAGE_DETAILS_NAME);
		GridDS gridDS = new GridDS(appCtx);
		gridDS.loadData(storageCSVPathFileName, true);
		gridDS.getSchema().getItemByName(Constants.DS_STORAGE_DOCUMENT_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);

		DataDoc dataDoc = scDSRequest.convertDocument(scDocument, gridDS.getSchema());
		String fileName = dataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME);
		String pathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_DOCUMENTS_PATH_NAME, fileName);
		File pathFile = new File(pathFileName);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		boolean isOK = gridDS.delete(dataDoc);
		if (isOK)
			isOK = pathFile.delete();
		stopWatch.stop();
		appLogger.debug(String.format("'%s': 1 row deleted in %d milliseconds.", scDS.getName(),
									  stopWatch.getTime()));

// Save our in-memory grid to local storage.

		if (isOK)
		{
			DataGrid dataGrid = gridDS.getDataGrid();
			DataGridCSV dataGridCSV = new DataGridCSV();
			dataGridCSV.save(dataGrid, storageCSVPathFileName, true);
		}

		SCDSResponse scdsResponse = new SCDSResponse(appCtx, scDS);
		scdsResponse.setDebugFlag(false);
		DSResponse dsResponse = scdsResponse.create(scDocument, isOK);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsResponse;
	}
}
