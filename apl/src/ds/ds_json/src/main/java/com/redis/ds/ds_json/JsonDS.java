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

package com.redis.ds.ds_json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.redis.ds.ds_grid.GridDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.io.DataDocJSON;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * The JSON data source manages a row x column grid of
 * <i>DataItem</i> cells in memory using JSON path expressions
 * to populate them.  It implements the five CRUD+S methods
 * and supports advanced queries using a <i>DSCriteria</i>.
 * In addition, this data source offers save and load methods for
 * data values.  This can be a useful data source if your grid
 * size is small in nature and there is sufficient heap space
 * available to load it.
 *
 * JSON Path Notes:
 * 1) JSON Path query expressions are not supported - use
 *    the built-in grid criteria query features.
 * 2) Form your JSON Path expressions for each individual
 *    JSON object since the parent array is stripped during
 *    the loading sequence.
 * 3) Data items can handle multi-value assignments, but
 *    this package does not support arrays of objects
 *    (e.g. child DotaDoc) since the data is being
 *    collapsed into a grid.
 *
 * @see <a href="https://github.com/json-path/JsonPath">Java JSON Path Source Code</a>
 * @see <a href="https://www.baeldung.com/guide-to-jayway-jsonpath">Java JSON Path Tutorial</a>
 * @see <a href="https://jsonpath.herokuapp.com/">JSON Interactive Expression Evaluator</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class JsonDS extends GridDS
{
	private boolean mCaptureErrors;
	final private DataGrid mMessagesGrid;

	/**
     * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	*/
	public JsonDS(AppCtx anAppCtx)
	{
		super(anAppCtx);

		mMessagesGrid = new DataGrid(messageSchemaDoc());
	}

	/**
	 * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	 * @param aCaptureErrors If <i>true</i>, then JSON path evaluation errors will be captured during parsing.
	 */
	public JsonDS(AppCtx anAppCtx, boolean aCaptureErrors)
	{
		super(anAppCtx);

		mCaptureErrors = aCaptureErrors;
		mMessagesGrid = new DataGrid(messageSchemaDoc());
	}

	private DataDoc messageSchemaDoc()
	{
		DataDoc msgSchemaDoc = new DataDoc("Messages Grid");
		msgSchemaDoc.add(new DataItem.Builder().name("document_id").title("Document Id").build());
		msgSchemaDoc.add(new DataItem.Builder().name("message_type").title("Type").build());
		msgSchemaDoc.add(new DataItem.Builder().name("message_info").title("Message").build());
		msgSchemaDoc.add(new DataItem.Builder().name("json_path").title("JSON Path").build());
		msgSchemaDoc.add(new DataItem.Builder().name("json_document").title("JSON Document").build());

		return msgSchemaDoc;
	}

	/**
	 * Assign JSON path evaluation error tracking flag.
	 *
	 * @param aCaptureErrors If <i>true</i>, then JSON path evaluation errors will be captured during parsing.
	 */
	public void setErrorTrackingFlag(boolean aCaptureErrors)
	{
		mCaptureErrors = aCaptureErrors;
	}

	/**
	 * Clears (empties rows) the JSON parsing error message tracking grid.
	 */
	public void clearMessages()
	{
		mMessagesGrid.emptyRows();
	}

	/**
	 * Identifies if errors or warnings were detected while parsing the JSON
	 * file.
	 *
	 * @return <i>true</i> if parsing JSON path issues were discovered and
	 * <i>false</i> otherwise.
	 */
	public boolean isMessagesAssigned()
	{
		return mMessagesGrid.rowCount() > 0;
	}

	/**
	 * Returns the internally managed error/warning data grid.
	 *
	 * @return Data grid instance
	 */
	public DataGrid getMessagesGrid()
	{
		return mMessagesGrid;
	}

	/**
	 * Parses the JSON file while evaluating any JSON path expressions
	 * assigned as item features in the schema document.  Each document
	 * that is parsed is added to the grid data source.
	 *
	 * If any parsing or JSON path expression errors/warnings are
	 * detected, they will be added to the message grid.
	 *
	 * @param aDataDocListDocList A list of parsed JSON documents as data document instances
	 *
	 * @return <i>true</i> if parsing JSON path issues were discovered and <i>false</i> otherwise.
	 *
	 * @throws IOException I/O exception
	 */
	@SuppressWarnings("PatternVariableCanBeUsed")
	public boolean loadDataEvaluatePath(List<DataDoc> aDataDocListDocList)
		throws IOException
	{
		DataDoc msgDoc;
		Object jsonObject;
		Object jsonDocument;
		Optional<DataDoc> optDataDoc;
		String jsonPath, jsonString, jsonDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadDataEvaluatePath");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int docId = 0;
		clearMessages();
		DataDoc jsonSchemaDoc = getSchema();
		DataDocJSON dataDocJSON = new DataDocJSON();
		for (DataDoc dataDoc : aDataDocListDocList)
		{
			docId++;
			jsonDoc = dataDocJSON.saveAsAString(dataDoc);
			jsonDocument = Configuration.defaultConfiguration().jsonProvider().parse(jsonDoc);
			for (DataItem dataItem : jsonSchemaDoc.getItems())
			{
				if (dataItem.isFeatureAssigned(Data.FEATURE_JSON_PATH))
					jsonPath = dataItem.getFeature(Data.FEATURE_JSON_PATH);
				else
				{
					jsonPath = String.format("$.%s", dataItem.getName());
					dataItem.addFeature(Data.FEATURE_JSON_PATH, jsonPath);
				}
				try
				{
					jsonObject = JsonPath.read(jsonDocument, jsonPath);
				}
				catch (Exception e)
				{
					if (mCaptureErrors)
					{
						msgDoc = messageSchemaDoc();
						msgDoc.setValueByName("document_id", docId);
						msgDoc.setValueByName("message_type", "ERROR");
						msgDoc.setValueByName("message_info", e.getMessage());
						msgDoc.setValueByName("json_path", jsonPath);
						msgDoc.setValueByName("json_document", jsonDoc);
						mMessagesGrid.addRow(msgDoc);
					}
					continue;
				}
				jsonString = jsonObject.toString();
				if (StringUtils.startsWith(jsonString, "["))
				{
					if ((mCaptureErrors) && (StringUtils.startsWith(jsonString, "[{")))
					{
						msgDoc = messageSchemaDoc();
						msgDoc.setValueByName("document_id", docId);
						msgDoc.setValueByName("message_type", "ERROR");
						msgDoc.setValueByName("message_info", String.format("'%s' is an array of objects - cannot add to a grid row.", jsonString));
						msgDoc.setValueByName("json_path", jsonPath);
						msgDoc.setValueByName("json_document", jsonDoc);
						mMessagesGrid.addRow(msgDoc);
					}
					else
					{
						String itemJSONString = String.format("{ \"%s\": %s }", dataItem.getName(), jsonString);
						optDataDoc = dataDocJSON.loadFromString(itemJSONString);
						if (optDataDoc.isPresent())
						{
							DataDoc tmpDataDoc = optDataDoc.get();
							dataItem.setValues(tmpDataDoc.getValuesByName(dataItem.getName()));
						}
					}
				}
				else if ((jsonObject instanceof Integer) && (dataItem.getType() == Data.Type.Integer))
				{
					Integer jsonValue = (Integer) jsonObject;
					dataItem.setValue(jsonValue);
				}
				else if ((jsonObject instanceof Double) && (dataItem.getType() == Data.Type.Double))
				{
					Double jsonValue = (Double) jsonObject;
					dataItem.setValue(jsonValue);
				}
				else
					dataItem.setValue(jsonString);
			}
			mDataGrid.addRow(dataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return !isMessagesAssigned();
	}

	/**
	 * Parses the JSON file while evaluating any JSON path expressions
	 * assigned as item features in the schema document.  Each document
	 * that is parsed is added to the grid data source.
	 *
	 * If any parsing or JSON path expression errors/warnings are
	 * detected, they will be added to the message grid.
	 *
	 * @param aPathFileName JSON path/file name
	 *
	 * @return <i>true</i> if parsing JSON path issues were discovered and
	 * 	 * <i>false</i> otherwise.
	 *
	 * @throws IOException I/O exception
	 */
	public boolean loadDataEvaluatePath(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadDataEvaluatePath");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		File jsonFile = new File(aPathFileName);
		if (! jsonFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		DataDocJSON dataDocJSON = new DataDocJSON();
		List<DataDoc> dataDocList = dataDocJSON.loadList(aPathFileName);
		int docListSize = dataDocList.size();
		appLogger.debug(String.format("%s: contained %d data documents within it.", aPathFileName, docListSize));
		boolean isOK = loadDataEvaluatePath(dataDocList);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Uses the Data Document JSON parsing interfaces to load the document
	 * and item information into the grid data source.
	 *
	 * @param aPathFileName JSON path/file name
	 *
	 * @throws IOException I/O exception
	 */
	public void loadData(String aPathFileName)
		throws IOException
	{
		DataDoc rowDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadData");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		clearMessages();
		File jsonFile = new File(aPathFileName);
		if (!jsonFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		DataDoc jsonSchemaDoc = getSchema();
		DataDocJSON dataDocJSON = new DataDocJSON();
		List<DataDoc> dataDocList = dataDocJSON.loadList(aPathFileName);
		for (DataDoc jsonDataDoc : dataDocList)
		{
			rowDataDoc = new DataDoc(jsonSchemaDoc);
			for (DataItem jsonDataItem : jsonDataDoc.getItems())
				rowDataDoc.setValuesByName(jsonDataItem.getName(), jsonDataItem.getValues());
			mDataGrid.addRow(rowDataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
