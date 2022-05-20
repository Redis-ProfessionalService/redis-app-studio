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

package com.redis.ds.ds_redis.json;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DS;
import com.redis.foundation.io.DataDocJSON;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.xml.sax.SAXException;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.json.JsonSetParams;
import redis.clients.jedis.json.Path2;
import org.json.JSONArray;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * The RedisJson class is responsible for accessing the RedisGraph
 * module commands via the Jedis programmatic library.  It designed to
 * simplify the use of core Foundation class objects like items,
 * documents and grids.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * Note: This RedisJSON DS class assumes that it will be coupled
 *       with RediSearch DS class for data grid management.  In
 *       this scenario, each data document will be stored as a
 *       key/value (JSON document) in the database and RediSearch
 *       will be used to locate them.
 *
 * @see <a href="https://oss.redis.com/redisjson/">OSS RedisJSON</a>
 * @see <a href="https://github.com/RedisJSON/JRedisJSON/">RedisJSON Java Client</a>
 * @see <a href="https://github.com/stleary/JSON-java">OSS Java-to-JSON-to-Java</a>
 * @see <a href="https://www.baeldung.com/java-org-json">JSON-Java Tutorial</a>
 * @see <a href="https://github.com/redis/jedis">Java Redis</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisJson
{
	public final String CFG_JSON_PREFIX = "redis.json";

	private DataDoc mDataSchema;
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private final RedisKey mRedisKey;
	private final UnifiedJedis mCmdConnection;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the JSON objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisJson(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mRedisDS.setEncryptionOption(Redis.Encryption.None);
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
	}

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the JSON objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 * @param aDataSchemaDoc Data schema data document
	 */
	public RedisJson(RedisDS aRedisDS, DataDoc aDataSchemaDoc)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mRedisDS.setEncryptionOption(Redis.Encryption.None);
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
		setDataSchema(aDataSchemaDoc);
	}

	/**
	 * Returns a Redis unified command connection instance.
	 *
	 * @return Unified command connection instance
	 */
	public UnifiedJedis getCmdConnection()
	{
		return mCmdConnection;
	}

	/**
	 * Assigns the data document instance as the schema definition.
	 * The data schema definition is tracked in this package as a
	 * convenience to the calling application and used when the
	 * parent application needs to add, update or delete documents
	 * in the search index.
	 *
	 * @param aSchemaDoc Data document instance
	 */
	public void setDataSchema(DataDoc aSchemaDoc)
	{
		mDataSchema = aSchemaDoc;
	}

	/**
	 * Retrieves the internal data schema data document definition.
	 *
	 * @return Data schema data document definition
	 */
	public DataDoc getDataSchema()
	{
		return mDataSchema;
	}

	/**
	 * Identifies if the feature name is standard to the search
	 * data source package.
	 *
	 * @param aName Name of the feature
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureStandard(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
		{
			switch (aName)
			{
				case Data.FEATURE_IS_PRIMARY:
				case Data.FEATURE_IS_REQUIRED:
				case Data.FEATURE_IS_VISIBLE:
				case Data.FEATURE_IS_SEARCH:
				case Data.FEATURE_IS_SUGGEST:
					return true;
			}
		}

		return false;
	}

	/**
	 * Creates a data document with items suitable for a schema editor UI.
	 *
	 * @param aName Name of the schema
	 *
	 * @return Data document representing a schema
	 */
	public DataDoc createSchemaDoc(String aName)
	{
		DataDoc schemaDoc = new DataDoc(aName);
		schemaDoc.add(new DataItem.Builder().name("item_name").title("Item Name").build());
		schemaDoc.add(new DataItem.Builder().name("item_type").title("Item Type").build());
		schemaDoc.add(new DataItem.Builder().name("item_title").title("Item Title").build());
		schemaDoc.add(new DataItem.Builder().name("json_path").title("JSON Path").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_PRIMARY).title("Is Primary").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_REQUIRED).title("Is Required").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_VISIBLE).title("Is Visible").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_SEARCH).title("Is Search").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_SUGGEST).title("Is Suggest").build());

		return schemaDoc;
	}

	/**
	 * Convert a data document schema definition into a data grid suitable for
	 * rendering in a schema editor UI.
	 *
	 * @param aSchemaDoc Data document instance (representing the schema)
	 * @param anIsExtended If <i>true</i>, then non standard features will be recognized
	 *
	 * @return Data grid representing the schema definition
	 */
	public DataGrid schemaDocToDataGrid(DataDoc aSchemaDoc, boolean anIsExtended)
	{
		HashMap<String,String> mapFeatures;

// Create our initial data grid schema based on standard item info plus features.

		String schemaName = String.format("%s Schema", aSchemaDoc.getName());
		DataDoc schemaDoc = createSchemaDoc(schemaName);
		DataGrid dataGrid = new DataGrid(schemaDoc);

// Extend the data grid schema for user defined features.

		if (anIsExtended)
		{
			Data.Type featureType;
			String featureKey, featureValue, featureTitle;

			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				mapFeatures = dataItem.getFeatures();
				for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				{
					featureKey = featureEntry.getKey();
					if (! isFeatureStandard(featureKey))
					{
						if (StringUtils.startsWith(featureKey, "is"))
							featureType = Data.Type.Boolean;
						else
						{
							featureValue = featureEntry.getValue();
							if (NumberUtils.isParsable(featureValue))
							{
								int offset = featureValue.indexOf(StrUtl.CHAR_DOT);
								if (offset == -1)
									featureType = Data.Type.Integer;
								else
									featureType = Data.Type.Float;
							}
							else
								featureType = Data.Type.Text;
						}
						featureTitle = Data.nameToTitle(featureKey);
						dataGrid.addCol(new DataItem.Builder().type(featureType).name(featureKey).title(featureTitle).build());
					}
				}
			}
		}

// Populate each row of the data grid based on the schema data document.

		for (DataItem dataItem : aSchemaDoc.getItems())
		{
			dataGrid.newRow();
			dataGrid.setValueByName("item_name", dataItem.getName());
			dataGrid.setValueByName("item_type", Data.typeToString(dataItem.getType()));
			dataGrid.setValueByName("item_title", dataItem.getTitle());
			dataGrid.setValueByName("json_path", dataItem.getFeature(Data.FEATURE_JSON_PATH));
			mapFeatures = dataItem.getFeatures();
			for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				dataGrid.setValueByName(featureEntry.getKey(), featureEntry.getValue());
			dataGrid.addRow();
		}

		return dataGrid;
	}

	/**
	 * Collapses a data grid representing a schema definition back into a
	 * data document schema.  This method assumes that you invoked the
	 * schemaDocToDataGrid() method to build the data grid originally.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Data document schema definition
	 */
	public DataDoc dataGridToSchemaDoc(DataGrid aDataGrid)
	{
		DataDoc dataDoc;
		DataItem schemaItem;
		String itemName, itemType, itemTitle;

		DataDoc schemaDoc = new DataDoc(aDataGrid.getName());
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			dataDoc = aDataGrid.getRowAsDoc(row);
			itemName = dataDoc.getValueByName("item_name");
			itemType = dataDoc.getValueByName("item_type");
			itemTitle = dataDoc.getValueByName("item_title");
			if ((StringUtils.isNotEmpty(itemName)) && (StringUtils.isNotEmpty(itemType)) && (StringUtils.isNotEmpty(itemTitle)))
			{
				schemaItem = new DataItem.Builder().type(Data.stringToType(itemType)).name(itemName).title(itemTitle).build();
				for (DataItem dataItem : dataDoc.getItems())
				{
					if (! StringUtils.startsWith(dataItem.getName(), "item_"))
						dataItem.addFeature(dataItem.getName(), dataItem.getValue());
				}
				schemaDoc.add(schemaItem);
			}
		}

		return schemaDoc;
	}

	/**
	 * Returns the key prefix string associated with the schema.
	 *
	 * @return Hash prefix string
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public String keyPrefixFromSchema()
		throws RedisDSException
	{
		if (StringUtils.isNotEmpty(mRedisDS.getApplicationPrefix()))
			return String.format("%s:", mRedisDS.getApplicationPrefix());
		else
			return CFG_JSON_PREFIX;
	}

	/**
	 * Generates and returns a unique key name for the JSON schema
	 * that can be used to store/retrieve the definition in Redis.
	 *
	 * @return Search schema key name
	 */
	public String getSchemaKeyName()
	{
		return mRedisKey.moduleJson().redisJsonSchema().dataObject(mDataSchema).name();
	}

	private String columnSchemaToString(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "columnSchemaToString");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = aDataGrid.getColumns();
		schemaDoc.resetValues();
		DataDocXML dataDocXML = new DataDocXML(schemaDoc);
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);
		try
		{
			dataDocXML.save(printWriter);
		}
		catch (IOException e)
		{
			throw new RedisDSException(e.getMessage());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return stringWriter.toString();
	}

	private DataDoc stringToColumnSchema(String aSchemaString)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "stringToColumnSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDocXML dataDocXML = new DataDocXML();
		try
		{
			InputStream inputStream = IOUtils.toInputStream(aSchemaString, StrUtl.CHARSET_UTF_8);
			dataDocXML.load(inputStream);
		}
		catch (ParserConfigurationException | SAXException | IOException e)
		{
			throw new RedisDSException(e.getMessage());
		}
		DataDoc schemaDoc = dataDocXML.getDataDoc();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return schemaDoc;
	}

	/**
	 * Updates the data items captured in the <i>DataDoc</i>
	 * against the schema.  The data items must be derived
	 * from the schema definition.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aSchemaDoc Data document instance
	 *
	 * @return <i>true</i> if schema needed to be updated and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source related exception
	 */
	public boolean updateSchema(DataGrid aDataGrid, DataDoc aSchemaDoc)
		throws RedisDSException
	{
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "updateSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		boolean isOK = false;
		DataDoc schemaDoc = aDataGrid.getColumns();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aSchemaDoc.getValueByName("item_name"));
		if (optDataItem.isPresent())
		{
			DataItem schemaItem = optDataItem.get();
			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				itemName = dataItem.getName();
				if (itemName.equals("item_title"))
				{
					schemaItem.setTitle(dataItem.getValue());
					if (! isOK) isOK = true;
				}
				else if (! itemName.startsWith("item_"))
				{
					schemaItem.addFeature(dataItem.getName(), dataItem.getValue());
					if (! isOK) isOK = true;
				}
			}
		}

// If OK, then we can persist the updated schema in the database.

		if (isOK)
		{
			// First entry in the sorted set will always be a string representing the DataGrid schema.
			String gridKeyName = mRedisKey.moduleJson().redisJsonDocument().dataObject(aDataGrid).name();
			String keyValue = columnSchemaToString(aDataGrid);
			mCmdConnection.zadd(gridKeyName, 0, keyValue);
			mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), 0, mRedisDS.escapeValue(keyValue)));
			mRedisDS.createCore().expire(gridKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	private JSONObject dataDocToJSONObject(DataDoc aDataDoc)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "dataDocToJSONObject");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDocJSON dataDocJSON = new DataDocJSON();
		String jsonString = dataDocJSON.saveAsAString(aDataDoc);
		JSONObject jsonObject = new JSONObject(jsonString);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return jsonObject;
	}

	/**
	 * Converts a JSONObject instance to a data document instance.
	 *
	 * @param aJSONObject JSON object instance
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> jsonObjectToDataDoc(JSONObject aJSONObject)
	{
		Logger appLogger = mAppCtx.getLogger(this, "jsonObjectToDataDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataDoc> optDataDoc = Optional.empty();
		if (aJSONObject != null)
		{
			DataDocJSON dataDocJSON = new DataDocJSON();
			optDataDoc = dataDocJSON.loadFromString(aJSONObject.toString());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	private void ensurePreconditions(boolean aIsSchemaDefined)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "ensurePreconditions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if ((aIsSchemaDefined) && (mDataSchema == null))
			throw new RedisDSException("Data schema has not been defined.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private DataItem shadowDataItem(DataItem aDataItem, Date aDate)
	{
		DataItem dataItem = new DataItem(Data.Type.Long, Redis.shadowFieldName(aDataItem.getName()));
		dataItem.setValue(aDate.getTime());

		return dataItem;
	}

	private DataDoc enrichDataDoc(DataDoc aDataDoc)
		throws RedisDSException
	{
		Date itemDate;
		DataItem shadowDataItem;
		DataDoc enrichedDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "enrichDataDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataDoc == null)
			throw new RedisDSException("Data document is null.");

		enrichedDataDoc = new DataDoc(aDataDoc);
		for (DataItem dataItem : aDataDoc.getItems())
		{
			if (dataItem.isValueAssigned())
			{
				if (Data.isDateOrTime(dataItem.getType()))
				{
					itemDate = dataItem.getValueAsDate();
					if (itemDate == null)
						appLogger.error(String.format("%s: Unable to parse '%s' format of '%s'", dataItem.getName(),
													  dataItem.getValue(), dataItem.getDataFormat()));
					else
					{
						shadowDataItem = shadowDataItem(dataItem, itemDate);
						enrichedDataDoc.add(shadowDataItem);
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return enrichedDataDoc;
	}

	private void add(DataDoc aDataDoc, Pipeline aPipeline)
		throws RedisDSException
	{
		String keyName;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		if ((aDataDoc == null) || (aDataDoc.count() == 0))
			throw new RedisDSException("Data document is null or emtpy - cannot add it to Redis.");
		try
		{
			JSONObject jsonObject = dataDocToJSONObject(enrichDataDoc(aDataDoc));
			Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
			if (optDataItem.isPresent())
			{
				DataItem dataItem = optDataItem.get();
				if (dataItem.isValueAssigned())
					keyName = mRedisKey.moduleJson().redisJsonDocument().dataObject(aDataDoc).primaryId(dataItem.getValue()).name();
				else
					keyName = mRedisKey.moduleJson().redisJsonDocument().dataObject(aDataDoc).name();
			}
			else
				keyName = mRedisKey.moduleJson().redisJsonDocument().dataObject(aDataDoc).name();
			if (aPipeline != null)
				aPipeline.jsonSet(keyName, jsonObject);
			else
				mCmdConnection.jsonSet(keyName, jsonObject);
			aDataDoc.addFeature(Redis.FEATURE_KEY_NAME, keyName);
			String redisCommand = String.format("JSON.SET %s %s '%s'", mRedisDS.escapeKey(keyName), Redis.JSON_PATH_ROOT, jsonObject.toString());
			mRedisDS.saveCommand(appLogger, redisCommand);
		}
		catch (IOException e)
		{
			String errMsg = String.format("%s: %s", aDataDoc.getName(), e.getMessage());
			appLogger.error(errMsg);
			throw new RedisDSException(errMsg);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds a data document as a JSON document to the Redis database.
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataDoc aDataDoc)
		throws RedisDSException
	{
		add(aDataDoc, null);
	}

	/**
	 * Adds a list of data documents (as a JSON documents) to the Redis database.
	 *
	 * @param aDataDocList List of data documents
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(List<DataDoc> aDataDocList)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int multiCount = 0;
		Pipeline commandPipeline = null;
		if (aDataDocList != null)
		{
			for (DataDoc dataDoc : aDataDocList)
			{
				if (multiCount == 0)
				{
					commandPipeline = new Pipeline(mRedisDS.getCmdConnection());
					mRedisDS.saveCommand(appLogger, "PIPELINE");
				}
				add(dataDoc, commandPipeline);
				multiCount++;
				if (multiCount >= Redis.PIPELINE_BATCH_COUNT)
				{
					commandPipeline.sync();
					commandPipeline = null;
					mRedisDS.saveCommand(appLogger, "SYNC");
					multiCount = 0;
				}
			}
			if (multiCount > 0)
			{
				commandPipeline.sync();
				mRedisDS.saveCommand(appLogger, "SYNC");
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the rows of data documents as JSON documents in the Redis database.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataGrid aDataGrid)
		throws RedisDSException
	{
		String keyName;
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		DataDoc schemaDoc = aDataGrid.getColumns();
		int colCount = schemaDoc.count();
		int rowCount = aDataGrid.rowCount();
		if ((colCount > 0) && (rowCount > 0))
		{
			String gridKeyName = mRedisKey.moduleJson().redisJsonDocument().dataObject(aDataGrid).name();
			aDataGrid.addFeature(Redis.FEATURE_KEY_NAME, gridKeyName);
			String gridKeyValue = columnSchemaToString(aDataGrid);
			RedisCore redisCore = mRedisDS.createCore();
			redisCore.sortedSetAdd(gridKeyName, Redis.GRID_RANGE_SCHEMA, gridKeyValue);
			int multiCount = 0;
			Pipeline commandPipeline = null;
			for (int row = 0; row < rowCount; row++)
			{
				if (multiCount == 0)
				{
					commandPipeline = new Pipeline(mRedisDS.getCmdConnection());
					mRedisDS.saveCommand(appLogger, "PIPELINE");
				}
				dataDoc = aDataGrid.getRowAsDoc(row);
				add(dataDoc, commandPipeline);
				keyName = dataDoc.getFeature(Redis.FEATURE_KEY_NAME);
				commandPipeline.zadd(gridKeyName, row+1, keyName);
				mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), row+1, mRedisDS.escapeValue(keyName)));
				multiCount += 2;
				if (multiCount >= Redis.PIPELINE_BATCH_COUNT)
				{
					commandPipeline.sync();
					commandPipeline = null;
					mRedisDS.saveCommand(appLogger, "SYNC");
					multiCount = 0;
				}
			}
			if (multiCount > 0)
			{
				commandPipeline.sync();
				mRedisDS.saveCommand(appLogger, "SYNC");
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the data document as JSON documents in the Redis database.
	 * The new JSON document will be added to the end of the tracking
	 * list.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aDataDoc Data document instance
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataGrid aDataGrid, DataDoc aDataDoc)
		throws RedisDSException
	{
		String gridKeyName, docKeyName;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		gridKeyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(gridKeyName))
		{
			RedisCore redisCore = mRedisDS.createCore();
			long dbRowCount = redisCore.sortedSetCount(gridKeyName);
			if (dbRowCount > 0)
			{
				add(aDataDoc);
				docKeyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
				redisCore.sortedSetAdd(gridKeyName, dbRowCount, docKeyName);
			}
			else
				throw new RedisDSException("Data grid row count for JSON documents is zero.");
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns an optional data grid schema from the Redis database
	 * based on the key name.
	 *
	 * @param aKeyName Key name for data grid
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataGrid> getGridSchema(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getGridSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
		if (optDataGrid.isPresent())
		{
			DataGrid dataGrid = optDataGrid.get();
			List<String> listValues = mCmdConnection.zrange(aKeyName, Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH);
			mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(aKeyName), Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH));
			int valueCount = listValues.size();
			if (valueCount > 0)
			{
				DataDoc schemaDataDoc = stringToColumnSchema(listValues.get(0));
				schemaDataDoc.setName(dataGrid.getName());
				dataGrid.setColumns(schemaDataDoc);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataGrid;
	}

	/**
	 * Returns an optional data document from the Redis database based on
	 * the key name.
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataDoc> getDoc(String aKeyName)
		throws RedisDSException
	{
		Optional<DataDoc> optDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "getDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		if (StringUtils.isNotEmpty(aKeyName))
		{
			JSONArray jsonArray = (JSONArray) mCmdConnection.jsonGet(aKeyName, Path2.ROOT_PATH);
			if ((jsonArray != null) && (! jsonArray.isEmpty()))
			{
				JSONObject jsonObject = jsonArray.getJSONObject(0);
				optDataDoc = jsonObjectToDataDoc(jsonObject);
				if (optDataDoc.isPresent())
				{
					DataDoc dataDoc = optDataDoc.get();
					dataDoc.addFeature(Redis.FEATURE_KEY_NAME, aKeyName);
					dataDoc.addFeature(Redis.FEATURE_DS_TYPE_NAME, Redis.DS_TYPE_JSON_DOC_NAME);

					String redisCommand = String.format("JSON.GET %s %s", mRedisDS.escapeKey(aKeyName), Redis.JSON_PATH_ROOT);
					mRedisDS.saveCommand(appLogger, redisCommand);
				}
			}
			else
			{
				String msgStr = String.format("[%s]: JSON get operation returns an empty array.", aKeyName);
				appLogger.error(msgStr);
				throw new RedisDSException(msgStr);
			}
		}
		else
		{
			String msgStr = "Data document lacks a feature key name.";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	/**
	 * Loads the data document with fields/values of a Redis JSON
	 * document.  The key name is obtained from Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * Note: This method does a flat data item assignment.  If you
	 *       need to recover the entire hierarchy, then use the
	 *       {@link #getDoc(String)} method.
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void loadDoc(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		Optional<DataDoc> optDataDoc = getDoc(keyName);
		if (optDataDoc.isPresent())
		{
			DataDoc dataDoc = optDataDoc.get();
			for (DataItem dataItem : dataDoc.getItems())
				aDataDoc.setValueByName(dataItem.getName(), mRedisDS.collapseValue(dataItem));
			aDataDoc.addFeature(Redis.FEATURE_DS_TYPE_NAME, Redis.DS_TYPE_JSON_DOC_NAME);
		}
		else
		{
			String msgStr = String.format("%s: Unable to load data document by key name.", keyName);
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns an optional data grid with row data populated from the
	 * Redis database based on the key name.
	 *
	 * <b>Note:</b> Since streams storage could consist of events with
	 * different member fields, it is not supported by this call.
	 * Also, geo location is best loaded with specialize distance
	 * queries and therefore unsupported with this method.
	 *
	 * @param aKeyName Key name for data grid
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataGrid> getGridData(String aKeyName)
		throws RedisDSException
	{
		DataDoc dataDoc;
		Optional<DataDoc> optDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "getGridData");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
		if (optDataGrid.isPresent())
		{
			DataGrid dataGrid = optDataGrid.get();
			List<String> listValues = mRedisDS.createCore().sortedSetLoad(aKeyName, Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH);
			int valueCount = listValues.size();
			if (valueCount > 0)
			{
				DataDoc schemaDataDoc = stringToColumnSchema(listValues.get(0));
				if (schemaDataDoc.count() > 0)
				{
					dataGrid.setColumns(schemaDataDoc);
					for (int row = 1; row < valueCount; row++)
					{
						optDataDoc = getDoc(listValues.get(row));
						if (optDataDoc.isPresent())
						{
							dataDoc = optDataDoc.get();
							dataGrid.addRow(dataDoc);
						}
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataGrid;
	}

	/**
	 * Returns the key name of the data document stored in the data
	 * grid based on the row offset value.
	 *
	 * @param aKeyName Data grid key name
	 * @param aRowOffset Row offset
	 *
	 * @return Key name or an empty string if the row offset is out of range
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public String getDataDocKeyNameByRowOffset(String aKeyName, int aRowOffset)
		throws RedisDSException
	{
		String keyName;
		Logger appLogger = mAppCtx.getLogger(this, "getDataDocKeyNameByRowOffset");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		List<String> listValues = mRedisDS.createCore().sortedSetLoad(aKeyName, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
		if (aRowOffset < listValues.size())
			keyName = listValues.get(aRowOffset);
		else
			keyName = StringUtils.EMPTY;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return keyName;
	}

	/**
	 * Return the count of rows for the data grid identified by the key
	 * name in the Redis database.
	 *
	 * @param aKeyName Key name for data grid
	 *
	 * @return Row count or -1 if key does not exist
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long getRowCount(String aKeyName)
		throws RedisDSException
	{
		long rowCount;
		Logger appLogger = mAppCtx.getLogger(this, "getRowCount");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
		if (optDataGrid.isPresent())
		{
			rowCount = mCmdConnection.zcard(aKeyName);
			mRedisDS.saveCommand(appLogger, String.format("ZCARD %s", mRedisDS.escapeKey(aKeyName)));
			if (rowCount > 0)
				rowCount--;
		}
		else
			rowCount = 0;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rowCount;
	}

	/**
	 * Return the count of rows for the data grid identified by the key
	 * name in the Redis database.  The key name is obtained from the
	 * feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Row count or -1 if key does not exist
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long getRowCount(DataGrid aDataGrid)
		throws RedisDSException
	{
		long rowCount = getRowCount(aDataGrid.getFeature(Redis.FEATURE_KEY_NAME));
		aDataGrid.addFeature(DS.FEATURE_TOTAL_DOCUMENTS, rowCount);

		return rowCount;
	}

	/**
	 * Loads the data grid rows and columns from the Redis database based on
	 * the row start/finish parameters.  The key name is obtained from the
	 * feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * <b>Note:</b> Any existing rows in the grid will be emptied prior
	 * to the load operation.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aRowNumberStart Row number start (1 - N)
	 * @param aRowNumberFinish Row number finish (1 - N)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void loadGrid(DataGrid aDataGrid, long aRowNumberStart, long aRowNumberFinish)
		throws RedisDSException
	{
		DataDoc dataDoc;
		List<String> listValues;
		Optional<DataDoc> optDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if ((StringUtils.isNotEmpty(keyName)) && (aDataGrid.getColumns().count() > 0))
		{
			aRowNumberStart = Math.max(aRowNumberStart, Redis.GRID_RANGE_START);
			aDataGrid.emptyRows();
			if (aRowNumberFinish != Redis.GRID_RANGE_FINISH)
			{
				long listSize = mCmdConnection.zcard(keyName);
				mRedisDS.saveCommand(appLogger, String.format("ZCARD %s", mRedisDS.escapeKey(keyName)));
				if (aRowNumberFinish > listSize)
					aRowNumberFinish = Redis.GRID_RANGE_FINISH;
			}
			listValues = mCmdConnection.zrange(keyName, aRowNumberStart, aRowNumberFinish);
			mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(keyName), aRowNumberStart, aRowNumberFinish));
			int valueCount = listValues.size();
			for (int row = 0; row < valueCount; row++)
			{
				optDataDoc = getDoc(listValues.get(row));
				if (optDataDoc.isPresent())
				{
					dataDoc = optDataDoc.get();
					aDataGrid.addRow(dataDoc);
				}
			}
		}
		getRowCount(aDataGrid);
		long curLimit = aRowNumberFinish - aRowNumberStart;
		aDataGrid.addFeature(DS.FEATURE_CUR_LIMIT, curLimit);
		aDataGrid.addFeature(DS.FEATURE_CUR_OFFSET, aRowNumberStart);
		aDataGrid.addFeature(DS.FEATURE_NEXT_OFFSET, aRowNumberFinish+1);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the data grid rows and columns from the Redis database based on
	 * the row start/finish parameters.  The key name is obtained from the
	 * feature Redis.REDIS_FEATURE_KEY_NAME.  As a network optimization,
	 * command pipelining is utilized.
	 *
	 * <b>Note:</b> Any existing rows in the grid will be emptied prior
	 * to the load operation.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aRowNumberStart Row number start (1 - N)
	 * @param aRowNumberFinish Row number finish (1 - N)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void loadGridPipeline(DataGrid aDataGrid, long aRowNumberStart, long aRowNumberFinish)
		throws RedisDSException
	{
		String keyName;
		DataDoc dataDoc;
		List<String> listValues;
		Optional<DataDoc> optDataDoc;
		List<Object> pipelineResponseList;
		Logger appLogger = mAppCtx.getLogger(this, "loadGridPipeline");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if ((StringUtils.isNotEmpty(keyName)) && (aDataGrid.getColumns().count() > 0))
		{
			aDataGrid.emptyRows();
			aRowNumberStart = Math.max(aRowNumberStart, Redis.GRID_RANGE_START);
			if (aRowNumberFinish != Redis.GRID_RANGE_FINISH)
			{
				long listSize = mCmdConnection.zcard(keyName);
				mRedisDS.saveCommand(appLogger, String.format("ZCARD %s", mRedisDS.escapeKey(keyName)));
				if (aRowNumberFinish > listSize)
					aRowNumberFinish = Redis.GRID_RANGE_FINISH;
			}
			listValues = mCmdConnection.zrange(keyName, aRowNumberStart, aRowNumberFinish);
			mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(keyName), aRowNumberStart, aRowNumberFinish));
			int multiCount = 0;
			Pipeline commandPipeline = null;
			int valueCount = listValues.size();
			for (int row = 0; row < valueCount; row++)
			{
				if (multiCount == 0)
				{
					commandPipeline = new Pipeline(mRedisDS.getCmdConnection());
					mRedisDS.saveCommand(appLogger, "PIPELINE");
				}
				keyName = listValues.get(row);
				commandPipeline.jsonGet(keyName, Path2.ROOT_PATH);
				mRedisDS.saveCommand(appLogger, String.format("JSON.GET %s %s", mRedisDS.escapeKey(keyName), Redis.JSON_PATH_ROOT));
				multiCount++;
				if (multiCount >= Redis.PIPELINE_BATCH_COUNT)
				{
					pipelineResponseList = commandPipeline.syncAndReturnAll();
					mRedisDS.saveCommand(appLogger, "SYNC");
					for (Object responseObject : pipelineResponseList)
					{
						JSONArray jsonArray = (JSONArray) responseObject;
						if ((jsonArray != null) && (! jsonArray.isEmpty()))
						{
							JSONObject jsonObject = jsonArray.getJSONObject(0);
							optDataDoc = jsonObjectToDataDoc(jsonObject);
							if (optDataDoc.isPresent())
							{
								dataDoc = optDataDoc.get();
								aDataGrid.addRow(dataDoc);
							}
						}
					}
					commandPipeline = null;
					multiCount = 0;
				}
			}
			if (multiCount > 0)
			{
				pipelineResponseList = commandPipeline.syncAndReturnAll();
				mRedisDS.saveCommand(appLogger, "SYNC");
				for (Object responseObject : pipelineResponseList)
				{
					JSONArray jsonArray = (JSONArray) responseObject;
					if ((jsonArray != null) && (! jsonArray.isEmpty()))
					{
						JSONObject jsonObject = jsonArray.getJSONObject(0);
						optDataDoc = jsonObjectToDataDoc(jsonObject);
						if (optDataDoc.isPresent())
						{
							dataDoc = optDataDoc.get();
							aDataGrid.addRow(dataDoc);
						}
					}
				}
			}
		}
		getRowCount(aDataGrid);
		long curLimit = aRowNumberFinish - aRowNumberStart;
		aDataGrid.addFeature(DS.FEATURE_CUR_LIMIT, curLimit);
		aDataGrid.addFeature(DS.FEATURE_CUR_OFFSET, aRowNumberStart);
		aDataGrid.addFeature(DS.FEATURE_NEXT_OFFSET, aRowNumberFinish+1);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the data document stored as a Redis JSON document in the Redis database.
	 *
	 * Note: This method simply replaces the current Redis JSON
	 *       in the Redis database (e.g. it does not attempt
	 *       to selectively identify changed items and limit
	 *       the update to them via a JSON path).
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		if ((aDataDoc == null) || (aDataDoc.count() == 0))
			throw new RedisDSException("Data document is null or emtpy - cannot add it to Redis.");

		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			try
			{
				JSONObject jsonObject = dataDocToJSONObject(aDataDoc);
				mCmdConnection.jsonSet(keyName, jsonObject, JsonSetParams.jsonSetParams().xx());
				String redisCommand = String.format("JSON.SET %s %s '%s' XX", mRedisDS.escapeKey(keyName), Redis.JSON_PATH_ROOT, jsonObject.toString());
				mRedisDS.saveCommand(appLogger, redisCommand);
			}
			catch (IOException e)
			{
				String errMsg = String.format("%s: %s", aDataDoc.getName(), e.getMessage());
				appLogger.error(errMsg);
				throw new RedisDSException(errMsg);
			}
		}
		else
		{
			String msgStr = "Data document lacks a feature key name.";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the data document fields/values in the Redis database as
	 * JSON document.  The updates are based on a comparison of
	 * the two data document instance where updated fields are
	 * written to the database.  Please note that child data
	 * documents are ignored during this operation.
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aDataDoc1 Data document instance 1 (base)
	 * @param aDataDoc2 Data document instance 2 (changed)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(DataDoc aDataDoc1, DataDoc aDataDoc2)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		if ((aDataDoc1 == null) || (aDataDoc1.count() == 0) || (aDataDoc2 == null) || (aDataDoc2.count() == 0))
			throw new RedisDSException("Data document is null or emtpy - cannot update it in Redis.");

		String keyName = aDataDoc2.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			DataItem updDataItem;
			DataDoc changeDataDoc;
			Optional<DataDoc> optDataDoc;
			String jsonPath, redisCommand;

			DataDocDiff dataDocDiff = new DataDocDiff();
			dataDocDiff.compare(aDataDoc1, aDataDoc2);
			if (! dataDocDiff.isEqual())
			{
				optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_UPDATED);
				if (optDataDoc.isPresent())
				{
					changeDataDoc = optDataDoc.get();
					for (DataItem dataItem : changeDataDoc.getItems())
					{
						updDataItem = aDataDoc2.getItemByName(dataItem.getName());
						if (updDataItem.isFeatureAssigned(Data.FEATURE_JSON_PATH))
							jsonPath = updDataItem.getFeature(Data.FEATURE_JSON_PATH);
						else
						{
							jsonPath = String.format("$.%s", updDataItem.getName());
							updDataItem.addFeature(Data.FEATURE_JSON_PATH, jsonPath);
						}
						Path2 rjPath = new Path2(jsonPath);
						mCmdConnection.jsonSet(keyName, rjPath, updDataItem.getValueAsObject(), JsonSetParams.jsonSetParams().xx());
						if (Data.isText(updDataItem.getType()))
							redisCommand = String.format("JSON.SET %s '%s' '\"%s\"' XX", mRedisDS.escapeKey(keyName), jsonPath, updDataItem.getValue());
						else
							redisCommand = String.format("JSON.SET %s '%s' '%s' XX", mRedisDS.escapeKey(keyName), jsonPath, updDataItem.getValue());
						mRedisDS.saveCommand(appLogger, redisCommand);
					}
				}
			}
		}
		else
		{
			String msgStr = "Data document lacks a feature key name.";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes the data document stored as a Redis JSON document from the Redis database.
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aKeyName Redis JSON document key name
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(false);
		if (StringUtils.isNotEmpty(aKeyName))
		{
			mRedisDS.createCore().delete(aKeyName);
			String redisCommand = String.format("JSON.DEL %s", mRedisDS.escapeKey(aKeyName));
			mRedisDS.saveCommand(appLogger, redisCommand);
		}
		else
		{
			String msgStr = "Invalid key name.";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes the data document stored as a Redis JSON document from the Redis database.
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(false);
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
			delete(keyName);
		else
		{
			String msgStr = "Data document lacks a feature key name.";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes the row identified by the row number from the Redis database.
	 * The key name is obtained from the feature RedisJson.REDIS_FEATURE_KEY_NAME.
	 *
	 * <b>Note:</b> This method does not delete the data document
	 * instance within the data grid instance.  You must reload
	 * the data grid from the Redis database.
	 *
	 * @param aKeyName Key name for data grid
	 * @param aRowNumber Row number (1 - N)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(String aKeyName, long aRowNumber)
		throws RedisDSException
	{
		String memberValue;
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		if (aRowNumber > 0)
		{
			RedisCore redisCore = mRedisDS.createCore();
			List<String> listValues = redisCore.sortedSetLoad(aKeyName, aRowNumber, Redis.GRID_RANGE_FINISH);
			if (listValues.size() > 0)
			{
				memberValue = listValues.get(0);
				if (redisCore.sortedSetDelete(aKeyName, memberValue))
				{
					for (String lValue : listValues)
					{
						if (! lValue.equals(memberValue))
							redisCore.sortedSetIncrementBy(aKeyName, -1, lValue);
					}
					redisCore.delete(memberValue);
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes the row identified by the row number from the Redis database.
	 * The key name is obtained from the feature RedisJson.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aRowNumber Row number (1 - N)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(DataGrid aDataGrid, int aRowNumber)
		throws RedisDSException
	{
		if (aRowNumber > 0)
		{
			delete(aDataGrid.getFeature(Redis.FEATURE_KEY_NAME), aRowNumber);
			aDataGrid.deleteRow(aRowNumber-1);
		}
	}

	/**
	 * Deletes the rows of data documents from the Redis database.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @see <a href="https://oss.redis.com/redisjson/commands/">RedisJSON Commands</a>
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		String keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			RedisCore redisCore = mRedisDS.createCore();
			List<String> listValues = redisCore.sortedSetLoad(keyName, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
			for (String aListValue : listValues)
				delete(aListValue);
			redisCore.delete(keyName);
		}
		else
			throw new RedisDSException("Data grid does not have a key name assigned to it.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
