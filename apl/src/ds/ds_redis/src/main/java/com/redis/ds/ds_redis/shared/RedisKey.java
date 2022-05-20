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

package com.redis.ds.ds_redis.shared;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.std.DigitalHash;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * The RedisKey class handles Redis key generation and data object
 * restoration operations.  The Redis operations executed via the
 * Jedis programmatic library.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * Key Formats:
 *  "AppPrefix:Module:RedisType:DataObject:Method:Name[:Id]" (everything except DataItem)
 *  "AppPrefix:Module:RedisType:DataObject:Method:Name:DataType:ValueType:ValueFormat" (DataItem)
 *  "AppPrefix:Module:RedisType:DataObject:Method:Name:Id:DataType:ValueType:ValueFormat" (DataItem)
 *
 * @see <a href="https://redis.io/commands">OSS Redis Commands</a>
 * @see <a href="https://github.com/redis/jedis">Jedis GitHub site</a>
 * @see <a href="https://www.baeldung.com/jedis-java-redis-client-library">Intro to Jedis – the Java Redis Client Library</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisKey
{
	private DataDoc mDataDoc;
	private DataItem mDataItem;
	private DataGrid mDataGrid;
	private DataGraph mDataGraph;
	private final AppCtx mAppCtx;
	protected final RedisDS mRedisDS;
	private String mId = StringUtils.EMPTY;
	private String mName = StringUtils.EMPTY;
	private String mPrefix = StringUtils.EMPTY;
	private String mModule = StringUtils.EMPTY;
	private String mKeyName = StringUtils.EMPTY;
	private String mRedisType = StringUtils.EMPTY;
	private String mMethod = Redis.KEY_ID_METHOD_NAME;
	private String mDataObject = Redis.KEY_DATA_OBJECT_DOCUMENT;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the key objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisKey(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mPrefix = mRedisDS.getApplicationPrefix();
	}

	/**
	 * Constructor accepts a Redis data source parameter and key name.
	 *
	 * @param aRedisDS Redis data source instance
	 * @param aKeyName Key name
	 */
	public RedisKey(RedisDS aRedisDS, String aKeyName)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		parseAssign(aKeyName);
	}

	/**
	 * Assigns a module name to the Redis key sequence.
	 *
	 * @param aModule Module id (Redis.KEY_MODULE_xxxxx)
	 *
	 * @return Builder instance
	 */
	public RedisKey module(String aModule)
	{
		reset();
		mModule = aModule;
		return this;
	}
	public RedisKey moduleCore()
	{
		return module(Redis.KEY_MODULE_CORE);
	}
	public RedisKey moduleSearch()
	{
		return module(Redis.KEY_MODULE_SEARCH);
	}
	public RedisKey moduleJson()
	{
		return module(Redis.KEY_MODULE_JSON);
	}
	public RedisKey moduleGraph()
	{
		return module(Redis.KEY_MODULE_GRAPH);
	}
	public RedisKey moduleTimeSeries()
	{
		return module(Redis.KEY_MODULE_TIME_SERIES);
	}

	/**
	 * Assigns a data type to the Redis key sequence.
	 *
	 * @param aRedisType Redis type id (Redis.KEY_REDIS_TYPE_xxxxx)
	 *
	 * @return RedisKey instance
	 */
	public RedisKey redisType(String aRedisType)
	{
		mRedisType = aRedisType;
		mKeyName = StringUtils.EMPTY;
		return this;
	}
	public RedisKey redisSet()
	{
		return redisType(Redis.KEY_REDIS_TYPE_SET);
	}
	public RedisKey redisHash()
	{
		return redisType(Redis.KEY_REDIS_TYPE_HASH);
	}
	public RedisKey redisString()
	{
		return redisType(Redis.KEY_REDIS_TYPE_STRING);
	}
	public RedisKey redisStream()
	{
		return redisType(Redis.KEY_REDIS_TYPE_STREAM);
	}
	public RedisKey redisHyperLog()
	{
		return redisType(Redis.KEY_REDIS_TYPE_HYPER_LOG);
	}
	public RedisKey redisSortedSet()
	{
		return redisType(Redis.KEY_REDIS_TYPE_SORTED_SET);
	}
	public RedisKey redisGraph()
	{
		return redisType(Redis.KEY_REDIS_TYPE_GRAPH_PROPERTY);
	}
	public RedisKey redisGraphSchema()
	{
		return redisType(Redis.KEY_REDIS_TYPE_GRAPH_SCHEMA);
	}
	public RedisKey redisJsonSchema()
	{
		return redisType(Redis.KEY_REDIS_TYPE_JSON_SCHEMA);
	}
	public RedisKey redisJsonDocument()
	{
		return redisType(Redis.KEY_REDIS_TYPE_JSON_DOCUMENT);
	}
	public RedisKey redisTimeSeries()
	{
		return redisType(Redis.KEY_REDIS_TYPE_TIME_SERIES);
	}
	public RedisKey redisSearchIndex()
	{
		return redisType(Redis.KEY_REDIS_TYPE_SEARCH_INDEX);
	}
	public RedisKey redisSearchSchema()
	{
		return redisType(Redis.KEY_REDIS_TYPE_SEARCH_SCHEMA);
	}
	public RedisKey redisSearchSuggest()
	{
		return redisType(Redis.KEY_REDIS_TYPE_SEARCH_SUGGEST);
	}
	public RedisKey redisSearchSynonym()
	{
		return redisType(Redis.KEY_REDIS_TYPE_SEARCH_SYNONYM);
	}

	/**
	 * Assigns a data object to the Redis key sequence.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return RedisKey instance
	 */
	public RedisKey dataObject(DataItem aDataItem)
	{
		mDataItem = aDataItem;
		mName = aDataItem.getName();
		mDataObject = Redis.KEY_DATA_OBJECT_ITEM;
		mKeyName = StringUtils.EMPTY;
		return this;
	}

	/**
	 * Assigns a data object to the Redis key sequence.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return RedisKey instance
	 */
	public RedisKey dataObject(DataDoc aDataDoc)
	{
		mDataDoc = aDataDoc;
		mName = aDataDoc.getName();
		mDataObject = Redis.KEY_DATA_OBJECT_DOCUMENT;
		mKeyName = StringUtils.EMPTY;
		return this;
	}

	/**
	 * Assigns a data object to the Redis key sequence.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return RedisKey instance
	 */
	public RedisKey dataObject(DataGrid aDataGrid)
	{
		mDataGrid = aDataGrid;
		mName = aDataGrid.getName();
		mDataObject = Redis.KEY_DATA_OBJECT_GRID;
		mKeyName = StringUtils.EMPTY;
		return this;
	}

	/**
	 * Assigns a data object to the Redis key sequence.
	 *
	 * @param aDataGraph Data graph instance
	 *
	 * @return RedisKey instance
	 */
	public RedisKey dataObject(DataGraph aDataGraph)
	{
		mDataGraph = aDataGraph;
		mName = aDataGraph.getName();
		mDataObject = Redis.KEY_DATA_OBJECT_GRAPH;
		mKeyName = StringUtils.EMPTY;
		return this;
	}

	/**
	 * Assigns a data object name to the Redis key sequence.
	 *
	 * @param aName Data object name
	 *
	 * @return RedisKey instance
	 */
	public RedisKey dataName(String aName)
	{
		mName = aName;
		mKeyName = StringUtils.EMPTY;
		return this;
	}

	/**
	 * Assigns a hashing id based on a previously assigned data object.  Since
	 * an object's hash id can change as its members change, you should use
	 * another storage mechanism to hold the orignal key name (e.g. you may
	 * not be able to derive the same key name from an object if it changed).
	 *
	 * @return RedisKey instance
	 */
	public RedisKey hashId()
	{
		mKeyName = StringUtils.EMPTY;
		mMethod = Redis.KEY_ID_METHOD_HASH;
		if (mDataItem != null)
		{
			DigitalHash digitalHash = new DigitalHash();
			try
			{
				digitalHash.processBuffer(mDataItem.getName());
				digitalHash.processBuffer(Data.typeToString(mDataItem.getType()));
				digitalHash.processBuffer(mDataItem.getTitle());
				if (mDataItem.isMultiValue())
					digitalHash.processBuffer(mDataItem.getValuesCollapsed());
				else
					digitalHash.processBuffer(mDataItem.getValue());
				mId = digitalHash.getHashSequence();
			}
			catch (IOException e)
			{
				mId = UUID.randomUUID().toString();
			}
		}
		else if (mDataDoc != null)
			mId =  mDataDoc.generateUniqueHash(false);
		else if (mDataGrid != null)
			mId =  mDataGrid.getColumns().generateUniqueHash(false);
		else if (mDataGraph != null)
		{
			DigitalHash digitalHash = new DigitalHash();
			try
			{
				digitalHash.processBuffer(mDataGraph.getName());
				digitalHash.processBuffer(mDataGraph.getDataModel().toString());
				digitalHash.processBuffer(mDataGraph.getStructure().toString());
				mId = digitalHash.getHashSequence();
			}
			catch (IOException e)
			{
				mId = UUID.randomUUID().toString();
			}
		}
		return this;
	}

	/**
	 * Assigns a random universal unique id.  You should only use
	 * this method if you plan to track/store your key names in
	 * separately.
	 *
	 * @return RedisKey instance
	 */
	public RedisKey randomId()
	{
		mKeyName = StringUtils.EMPTY;
		mMethod = Redis.KEY_ID_METHOD_RANDOM;
		mId = UUID.randomUUID().toString();
		return this;
	}

	/**
	 * Assigns a primary id derived from a data document instance.
	 *
	 * @return RedisKey instance
	 */
	public RedisKey primaryId()
	{
		mKeyName = StringUtils.EMPTY;
		mMethod = Redis.KEY_ID_METHOD_PRIMARY;
		if (mDataDoc != null)
		{
			Optional<DataItem> optDataItem = mDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
			if (optDataItem.isPresent())
			{
				DataItem dataItem = optDataItem.get();
				mId = dataItem.getValue();
				return this;
			}
			else
				return randomId();
		}
		else
			return randomId();
	}

	/**
	 * Assigns a primary id provided as a parameter.
	 *
	 * @param anId Primary id string
	 *
	 * @return RedisKey instance
	 */
	public RedisKey primaryId(String anId)
	{
		mKeyName = StringUtils.EMPTY;
		mMethod = Redis.KEY_ID_METHOD_PRIMARY;
		mId = anId;

		return this;
	}

	@SuppressWarnings("EnhancedSwitchMigration")
	private String dataTypeToString(Data.Type aType)
	{
		switch (aType)
		{
			case Date:
				return Redis.KEY_DATA_TYPE_DATE;
			case Long:
				return Redis.KEY_DATA_TYPE_LONG;
			case Float:
				return Redis.KEY_DATA_TYPE_FLOAT;
			case Double:
				return Redis.KEY_DATA_TYPE_DOUBLE;
			case Boolean:
				return Redis.KEY_DATA_TYPE_BOOLEAN;
			case Integer:
				return Redis.KEY_DATA_TYPE_INTEGER;
			case DateTime:
				return Redis.KEY_DATA_TYPE_DATETIME;
			default:
				return Redis.KEY_DATA_TYPE_TEXT;
		}
	}

	@SuppressWarnings("EnhancedSwitchMigration")
	private Data.Type stringToDataType(String aDataType)
	{
		switch (aDataType)
		{
			case Redis.KEY_DATA_TYPE_DATE:
				return Data.Type.Date;
			case Redis.KEY_DATA_TYPE_LONG:
				return Data.Type.Long;
			case Redis.KEY_DATA_TYPE_FLOAT:
				return Data.Type.Float;
			case Redis.KEY_DATA_TYPE_DOUBLE:
				return Data.Type.Double;
			case Redis.KEY_DATA_TYPE_BOOLEAN:
				return Data.Type.Boolean;
			case Redis.KEY_DATA_TYPE_INTEGER:
				return Data.Type.Integer;
			case Redis.KEY_DATA_TYPE_DATETIME:
				return Data.Type.DateTime;
			default:
				return Data.Type.Text;
		}
	}

	/**
	 * Generates and assigns a key name.
	 *
	 * Key Formats:
	 *  "AppPrefix:Module:RedisType:DataObject:Method:Name[:Id]" (everything except DataItem)
	 *  "AppPrefix:Module:RedisType:DataObject:Method:Name:DataType:ValueType:ValueFormat" (DataItem)
	 *  "AppPrefix:Module:RedisType:DataObject:Method:Name:Id:DataType:ValueType:ValueFormat" (DataItem)
	 *
	 * @return Key name string
	 */
	public String name()
	{
		if (StringUtils.isEmpty(mKeyName))
		{
			StringBuilder stringBuilder = new StringBuilder(mRedisDS.getApplicationPrefix());
			stringBuilder.append(StrUtl.CHAR_COLON);
			stringBuilder.append(mModule);
			stringBuilder.append(StrUtl.CHAR_COLON);
			stringBuilder.append(mRedisType);
			stringBuilder.append(StrUtl.CHAR_COLON);
			stringBuilder.append(mDataObject);
			stringBuilder.append(StrUtl.CHAR_COLON);
			stringBuilder.append(mMethod);
			stringBuilder.append(StrUtl.CHAR_COLON);
			stringBuilder.append(mName);
			if (StringUtils.isNotEmpty(mId))
			{
				stringBuilder.append(StrUtl.CHAR_COLON);
				stringBuilder.append(mId);
			}
			if (mDataItem != null)
			{
				stringBuilder.append(StrUtl.CHAR_COLON);
				stringBuilder.append(dataTypeToString(mDataItem.getType()));
				stringBuilder.append(StrUtl.CHAR_COLON);
				if (mDataItem.isMultiValue())
					stringBuilder.append(Redis.KEY_VALUE_MULTI);
				else
					stringBuilder.append(Redis.KEY_VALUE_SINGLE);
				stringBuilder.append(StrUtl.CHAR_COLON);
				if (mDataItem.isFeatureTrue(Data.FEATURE_IS_SECRET))
					stringBuilder.append(Redis.KEY_VALUE_ENCRYPTED);
				else
					stringBuilder.append(Redis.KEY_VALUE_PLAIN);
			}
			mKeyName = stringBuilder.toString();
		}
		return mKeyName;
	}

	/**
	 * Returns the id value from the parsed key name.
	 *
	 * @return Id string
	 */
	public String getId()
	{
		return mId;
	}

	/**
	 * Returns a key prefix suitable for RediSearch schema creation.
	 *
	 * @param aModule Module id (Redis.KEY_MODULE_xxxxx)
	 *
	 * @return Search key name prefix string
	 */
	public String searchPrefix(String aModule)
	{
		if (StringUtils.equals(aModule, Redis.KEY_MODULE_JSON))
			return String.format("%s:%s:%s:", mRedisDS.getApplicationPrefix(), aModule, Redis.KEY_REDIS_TYPE_JSON_DOCUMENT);
		else
			return String.format("%s:%s:%s:", mRedisDS.getApplicationPrefix(), aModule, Redis.KEY_REDIS_TYPE_HASH);
	}

	/**
	 * Key Formats:
	 * 	"AppPrefix:Module:RedisType:DataObject:Method:Name[:Id]" (everything except DataItem)
	 * 	 0         1      2         3          4      5     6
	 * 	"AppPrefix:Module:RedisType:DataObject:Method:Name:DataType:ValueType:ValueFormat" (DataItem)
	 * 	 0         1      2         3          4      5    6        7         8
	 * 	"AppPrefix:Module:RedisType:DataObject:Method:Name:Id:DataType:ValueType:ValueFormat" (DataItem)
	 *   0         1      2         3          4      5    6  7        8         9
	 *
	 * @param aKeyName Key name
	 */
	private void parseAssign(String aKeyName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "parseAssign");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int separatorCount = StringUtils.countMatches(aKeyName, StrUtl.CHAR_COLON);
		if (separatorCount > 4)
		{
			int offset = 0;
			String[] keyParameters = aKeyName.split(String.valueOf(StrUtl.CHAR_COLON));
			mPrefix = keyParameters[offset++];
			mModule = keyParameters[offset++];
			mRedisType = keyParameters[offset++];
			mDataObject = keyParameters[offset++];
			mMethod = keyParameters[offset++];
			mName = keyParameters[offset++];
			if (StringUtils.isNotEmpty(mMethod))
			{
				switch (mMethod)
				{
					case Redis.KEY_ID_METHOD_HASH:
					case Redis.KEY_ID_METHOD_RANDOM:
					case Redis.KEY_ID_METHOD_PRIMARY:
						if (offset < separatorCount)
							mId = keyParameters[offset++];
						break;
					default:
						break;
				}
				mKeyName = aKeyName;

				if (mDataObject.equals(Redis.KEY_DATA_OBJECT_ITEM))
				{
					if (offset < separatorCount)
					{
						mDataItem = new DataItem.Builder().type(stringToDataType(keyParameters[offset++])).name(mName).title(Data.nameToTitle(mName)).build();
						mDataItem.addFeature(Redis.FEATURE_KEY_NAME, mKeyName);
						if (keyParameters[offset++].equals(Redis.KEY_VALUE_MULTI))
							mDataItem.enableFeature(Data.FEATURE_IS_MULTIVALUE);
						if (keyParameters[offset].equals(Redis.KEY_VALUE_ENCRYPTED))
							mDataItem.enableFeature(Data.FEATURE_IS_SECRET);
					}
				}
			}
		}
		else
			appLogger.error(String.format("[%s]: Incorrectly formatted with separator count of %d.", mKeyName, separatorCount));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);
	}

	/**
	 * Resets the state of the key members prior to new key name assignment.
	 */
	public void reset()
	{
		mDataDoc = null;
		mDataGrid = null;
		mDataItem = null;
		mDataGraph = null;
		mId = StringUtils.EMPTY;
		mName = StringUtils.EMPTY;
		mPrefix = StringUtils.EMPTY;
		mModule = StringUtils.EMPTY;
		mKeyName = StringUtils.EMPTY;
		mRedisType = StringUtils.EMPTY;
		mMethod = Redis.KEY_ID_METHOD_NAME;
		mDataObject = Redis.KEY_DATA_OBJECT_DOCUMENT;
	}

	/**
	 * Resets the state of the key members prior to a new key name assignment.
	 *
	 * @param aKeyName Key name
	 */
	public void resetParseAssign(String aKeyName)
	{
		reset();
		parseAssign(aKeyName);
	}

	/**
	 * Creates a data item instance from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> toDataItem()
	{
		DataItem dataItem = null;
		Logger appLogger = mAppCtx.getLogger(this, "toDataItem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(mKeyName)) && (mDataItem != null))
		{
			dataItem = new DataItem(mDataItem);
			dataItem.addFeature(Redis.FEATURE_KEY_NAME, mKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		return Optional.ofNullable(dataItem);
	}

	/**
	 * Creates a data item instance from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> toDataItem(String aKeyName)
	{
		resetParseAssign(aKeyName);
		return toDataItem();
	}

	/**
	 * Creates a data document from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> toDataDoc()
	{
		DataDoc dataDoc = null;
		Logger appLogger = mAppCtx.getLogger(this, "toDataDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(mKeyName)) && (mDataObject.equals(Redis.KEY_DATA_OBJECT_DOCUMENT)))
		{
			dataDoc = new DataDoc(mName);
			dataDoc.addFeature(Redis.FEATURE_KEY_NAME, mKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		return Optional.ofNullable(dataDoc);
	}

	/**
	 * Creates a data document from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> toDataDoc(String aKeyName)
	{
		resetParseAssign(aKeyName);
		return toDataDoc();
	}

	/**
	 * Creates a data grid instance from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @return Optional data grid instance
	 */
	public Optional<DataGrid> toDataGrid()
	{
		DataGrid dataGrid = null;
		Logger appLogger = mAppCtx.getLogger(this, "toDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(mKeyName)) && (mDataObject.equals(Redis.KEY_DATA_OBJECT_GRID)))
		{
			dataGrid = new DataGrid(mName);
			dataGrid.addFeature(Redis.FEATURE_KEY_NAME, mKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		return Optional.ofNullable(dataGrid);
	}

	/**
	 * Creates a data grid instance from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data grid instance
	 */
	public Optional<DataGrid> toDataGrid(String aKeyName)
	{
		resetParseAssign(aKeyName);
		return toDataGrid();
	}

	/**
	 * Creates a data item graph from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @return Optional data graph instance
	 */
	public Optional<DataGraph> toDataGraph()
	{
		DataGraph dataGraph = null;
		Logger appLogger = mAppCtx.getLogger(this, "toDataGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(mKeyName)) && (mDataObject.equals(Redis.KEY_DATA_OBJECT_GRAPH)))
		{
			dataGraph = new DataGraph(mName);
			dataGraph.addFeature(Redis.FEATURE_KEY_NAME, mKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		return Optional.ofNullable(dataGraph);
	}

	/**
	 * Creates a data item graph from the key name.  You must
	 * ensure you provide a key name with the constructor prior
	 * to invoking this method.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data graph instance
	 */
	public Optional<DataGraph> toDataGraph(String aKeyName)
	{
		resetParseAssign(aKeyName);
		return toDataGraph();
	}
}
