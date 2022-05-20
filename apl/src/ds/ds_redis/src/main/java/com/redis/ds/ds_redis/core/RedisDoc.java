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

package com.redis.ds.ds_redis.core;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.shared.RedisField;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataDocDiff;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.StreamEntry;

import java.util.*;

/**
 * The RedisDoc class handles Redis database operations centered around
 * {@link com.redis.foundation.data.DataDoc} objects.  The Redis operations
 * executed via the Jedis programmatic library.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * @see <a href="https://redis.io/commands">OSS Redis Commands</a>
 * @see <a href="https://github.com/redis/jedis">Jedis GitHub site</a>
 * @see <a href="https://www.baeldung.com/jedis-java-redis-client-library">Intro to Jedis – the Java Redis Client Library</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisDoc
{
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private final RedisKey mRedisKey;
	private final Jedis mCmdConnection;
	private final RedisField mRedisField;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the core objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisDoc(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mRedisField = new RedisField(mRedisDS);
		mCmdConnection = aRedisDS.getCmdConnection();
	}

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the core objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisDoc(RedisDS aRedisDS, boolean anIsFieldEhnanced)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mRedisField = new RedisField(mRedisDS);
		mCmdConnection = aRedisDS.getCmdConnection();
	}

	/**
	 * Generates a Redis key name based on the features of the DataItem.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Redis key name
	 */
	public String dataItemToFieldName(DataItem aDataItem)
	{
		if (((mRedisDS.getEncryptionOption() != Redis.Encryption.None) && (aDataItem.isFeatureTrue(Data.FEATURE_IS_SECRET))) ||
			(aDataItem.isFeatureTrue(Redis.FEATURE_IS_KEY)) || (aDataItem.isMultiValue()))
			return mRedisField.name(aDataItem);
		else
			return aDataItem.getName();
	}

	private Optional<DataDoc> saveChildDocuments(Pipeline aPipeline, DataDoc aDataDoc)
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveChildDocuments");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataDoc> optDataDoc = Optional.empty();
		if (aDataDoc.childrenCount() > 0)
		{
			DataDoc childDoc = new DataDoc("Child Documents");
			LinkedHashMap<String, ArrayList<DataDoc>> childDocsMap = aDataDoc.getChildDocs();
			childDocsMap.entrySet().forEach(es -> {
				DataItem childItem = new DataItem.Builder().name(es.getKey()).build();
				for (DataDoc dataDoc : es.getValue())
				{
					try
					{
						if (aPipeline != null)
							add(aPipeline, dataDoc);
						else
							add(dataDoc);
						String keyName = dataDoc.getFeature(Redis.FEATURE_KEY_NAME);
						childItem.addValue(keyName);
					}
					catch (RedisDSException re)
					{
						appLogger.error(String.format("%s: %s", dataDoc.getName(), re.getMessage()));
					}
				}
				childItem.enableFeature(Redis.FEATURE_IS_KEY);
				childDoc.add(childItem);
			});
			if (childDoc.count() > 0)
				optDataDoc = Optional.of(childDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	private DataItem shadowDataItem(DataItem aDataItem, Date aDate)
	{
		DataItem dataItem = new DataItem(Data.Type.Long, Redis.shadowFieldName(aDataItem.getName()));
		dataItem.setValue(aDate.getTime());

		return dataItem;
	}

	private void storeDataDocItems(Pipeline aPipeline, String aKeyName,
								   DataDoc aDataDoc, StringBuilder aStringBuilder)
		throws RedisDSException
	{
		Date itemDate;
		DataItem shadowDataItem;
		String fieldName, fieldValue;
		Logger appLogger = mAppCtx.getLogger(this, "storeDataDocItems");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		aStringBuilder.append(String.format("HSET %s", mRedisDS.escapeKey(aKeyName)));
		for (DataItem dataItem : aDataDoc.getItems())
		{
			fieldName =  dataItemToFieldName(dataItem);
			if (aPipeline == null)
				mCmdConnection.hset(aKeyName, fieldName, mRedisDS.collapseEncryptValue(dataItem));
			else
				aPipeline.hset(aKeyName, fieldName, mRedisDS.collapseEncryptValue(dataItem));
			aStringBuilder.append(StrUtl.CHAR_SPACE);
			aStringBuilder.append(mRedisDS.escapeKey(fieldName));
			aStringBuilder.append(StrUtl.CHAR_SPACE);
			if (dataItem.isMultiValue())
				fieldValue = dataItem.getValuesCollapsed();
			else
				fieldValue = dataItem.getValue();
			aStringBuilder.append(mRedisDS.escapeValue(fieldValue));

// Support for RediSearch date/time shadow items.

			if ((fieldValue.length() > 0) && (Data.isDateOrTime(dataItem.getType())))
			{
				itemDate = dataItem.getValueAsDate();
				if (itemDate == null)
					appLogger.error(String.format("%s: Unable to parse '%s' format of '%s'", dataItem.getName(),
												  dataItem.getValue(), dataItem.getDataFormat()));
				else
				{
					shadowDataItem = shadowDataItem(dataItem, itemDate);
					if (aPipeline == null)
						mCmdConnection.hset(aKeyName, dataItemToFieldName(shadowDataItem), mRedisDS.collapseEncryptValue(shadowDataItem));
					else
						aPipeline.hset(aKeyName, dataItemToFieldName(shadowDataItem), mRedisDS.collapseEncryptValue(shadowDataItem));
					aStringBuilder.append(StrUtl.CHAR_SPACE);
					aStringBuilder.append(mRedisDS.escapeKey(shadowDataItem.getName()));
					aStringBuilder.append(StrUtl.CHAR_SPACE);
					aStringBuilder.append(mRedisDS.escapeValue(shadowDataItem.getValue()));
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds a data document as a hash to the Redis database.  The logic
	 * automatically handles data items with multiple values and performs
	 * encryption on the values if the Data.FEATURE_IS_SECRET is enabled.
	 * In addition, if the data document represents a hierarchy, then all
	 * child documents will automatically be stored as hashes and linked
	 * back to the parent data document.
	 *
	 * @see <a href="https://redis.io/commands/hset">Redis Command</a>
	 * @see <a href="https://github.com/lettuce-io/lettuce-core/wiki/Pipelining-and-command-flushing">Pipelining and Flushing</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataDoc aDataDoc)
		throws RedisDSException
	{
		String keyName;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Pipeline commandPipeline = null;
		Optional<DataDoc> optChildDocs = saveChildDocuments(commandPipeline, aDataDoc);
		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			if (dataItem.isValueAssigned())
				keyName = mRedisKey.moduleCore().redisHash().dataObject(aDataDoc).primaryId(dataItem.getValue()).name();
			else
				keyName = mRedisKey.moduleCore().redisHash().dataObject(aDataDoc).name();
		}
		else
			keyName = mRedisKey.moduleCore().redisHash().dataObject(aDataDoc).name();
		int fieldCount = aDataDoc.count();
		if ((fieldCount > 0) || (optChildDocs.isPresent()))
		{
			StringBuilder stringBuilder = new StringBuilder();
			storeDataDocItems(commandPipeline, keyName, aDataDoc, stringBuilder);
			if (optChildDocs.isPresent())
			{
				stringBuilder.append(String.format("%n"));
				DataDoc childDocs = optChildDocs.get();
				storeDataDocItems(commandPipeline, keyName, childDocs, stringBuilder);
			}
			mRedisDS.saveCommand(appLogger, stringBuilder.toString());
			mRedisDS.createCore().expire(keyName);
		}
		aDataDoc.addFeature(Redis.FEATURE_KEY_NAME, keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds a data document as a hash to the Redis database.  The logic
	 * automatically handles data items with multiple values and performs
	 * encryption on the values if the Data.FEATURE_IS_SECRET is enabled.
	 * In addition, if the data document represents a hierarchy, then all
	 * child documents will automatically be stored as hashes and linked
	 * back to the parent data document.
	 *
	 * @see <a href="https://redis.io/commands/hset">Redis Command</a>
	 * @see <a href="https://github.com/lettuce-io/lettuce-core/wiki/Pipelining-and-command-flushing">Pipelining and Flushing</a>
	 *
	 * @param aPipeline Pipeline instance
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(Pipeline aPipeline, DataDoc aDataDoc)
		throws RedisDSException
	{
		String keyName;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataDoc> optChildDocs = saveChildDocuments(aPipeline, aDataDoc);
		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			if (dataItem.isValueAssigned())
				keyName = mRedisKey.moduleCore().redisHash().dataObject(aDataDoc).primaryId(dataItem.getValue()).name();
			else
				keyName = mRedisKey.moduleCore().redisHash().dataObject(aDataDoc).name();
		}
		else
			keyName = mRedisKey.moduleCore().redisHash().dataObject(aDataDoc).name();
		int fieldCount = aDataDoc.count();
		if ((fieldCount > 0) || (optChildDocs.isPresent()))
		{
			StringBuilder stringBuilder = new StringBuilder();
			storeDataDocItems(aPipeline, keyName, aDataDoc, stringBuilder);
			if (optChildDocs.isPresent())
			{
				stringBuilder.append(String.format("%n"));
				DataDoc childDocs = optChildDocs.get();
				storeDataDocItems(aPipeline, keyName, childDocs, stringBuilder);
			}
			mRedisDS.saveCommand(appLogger, stringBuilder.toString());
			mRedisDS.createCore().expire(keyName);
		}
		aDataDoc.addFeature(Redis.FEATURE_KEY_NAME, keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected Optional<DataDoc> mapToDataDoc(Map<String,String> aFieldValues)
	{
		DataItem dataItem;
		Optional<DataItem> optDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "mapToDataDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataDoc> optDataDoc = Optional.empty();
		if ((aFieldValues != null) && (aFieldValues.size() > 0))
		{
			DataDoc dataDoc = new DataDoc("Redis Document");
			for (Map.Entry<String,String> entry : aFieldValues.entrySet())
			{
				optDataItem = mRedisField.toDataItem(entry.getKey());
				if (optDataItem.isPresent())
				{
					dataItem = optDataItem.get();
					mRedisDS.decryptExpandValue(dataItem, entry.getValue());
					dataDoc.add(dataItem);
				}
			}
			optDataDoc = Optional.of(dataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	/**
	 * Returns an optional data document from the Redis database based on
	 * the key name.
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
		DataItem dataItem;
		Optional<DataItem> optDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "getDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataDoc> optDataDoc = Optional.empty();
		if (StringUtils.isNotEmpty(aKeyName))
		{
			Map<String,String> fieldValues = mCmdConnection.hgetAll(aKeyName);
			String cmdString = String.format("HGETALL %s", mRedisDS.escapeKey(aKeyName));
			mRedisDS.saveCommand(appLogger, cmdString);
			if ((fieldValues != null) && (fieldValues.size() > 0))
			{
				Optional<DataDoc> optKeyDataDoc = mRedisKey.toDataDoc(aKeyName);
				if (optKeyDataDoc.isPresent())
				{
					DataDoc dataDoc = optKeyDataDoc.get();
					for (Map.Entry<String,String> entry : fieldValues.entrySet())
					{
						optDataItem = mRedisField.toDataItem(entry.getKey());
						if (optDataItem.isPresent())
						{
							dataItem = optDataItem.get();
							mRedisDS.decryptExpandValue(dataItem, entry.getValue());
							if (dataItem.isFeatureAssigned(Redis.FEATURE_IS_KEY))
							{
								Optional<DataDoc> optChildDoc = getDoc(dataItem.getValue());
								if (optChildDoc.isPresent())
									dataDoc.addChild(dataItem.getName(), optChildDoc.get());
							}
							else
								dataDoc.add(dataItem);
						}
					}
					optDataDoc = Optional.of(dataDoc);
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

		return optDataDoc;
	}

	/**
	 * Loads the data document with fields/values of a Redis database
	 * hash.  The key name is obtained from Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void loadDoc(DataDoc aDataDoc)
		throws RedisDSException
	{
		DataItem dataItem;
		Optional<DataItem> optDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "loadDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			Map<String,String> fieldValues = mCmdConnection.hgetAll(keyName);
			String cmdString = String.format("HGETALL %s", mRedisDS.escapeKey(keyName));
			mRedisDS.saveCommand(appLogger, cmdString);
			if ((fieldValues != null) && (fieldValues.size() > 0))
			{
				for (Map.Entry<String,String> entry : fieldValues.entrySet())
				{
					optDataItem = mRedisField.toDataItem(entry.getKey());
					if (optDataItem.isPresent())
					{
						dataItem = optDataItem.get();
						mRedisDS.decryptExpandValue(dataItem, entry.getValue());
						if (dataItem.isFeatureAssigned(Redis.FEATURE_IS_KEY))
						{
							Optional<DataDoc> optChildDoc = getDoc(dataItem.getValue());
							if (optChildDoc.isPresent())
								aDataDoc.addChild(dataItem.getName(), optChildDoc.get());
						}
						else
							aDataDoc.add(dataItem);
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns the count of the Redis stream entries stored in the database
	 * via the key name.  Since stream entries can vary in their definition,
	 * data documents are used to store/retrieve them.
	 *
	 * @see <a href="https://redis.io/topics/streams-intro">Redis Commands</a>
	 *
	 * @param aKeyName Key name
	 *
	 * @return Count of stream entries for key name
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long getDocCount(String aKeyName)
		throws RedisDSException
	{
		long documentCount;
		Logger appLogger = mAppCtx.getLogger(this, "getDocCount");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aKeyName))
			throw new RedisDSException("Undefined key name - cannot get stream document count.");

		mRedisDS.ensurePreconditions();
		documentCount = mCmdConnection.xlen(aKeyName);
		String cmdString = String.format("XLEN %s", mRedisDS.escapeKey(aKeyName));
		mRedisDS.saveCommand(appLogger, cmdString);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return documentCount;
	}

	/**
	 * Loads a list of data documents representing stream entries in the Redis database
	 * identified by the key name, starting and ending with parameter ids and limited
	 * to the count specified.  These entries are loaded in descending order.
	 *
	 * @see <a href="https://redis.io/topics/streams-intro">Redis Commands</a>
	 * @see <a href="https://github.com/redis/jedis/blob/master/src/test/java/redis/clients/jedis/commands/jedis/StreamsCommandsTest.java">Redis Stream Tests</a>
	 *
	 * @param aKeyName Key name
	 * @param anOrder Order of the stream load
	 * @param aStartId Starting id (timestamp or meta character)
	 * @param anEndId Ending id (timestamp or meta character)
	 * @param aCount Maximum count of data documents to load
	 *
	 * @return List of data document instances
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public List<DataDoc> loadDocs(String aKeyName, Data.Order anOrder, String aStartId, String anEndId, int aCount)
		throws RedisDSException
	{
		String cmdString;
		Map<String,String> fieldValues;
		List<StreamEntry> streamEntryList;
		Logger appLogger = mAppCtx.getLogger(this, "loadDocs");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		List<DataDoc> dataDocList = new ArrayList<>();
		if (anOrder == Data.Order.ASCENDING)
			streamEntryList = mCmdConnection.xrange(aKeyName, aStartId, anEndId, aCount);
		else
			streamEntryList = mCmdConnection.xrevrange(aKeyName, aStartId, anEndId, aCount);
		if (streamEntryList != null)
		{
			if (anOrder == Data.Order.ASCENDING)
				cmdString = String.format("XRANGE %s %s %s COUNT %d", mRedisDS.escapeKey(aKeyName), aStartId, anEndId, aCount);
			else
				cmdString = String.format("XREVRANGE %s %s %s COUNT %d", mRedisDS.escapeKey(aKeyName), aStartId, anEndId, aCount);
			mRedisDS.saveCommand(appLogger, cmdString);

			for (StreamEntry streamEntry : streamEntryList)
			{
				DataDoc dataDoc = new DataDoc("Stream Document");
				dataDoc.add(new DataItem.Builder().name("id").value(streamEntry.getID().toString()).build());
				fieldValues = streamEntry.getFields();
				if ((fieldValues != null) && (fieldValues.size() > 0))
				{
					for (Map.Entry<String, String> entry : fieldValues.entrySet())
						dataDoc.add(new DataItem.Builder().name(entry.getKey()).value(entry.getValue()).build());
				}
				dataDocList.add(dataDoc);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataDocList;
	}

	/**
	 * Updates the data document fields/values in the Redis database as
	 * hash data structures.  The updates are selective and based on
	 * whether the data item has the feature Data.FEATURE_IS_UPDATED
	 * enabled.  However, child data documents are ignored during
	 * this operation.
	 *
	 * @see <a href="https://redis.io/commands/hmset">Redis Command</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(DataDoc aDataDoc)
		throws RedisDSException
	{
		String fieldValue;
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			int fieldCount = aDataDoc.count();
			if (fieldCount > 0)
			{
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(String.format("HSET %s", mRedisDS.escapeKey(keyName)));
				for (DataItem dataItem : aDataDoc.getItems())
				{
					if (dataItem.isFeatureTrue(Data.FEATURE_IS_UPDATED))
					{
						mCmdConnection.hset(keyName, dataItemToFieldName(dataItem), mRedisDS.collapseEncryptValue(dataItem));
						if (dataItem.isMultiValue())
							fieldValue = dataItem.getValuesCollapsed();
						else
							fieldValue = dataItem.getValue();
						stringBuilder.append(StrUtl.CHAR_SPACE);
						stringBuilder.append(mRedisDS.escapeKey(dataItem.getName()));
						stringBuilder.append(StrUtl.CHAR_SPACE);
						stringBuilder.append(mRedisDS.escapeValue(fieldValue));
					}
				}
				String cmdString = String.format("%s", stringBuilder.toString());
				mRedisDS.saveCommand(appLogger, cmdString);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the data document fields/values in the Redis database as
	 * hash data structures.  The updates are based on a comparison of
	 * the two data document instance where new fields are added to
	 * the hash, updated fields are assigned new values and deleted
	 * fields are removed from the hash.  Please note that child data
	 * documents are ignored during this operation.
	 *
	 * @see <a href="https://redis.io/commands/hmset">Redis Command</a>
	 *
	 * @param aDataDoc1 Data document instance 1 (base)
	 * @param aDataDoc2 Data document instance 2 (changed)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(DataDoc aDataDoc1, DataDoc aDataDoc2)
		throws RedisDSException
	{
		String fieldName, fieldValue;
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataDoc1.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			DataDoc changeDataDoc;
			Optional<DataDoc> optDataDoc;

			DataDocDiff dataDocDiff = new DataDocDiff();
			dataDocDiff.compare(aDataDoc1, aDataDoc2);
			if (! dataDocDiff.isEqual())
			{

				optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_ADDED);
				if (optDataDoc.isPresent())
				{
					changeDataDoc = optDataDoc.get();
					StringBuilder stringBuilder = new StringBuilder(String.format("HSET %s", mRedisDS.escapeKey(keyName)));
					for (DataItem dataItem : changeDataDoc.getItems())
					{
						mCmdConnection.hset(keyName, dataItemToFieldName(dataItem), mRedisDS.collapseEncryptValue(dataItem));
						if (dataItem.isMultiValue())
							fieldValue = dataItem.getValuesCollapsed();
						else
							fieldValue = dataItem.getValue();
						stringBuilder.append(StrUtl.CHAR_SPACE);
						stringBuilder.append(mRedisDS.escapeKey(dataItem.getName()));
						stringBuilder.append(StrUtl.CHAR_SPACE);
						stringBuilder.append(mRedisDS.escapeValue(fieldValue));
					}
					mRedisDS.saveCommand(appLogger, stringBuilder.toString());
				}
				optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_UPDATED);
				if (optDataDoc.isPresent())
				{
					changeDataDoc = optDataDoc.get();
					StringBuilder stringBuilder = new StringBuilder(String.format("HSET %s", mRedisDS.escapeKey(keyName)));
					for (DataItem dataItem : changeDataDoc.getItems())
					{
						mCmdConnection.hset(keyName, dataItemToFieldName(dataItem), mRedisDS.collapseEncryptValue(dataItem));
						if (dataItem.isMultiValue())
							fieldValue = dataItem.getValuesCollapsed();
						else
							fieldValue = dataItem.getValue();
						stringBuilder.append(StrUtl.CHAR_SPACE);
						stringBuilder.append(mRedisDS.escapeKey(dataItem.getName()));
						stringBuilder.append(StrUtl.CHAR_SPACE);
						stringBuilder.append(mRedisDS.escapeValue(fieldValue));
					}
					mRedisDS.saveCommand(appLogger, stringBuilder.toString());
				}
				optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_DELETED);
				if (optDataDoc.isPresent())
				{
					int delCount = 0;
					changeDataDoc = optDataDoc.get();
					StringBuilder stringBuilder = new StringBuilder(String.format("HDEL %s", mRedisDS.escapeKey(keyName)));
					for (DataItem dataItem : changeDataDoc.getItems())
					{
						fieldName = dataItem.getName();
						if (! StringUtils.endsWith(fieldName, Redis.SHADOW_FIELD_MARKER_NAME))
						{
							mCmdConnection.hdel(keyName, fieldName);
							stringBuilder.append(StrUtl.CHAR_SPACE);
							stringBuilder.append(mRedisDS.escapeKey(fieldName));
							delCount++;
						}
					}
					if (delCount > 0)
						mRedisDS.saveCommand(appLogger, stringBuilder.toString());
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Queries Redis for the memory usage (in bytes) of the value
	 * data structure identified by the data document.
	 *
	 * @see <a href="https://redis.io/commands/memory-usage">Redis Command</a>
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return Memory usage in bytes
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long memoryUsage(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "memoryUsage");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		long memorySizeInBytes = 0;
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
			memorySizeInBytes = mRedisDS.createCore().memoryUsage(keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return memorySizeInBytes;
	}

	/**
	 * Deletes the parent data document and any child documents synchronously
	 * from the Redis database.
	 *
	 * @see <a href="https://redis.io/commands/del">Redis Command</a>
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

		mRedisDS.ensurePreconditions();
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			Map<String,String> fieldValues = mCmdConnection.hgetAll(keyName);
			String cmdString = String.format("HGETALL %s", mRedisDS.escapeKey(keyName));
			mRedisDS.saveCommand(appLogger, cmdString);
			if ((fieldValues != null) && (fieldValues.size() > 0))
			{
				Optional<DataDoc> optDataDoc = mRedisKey.toDataDoc(keyName);
				if (optDataDoc.isPresent())
				{
					DataItem dataItem;
					Optional<DataItem> optDataItem;

					for (Map.Entry<String,String> entry : fieldValues.entrySet())
					{
						optDataItem = mRedisField.toDataItem(entry.getKey());
						if (optDataItem.isPresent())
						{
							dataItem = optDataItem.get();
							mRedisDS.decryptExpandValue(dataItem, entry.getValue());
							if (dataItem.isFeatureAssigned(Redis.FEATURE_IS_KEY))
							{
								Optional<DataDoc> optChildDoc = getDoc(dataItem.getValue());
								if (optChildDoc.isPresent())
									delete(optChildDoc.get());
							}
						}
					}
				}
			}
			long keyCount = mRedisDS.createCore().delete(keyName);
			aDataDoc.disableFeature(Redis.FEATURE_KEY_NAME);
			if (keyCount != 1)
				appLogger.warn(String.format("%s - expected delete count of 1, but got %d", keyName, keyCount));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
