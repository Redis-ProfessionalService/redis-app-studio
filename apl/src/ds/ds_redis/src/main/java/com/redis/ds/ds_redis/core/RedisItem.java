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
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.Optional;
import java.util.Set;

/**
 * The RedisItem class handles Redis database operations centered around
 * {@link com.redis.foundation.data.DataItem} objects.  The Redis operations
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
public class RedisItem
{
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private final RedisKey mRedisKey;
	private final Jedis mCmdConnection;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the core objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisItem(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = new RedisKey(mRedisDS);
		mCmdConnection = aRedisDS.getCmdConnection();
	}

	/**
	 * Adds a data item as a string to the Redis database.  The logic
	 * automatically handles data items with multiple values and performs
	 * encryption on the values if the Data.FEATURE_IS_SECRET is enabled.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = mRedisKey.moduleCore().redisString().dataObject(aDataItem).name();
		mRedisDS.createCore().set(keyName, mRedisDS.collapseEncryptValue(aDataItem));
		aDataItem.addFeature(Redis.FEATURE_KEY_NAME, keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Update the values from the previously added data item.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
			mRedisDS.createCore().set(keyName, mRedisDS.collapseEncryptValue(aDataItem));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns an optional data item from the Redis database based on
	 * the key name.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataItem> getItem(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getItem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataItem> optDataItem = Optional.empty();
		String keyValue = mRedisDS.createCore().get(aKeyName);
		if (StringUtils.isNotEmpty(keyValue))
		{
			optDataItem = mRedisKey.toDataItem(aKeyName);
			if (optDataItem.isPresent())
			{
				DataItem dataItem = optDataItem.get();
				if (StringUtils.isNotEmpty(keyValue))
					mRedisDS.decryptExpandValue(dataItem, keyValue);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataItem;
	}

	/**
	 * Returns an optional data item from the Redis database based
	 * on the data item parameter.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataItem> getItem(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getItem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = Optional.empty();
		String keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
			optDataItem = getItem(keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataItem;
	}

	/**
	 * Adds a data item as a set to the Redis database.
	 *
	 * @see <a href="https://redis.io/commands/sadd">Redis Command</a>
	 *
	 * @param aDataItem Data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void addSet(DataItem aDataItem)
		throws RedisDSException
	{
		String cmdString;
		Logger appLogger = mAppCtx.getLogger(this, "addSet");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = mRedisKey.moduleCore().redisSet().dataObject(aDataItem).name();
		aDataItem.addFeature(Redis.FEATURE_KEY_NAME, keyName);
		if (aDataItem.isMultiValue())
		{
			String[] multiValues = aDataItem.getValuesArray();
			mCmdConnection.sadd(keyName, mRedisDS.encryptValues(multiValues));
			StringBuilder stringBuilder = new StringBuilder(String.format("SADD %s", mRedisDS.escapeKey(keyName)));
			for (String singleValue : multiValues)
			{
				stringBuilder.append(StrUtl.CHAR_SPACE);
				stringBuilder.append(mRedisDS.escapeValue(singleValue));
			}
			cmdString = stringBuilder.toString();
		}
		else
		{
			String singleValue = aDataItem.getValue();
			mCmdConnection.sadd(keyName, aDataItem.getValue());
			cmdString = String.format("SADD %s %s", mRedisDS.escapeKey(keyName), mRedisDS.escapeValue(singleValue));
		}
		mRedisDS.saveCommand(appLogger, cmdString);
		mRedisDS.createCore().expire(keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns an optional data item from the Redis database based
	 * on the key name parameter.  Assumes that the value was stored
	 * as a set data structure.
	 *
	 * @see <a href="https://redis.io/commands/smembers">Redis Command</a>
	 *
	 * @param aKeyName Key name
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataItem> getSet(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getItem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataItem> optDataItem = Optional.empty();
		Set<String> valueSet = mCmdConnection.smembers(aKeyName);
		String cmdString = String.format("SMEMBERS %s", mRedisDS.escapeKey(aKeyName));
		mRedisDS.saveCommand(appLogger, cmdString);
		if (valueSet.size() > 0)
		{
			Optional<DataItem> optKeyDataItem = mRedisKey.toDataItem(aKeyName);
			if (optKeyDataItem.isPresent())
			{
				DataItem dataItem = optKeyDataItem.get();
				for (String singleValue : valueSet)
					dataItem.addValue(mRedisDS.decryptExpandValue(singleValue));
				optDataItem = Optional.of(dataItem);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataItem;
	}

	/**
	 * Returns an optional data item from the Redis database based
	 * on the data item parameter.  Assumes that the value was stored
	 * as a set data structure.
	 *
	 * @see <a href="https://redis.io/commands/smembers">Redis Command</a>
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Optional data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataItem> getSet(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getSet");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = Optional.empty();
		String keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
			optDataItem = getSet(keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataItem;
	}

	/**
	 * Adds a data item as a HyperLogLog counter to the Redis database.
	 *
	 * @see <a href="https://redis.io/commands/pfadd">Redis Command</a>
	 *
	 * @param aDataItem Data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void addCounter(DataItem aDataItem)
		throws RedisDSException
	{
		String keyName, cmdString;
		Logger appLogger = mAppCtx.getLogger(this, "addCounter");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isEmpty(keyName))
		{
			keyName = mRedisKey.moduleCore().redisHyperLog().dataObject(aDataItem).name();
			aDataItem.addFeature(Redis.FEATURE_KEY_NAME, keyName);
		}
		if (aDataItem.isMultiValue())
		{
			String[] multiValues = aDataItem.getValuesArray();
			mCmdConnection.pfadd(keyName, mRedisDS.encryptValues(multiValues));
			StringBuilder stringBuilder = new StringBuilder(String.format("PFADD %s", mRedisDS.escapeKey(keyName)));
			for (String singleValue : multiValues)
			{
				stringBuilder.append(StrUtl.CHAR_SPACE);
				stringBuilder.append(mRedisDS.escapeValue(singleValue));
			}
			cmdString = stringBuilder.toString();
		}
		else
		{
			String singleValue = aDataItem.getValue();
			mCmdConnection.pfadd(keyName, aDataItem.getValue());
			cmdString = String.format("PFADD %s %s", mRedisDS.escapeKey(keyName), mRedisDS.escapeValue(singleValue));
		}
		mRedisDS.saveCommand(appLogger, cmdString);
		mRedisDS.createCore().expire(keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Return an approximated cardinality computed by the HyperLogLog
	 * data structure in the Redis database.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Count of unique values
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long getCounter(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getCounter");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
		long hllCounter = mCmdConnection.pfcount(keyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return hllCounter;
	}

	/**
	 * Queries Redis for the memory usage (in bytes) of the value
	 * data structure identified by the data item.
	 *
	 * @see <a href="https://redis.io/commands/memory-usage">Redis Command</a>
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Memory usage in bytes
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long memoryUsage(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "memoryUsage");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		long memorySizeInBytes = 0;
		if (aDataItem != null)
		{
			String keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
			memorySizeInBytes = mRedisDS.createCore().memoryUsage(keyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return memorySizeInBytes;
	}

	/**
	 * Deletes the data item and associated value synchronously from the
	 * Redis database.
	 *
	 * @see <a href="https://redis.io/commands/del">Redis Command</a>
	 *
	 * @param aDataItem Data item instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(DataItem aDataItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String keyName = aDataItem.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
			mRedisDS.createCore().delete(keyName);
	}
}
