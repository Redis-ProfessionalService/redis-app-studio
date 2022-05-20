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
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The RedisCore class is responsible for accessing the core Redis
 * commands via the Jedis programmatic library.  It designed to
 * simplify the use of core Foundation class objects like items,
 * documents and grids.
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
public class RedisCore
{
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private final Jedis mCmdConnection;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the core objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisCore(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mCmdConnection = aRedisDS.getCmdConnection();
	}

	/**
	 * Determines if a key already exists in the database.
	 *
	 * @param aKeyName Key name
	 *
	 * @return <i>true</i> if it exists and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public boolean exists(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "exists");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		boolean keyExists = mCmdConnection.exists(aKeyName);
		mRedisDS.saveCommand(appLogger, String.format("EXISTS %s", mRedisDS.escapeKey(aKeyName)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		return keyExists;
	}

	/**
	 * Expires the key/value based on the default configuration value specified.
	 *
	 * @param aKeyName Name of key
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void expire(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "expire");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		long keyExpirationInSeconds = mRedisDS.getKeyExpiration();
		if (keyExpirationInSeconds > 0)
		{
			mCmdConnection.expire(aKeyName, keyExpirationInSeconds);
			mRedisDS.saveCommand(appLogger, String.format("EXPIRE %s %d", mRedisDS.escapeKey(aKeyName), keyExpirationInSeconds));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);
	}

	/**
	 * Sets a key and its string value in the Redis database.  Optionally,
	 * if the timeout parameter is non-zero, then its expiration time will
	 * be assigned. If global value encryption is enabled, then the string
	 * value will be encrypted before it is written to the database.
	 *
	 * @see <a href="https://redis.io/commands/set">Redis Command</a>
	 *
	 * @param aKeyName Key name
	 * @param aValue String value
	 * @param aTimeoutSeconds Timeout in seconds (zero will bypass any expiration)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void set(String aKeyName, String aValue, int aTimeoutSeconds)
		throws RedisDSException
	{
		String cmdString;
		Logger appLogger = mAppCtx.getLogger(this, "set");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if (aTimeoutSeconds > 0)
		{
			mCmdConnection.setex(aKeyName, aTimeoutSeconds, mRedisDS.collapseEncryptValue(aValue));
			cmdString = String.format("SET %s %s EX %d", mRedisDS.escapeKey(aKeyName), mRedisDS.escapeValue(aValue), aTimeoutSeconds);
			mRedisDS.saveCommand(appLogger, cmdString);
		}
		else
		{
			mCmdConnection.set(aKeyName, mRedisDS.collapseEncryptValue(aValue));
			cmdString = String.format("SET %s %s", mRedisDS.escapeKey(aKeyName), mRedisDS.escapeValue(aValue));
			mRedisDS.saveCommand(appLogger, cmdString);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);
	}

	/**
	 * Sets a key and its string value in the Redis database.  If global
	 * value encryption is enabled, then the string value will be
	 * encrypted before it is written to the database.
	 *
	 * @see <a href="https://redis.io/commands/set">Redis Command</a>
	 *
	 * @param aKeyName Key name
	 * @param aValue String value
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void set(String aKeyName, String aValue)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "set");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		int keyExpirationInSeconds = mRedisDS.getKeyExpiration();
		set(aKeyName, aValue, Math.max(keyExpirationInSeconds, 0));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);
	}

	/**
	 * Retrieves the string value for the key name.
	 *
	 * @see <a href="https://redis.io/commands/get">Redis Command</a>
	 *
	 * @param aKeyName Key name
	 *
	 * @return String value
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public String get(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "get");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String valueString = mCmdConnection.get(aKeyName);
		mRedisDS.saveCommand(appLogger, String.format("GET %s", mRedisDS.escapeKey(aKeyName)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return mRedisDS.decryptExpandValue(valueString);
	}

	/**
	 * Adds a score and value to a sorted set in the Redis database.
	 *
	 * @param aKeyName Key name
	 * @param aScore Numeric score
	 * @param aValue Set value
	 *
	 * @throws RedisDSException Redis data source error
	 */
	public void sortedSetAdd(String aKeyName, long aScore, String aValue)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "sortedSetAdd");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if (StringUtils.isEmpty(aKeyName))
			throw new RedisDSException("Key name is empty or null.");

		mCmdConnection.zadd(aKeyName, aScore, aValue);
		mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(aKeyName), aScore, mRedisDS.escapeValue(aValue)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Increments the score of a sorted set element in the Redis database.
	 *
	 * @param aKeyName Key name
	 * @param anAmount Increment amount
	 * @param aValue Set value
	 *
	 * @throws RedisDSException Redis data source error
	 */
	public void sortedSetIncrementBy(String aKeyName, long anAmount, String aValue)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "sortedSetIncrementBy");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if (StringUtils.isEmpty(aKeyName))
			throw new RedisDSException("Key name is empty or null.");

		mCmdConnection.zincrby(aKeyName, anAmount, aValue);
		mRedisDS.saveCommand(appLogger, String.format("ZINCRBY %s %d %s", mRedisDS.escapeKey(aKeyName), anAmount, mRedisDS.escapeValue(aValue)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns the count of sorted set members in the Redis database.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Count of set members
	 *
	 * @throws RedisDSException Redis data source error
	 */
	public long sortedSetCount(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "sortedSetCount");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aKeyName))
			throw new RedisDSException("Key name is empty or null.");

		mRedisDS.ensurePreconditions();
		long sortedSetCount = mCmdConnection.zcard(aKeyName);
		mRedisDS.saveCommand(appLogger, String.format("ZCARD %s", mRedisDS.escapeKey(aKeyName)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return sortedSetCount;
	}

	/**
	 * Loads the list of sorted set values between the range parameters from
	 * the Redis database.
	 *
	 * Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH
	 *
	 * @param aKeyName Key name
	 * @param anOffsetStart Starting offset
	 * @param anOffsetEnd Ending offset
	 *
	 * @return List of set values
	 *
	 * @throws RedisDSException Redis data source error
	 */
	public List<String> sortedSetLoad(String aKeyName, long anOffsetStart, long anOffsetEnd)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "sortedSetLoad");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aKeyName))
			throw new RedisDSException("Key name is empty or null.");

		mRedisDS.ensurePreconditions();
		List<String> listValues = mCmdConnection.zrange(aKeyName, anOffsetStart, anOffsetEnd);
		mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(aKeyName), anOffsetStart, anOffsetEnd));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return listValues;
	}

	/**
	 * Deletes the sorted set member identify by the value parameter.
	 *
	 * @param aKeyName Key name
	 * @param aValue Member value
	 *
	 * @return <i>true</i> if successful, <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source error
	 */
	public boolean sortedSetDelete(String aKeyName, String aValue)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "sortedSetDelete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aKeyName))
			throw new RedisDSException("Key name is empty or null.");

		mRedisDS.ensurePreconditions();
		long deleteCount = mCmdConnection.zrem(aKeyName, aValue);
		mRedisDS.saveCommand(appLogger, String.format("ZREM %s %s", mRedisDS.escapeKey(aKeyName), mRedisDS.escapeValue(aValue)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return deleteCount > 0;
	}

	/**
	 * Create a lock item that can be used to acquire and release
	 * a resource lock in Redis.
	 *
	 * @param aKeyName Key name
	 *
	 * @return Lock item instance
	 */
	public DataItem createLock(String aKeyName)
	{
		String keyName = String.format ("%s:Lock", aKeyName);
		DataItem dataItem = new DataItem.Builder().name(keyName).value(UUID.randomUUID().toString()).build();
		dataItem.addFeature(Redis.FEATURE_LOCK_RELEASE, Redis.LOCK_RELEASE_TIMEOUT_DEFAULT);
		dataItem.addFeature(Redis.FEATURE_LOCK_WAITFOR, Redis.LOCK_WAITFOR_TIMEOUT_DEFAULT);

		return dataItem;
	}

	/**
	 * Create a lock item that can be used to acquire and release
	 * a resource lock in Redis.
	 *
	 * @param aKeyName Key name
	 * @param aReleaseInMilliseconds Amount of time to hold the lock (in milliseconds)
	 * @param aWaitForMilliseconds Amount of time to wait for the lock to free up (in milliseconds)
	 *
	 * @return Lock item instance
	 */
	public DataItem createLock(String aKeyName, long aReleaseInMilliseconds, long aWaitForMilliseconds)
	{
		String keyName = String.format ("%s:Lock", aKeyName);
		DataItem dataItem = new DataItem.Builder().name(keyName).value(UUID.randomUUID().toString()).build();
		dataItem.addFeature(Redis.FEATURE_LOCK_RELEASE, aReleaseInMilliseconds);
		dataItem.addFeature(Redis.FEATURE_LOCK_WAITFOR, aWaitForMilliseconds);

		return dataItem;
	}

	/**
	 * Acquires a temporary lock for a key-based operation.  You must call
	 * createLock() with the lock properties prior to invoking this method.
	 *
	 *  @see <a href="https://redis.io/topics/distlock">Redis Resource Locking</a>
	 *
	 * @param aLockItem Lock item instance
	 *
	 * @return <i>true</i> if lock was successfully acquired and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public boolean acquireLock(DataItem aLockItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "acquireLock");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isLockAcquired = false;
		String keyName = aLockItem.getName();
		String keyValue = aLockItem.getValue();
		long currentTimeInMilliseconds = System.currentTimeMillis();
		long lockReleaseMilliseconds = aLockItem.getFeatureAsLong(Redis.FEATURE_LOCK_RELEASE);
		long finishTimeInMilliseconds = currentTimeInMilliseconds + aLockItem.getFeatureAsLong(Redis.FEATURE_LOCK_WAITFOR);
		long sleepAmountInMilliseconds = Math.max(10, lockReleaseMilliseconds / 10); // sleep for 10% of the lock release time

		mRedisDS.ensurePreconditions();
		while ((! isLockAcquired) && (currentTimeInMilliseconds < finishTimeInMilliseconds))
		{
			long lockStatus = mCmdConnection.setnx(keyName, keyValue);
			isLockAcquired = lockStatus > 0;
			if (! isLockAcquired)
			{
				try
				{
					TimeUnit.MILLISECONDS.sleep(sleepAmountInMilliseconds);
				}
				catch (InterruptedException e)
				{
					appLogger.error(String.format("Sleep interruption: %s", e.getMessage()));
				}
				currentTimeInMilliseconds = System.currentTimeMillis();
			}
		}

		if (isLockAcquired)
		{
			String cmdString = String.format("SETNX %s %s", mRedisDS.escapeKey(keyName), mRedisDS.escapeValue(keyValue));
			mRedisDS.saveCommand(appLogger, cmdString);
			mCmdConnection.pexpire(keyName, lockReleaseMilliseconds);
			mRedisDS.saveCommand(appLogger, String.format("PEXPIRE %s, %d", mRedisDS.escapeKey(keyName), lockReleaseMilliseconds));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isLockAcquired;
	}

	/**
	 * Deletes the key and associated value synchronously from the
	 * Redis database.
	 *
	 * @see <a href="https://redis.io/commands/del">Redis Command</a>
	 *
	 * @param aKeyName Key name
	 *
	 * @return Count of successfully deleted key/value(s)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long delete(String aKeyName)
		throws RedisDSException
	{
		long deleteCount;
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if (StringUtils.isNotEmpty(aKeyName))
		{
			deleteCount = mCmdConnection.del(aKeyName);
			String cmdString = String.format("DEL %s", mRedisDS.escapeKey(aKeyName));
			mRedisDS.saveCommand(appLogger, cmdString);
		}
		else
			throw new RedisDSException("Key name is null.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return deleteCount;
	}

	/**
	 * Deletes the list of keys and associated values asynchronously
	 * from the Redis database.
	 *
	 * @see <a href="https://redis.io/commands/unlink">Redis Command</a>
	 *
	 * @param aKeyNameList List of key names
	 *
	 * @return Count of successfully deleted key/value(s)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long delete(List<String> aKeyNameList)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		long deleteCount = 0;
		if (aKeyNameList.size() > 0)
		{
			String[] keyNames = aKeyNameList.toArray(new String[0]);
			deleteCount = mCmdConnection.unlink(keyNames);
			StringBuilder stringBuilder = new StringBuilder("UNLINK");
			for (String keyName: aKeyNameList)
				stringBuilder.append(String.format(" %s", mRedisDS.escapeKey(keyName)));
			stringBuilder.append(StrUtl.CHAR_SPACE);
			mRedisDS.saveCommand(appLogger, stringBuilder.toString());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return deleteCount;
	}

	/**
	 * Deletes the key/value associated with the data item.
	 *
	 * @param aDataItem Data item instance.
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
			delete(keyName);
		else
		{
			String msgStr = "Unable to delete: Undefined key name";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Flushes the data from the Redis database.
	 *
	 * @throws RedisDSException Command operation failed
	 */
	public void flushDatabase()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "flushDatabase");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		mCmdConnection.flushDB();
		mRedisDS.saveCommand(appLogger, "FLUSHDB");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Releases a previoulsy acquired lock.
	 *
	 * @param aLockItem Lock item instance
	 *
	 * @throws RedisDSException Command operation failed
	 */
	public void releaseLock(DataItem aLockItem)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "releaseLock");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String keyName = aLockItem.getName();
		String lockValue = aLockItem.getValue();
		String keyValue = get(keyName);
		if (StringUtils.equals(keyValue, lockValue))
			delete(keyName);
		else
			appLogger.warn(String.format("Unable to release lock: '%s' and '%s' mismatch.", lockValue, keyValue));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Queries Redis for the memory usage (in bytes) of the value
	 * data structure identified by the key name.
	 *
	 * @see <a href="https://redis.io/commands/memory-usage">Redis Command</a>
	 *
	 * @param aKeyName Key name
	 *
	 * @return Memory usage in bytes
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long memoryUsage(String aKeyName)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "memoryUsage");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		long memorySizeInBytes = 0;
		if (StringUtils.isNotEmpty(aKeyName))
		{
			memorySizeInBytes = mCmdConnection.memoryUsage(aKeyName);
			mRedisDS.saveCommand(appLogger, String.format("MEMORY USAGE %s", mRedisDS.escapeKey(aKeyName)));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return memorySizeInBytes;
	}

	private DataDoc infoBufferToDataDoc(String anInfoBuffer)
	{
		Logger appLogger = mAppCtx.getLogger(this, "infoBufferToDataDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc dataDoc = new DataDoc("Redis Info");
		if (StringUtils.isNotEmpty(anInfoBuffer))
		{
			String infoSection = "Redis";
			String[] infoLines = anInfoBuffer.split("\\r?\\n");
			for (String infoLine : infoLines)
			{
				if (StringUtils.startsWith(infoLine, "#"))
					infoSection = infoLine.substring(2);
				else
				{
					String[] infoItems = infoLine.split(":");
					if (infoItems.length == 2)
						dataDoc.add(new DataItem.Builder().name(infoItems[0]).title(infoSection).value(infoItems[1]).build());
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataDoc;
	}

	/**
	 * Executes an information request call on the Redis Server and populates
	 * a Data document with the details.
	 *
	 * @see <a href="https://redis.io/commands/memory-usage">Redis Command</a>
	 *
	 * @return Memory usage in bytes
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public DataDoc info()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "info");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String infoBuffer = mCmdConnection.info();
		mRedisDS.saveCommand(appLogger, "INFO");
		DataDoc dataDoc = infoBufferToDataDoc(infoBuffer);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataDoc;
	}

	/**
	 * Executes an information request call on the Redis Server and populates
	 * a Data document with the details.
	 *
	 * @param aSection Defined in Redis class as REDIS_INFO_SECTION_xxxx
	 *
	 * @see <a href="https://redis.io/commands/memory-usage">Redis Command</a>
	 *
	 * @return Memory usage in bytes
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public DataDoc info(String aSection)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "info");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String infoBuffer = mCmdConnection.info(aSection);
		mRedisDS.saveCommand(appLogger, String.format("INFO %s", aSection));
		DataDoc dataDoc = infoBufferToDataDoc(infoBuffer);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataDoc;
	}

	/**
	 * Identifies if the feature name is standard to the Redis
	 * data source package.
	 *
	 * @param aName Name of the feature
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	private boolean isFeatureStandard(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
		{
			switch (aName)
			{
				case Data.FEATURE_IS_PRIMARY:
				case Data.FEATURE_IS_REQUIRED:
				case Data.FEATURE_IS_VISIBLE:
				case Data.FEATURE_IS_SECRET:
				case Data.FEATURE_IS_LATITUDE:
				case Data.FEATURE_IS_LONGITUDE:
				case Data.FEATURE_IS_HIDDEN:
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
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_PRIMARY).title("Is Primary").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_REQUIRED).title("Is Required").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_VISIBLE).title("Is Visible").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_SECRET).title("Is Secret").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_LATITUDE).title("Is Latitude").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_LONGITUDE).title("Is Longitude").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_HIDDEN).title("Is Hidden").build());

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
			mapFeatures = dataItem.getFeatures();
			for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				dataGrid.setValueByName(featureEntry.getKey(), featureEntry.getValue());
			dataGrid.addRow();
		}

		return dataGrid;
	}
}
