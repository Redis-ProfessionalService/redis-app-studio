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

package com.redis.ds.ds_redis;

import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.core.RedisDoc;
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.ds.ds_redis.core.RedisItem;
import com.redis.ds.ds_redis.graph.RedisGraphs;
import com.redis.ds.ds_redis.json.RedisJson;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.ds.ds_redis.time_series.RedisTimeseries;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.CfgMgr;
import com.redis.foundation.crypt.Secret;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.util.Pool;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Redis Data Source class manages connections with one or more
 * Redis Server processes.  It developed against the Jedis programmatic
 * library which is maintained and supported by Redis.
 *
 * You can use RedisDS to instantiate subclasses for Redis core, graph,
 * JSON, search and time series operations within a Redis database.
 *
 * @see <a href="https://redis.io/commands">OSS Redis Commands</a>
 * @see <a href="https://github.com/redis/jedis">Jedis GitHub site</a>
 * @see <a href="https://www.baeldung.com/jedis-java-redis-client-library">Intro to Jedis – the Java Redis Client Library</a>
 * @see <a href="https://commons.apache.org/proper/commons-pool/index.html">Apache Commons Pool</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisDS
{
	private Secret mSecret;
	private Jedis mCmdStream;
	private final AppCtx mAppCtx;
	private final CfgMgr mCfgMgr;
	private Jedis mCmdOperations;
	private final RedisKey mRedisKey;
	private PrintWriter mPrintWriter;
	private int mExpirationInSeconds;
	private JedisPool mConnectionPool;
	private long mStreamCommandsLimit;
	private final StopWatch mStopWatch;
	private boolean mIsCommandStreamActive;
	private Redis.Encryption mEncryptionOption;
	private String aMarkerName = StringUtils.EMPTY;
	private String mStreamKeyName = StringUtils.EMPTY;
	private String mApplicationPrefix = StringUtils.EMPTY;

	/**
     * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	*/
	public RedisDS(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
		mStopWatch = new StopWatch();
		mIsCommandStreamActive = true;
		mRedisKey = new RedisKey(this);
		mEncryptionOption = Redis.Encryption.Field;
		mStreamCommandsLimit = Redis.STREAM_LIMIT_DEFAULT;
		mSecret = new Secret(Redis.ENCRYPTION_SECRET_DEFAULT);
		mCfgMgr = new CfgMgr(mAppCtx, Redis.CFG_PROPERTY_PREFIX);
	}

	/**
	 * Assigns the configuration property prefix to the document data source.
	 *
	 * @param aPropertyPrefix Property prefix.
	 */
	public void setCfgPropertyPrefix(String aPropertyPrefix)
	{
		mCfgMgr.setCfgPropertyPrefix(aPropertyPrefix);
	}

	/**
	 * Returns the application context instance for subclasses to utilize.
	 *
	 * @return Application context instance
	 */
	public AppCtx getAppCtx()
	{
		return mAppCtx;
	}

	/**
	 * Returns the configuration manager instance for subclasses to utilize.
	 *
	 * @return Configuration manager instance
	 */
	public CfgMgr getCfgMgr()
	{
		return mCfgMgr;
	}

	/**
	 * Returns the Redis key name instance for subclasses to utilize.
	 *
	 * @return Key name instance
	 */
	public RedisKey getRedisKey()
	{
		return mRedisKey;
	}

	/**
	 * Assigns the command stream active flag - if <i>true</i> then
	 * commands will be captured in the file/stream storage.  Otherwise,
	 * those commands will be ignored.
	 *
	 * @param anIsCommandStreamActive  <i>true</i> or <i>false</i>
	 */
	public void setCommandStreamActiveFlag(boolean anIsCommandStreamActive)
	{
		mIsCommandStreamActive = anIsCommandStreamActive;
	}

	/**
	 * Assigns an encryption option for the data values stored in Redis.
	 *
	 * @param anEncryptionOption Encryption option
	 */
	public void setEncryptionOption(Redis.Encryption anEncryptionOption)
	{
		mEncryptionOption = anEncryptionOption;
	}

	/**
	 * Returns the Redis data values encryption option.
	 *
	 * @return Redis data encryption option
	 */
	public Redis.Encryption getEncryptionOption()
	{
		return mEncryptionOption;
	}

	/**
	 * Returns the Redis command connection instance for subclasses to utilize.
	 *
	 * @return Jedis connection instance
	 */
	public Jedis getCmdConnection()
	{
		return mCmdOperations;
	}

	/**
	 * Assigns a default application prefix for key name building.
	 *
	 * @param aPrefix Application prefix
	 */
	public void setApplicationPrefix(String aPrefix)
	{
		if (StringUtils.isNotEmpty(aPrefix))
			mApplicationPrefix = aPrefix;
	}

	/**
	 * Returns the application prefix string.
	 *
	 * @return  Application prefix string
	 */
	public String getApplicationPrefix()
	{
		return mApplicationPrefix;
	}

	/**
	 * Assigns a default key expiration in seconds.  Once set,
	 * then every new key/value created will have an expiration
	 * assigned to it.
	 *
	 * @param aSeconds Seconds to expire key/value
	 */
	public void setKeyExpiration(int aSeconds)
	{
		if (aSeconds > 0)
			mExpirationInSeconds = aSeconds;
	}

	/**
	 * Returns the key expiration (in seconds) count.
	 *
	 * @return Expiration time in seconds
	 */
	public int getKeyExpiration()
	{
		return mExpirationInSeconds;
	}

	/**
	 * Create a key name (using the Redis App Studio standard format) based on the application
	 * prefix string.
	 *
	 * @return Key name
	 */
	public String streamKeyName()
	{
		mStreamKeyName = mRedisKey.moduleCore().redisStream().dataName(Redis.STREAM_COMMAND_DATA_NAME).name();
		return mStreamKeyName;
	}

	/**
	 * Assigns a maximum number of commands that can be saved
	 * to the tracking stream.
	 *
	 * @param aMaxCommands Maximum number of commands
	 */
	public void setStreamCommandLimit(long aMaxCommands)
	{
		mStreamCommandsLimit = aMaxCommands;
	}

	/**
	 * Return the host name configuration parameter.
	 *
	 * @return Host name
	 */
	public String getHostName()
	{
		return mCfgMgr.getString("host_name", Redis.HOST_NAME_DEFAULT);
	}

	/**
	 * Return the port number configuration parameter.
	 *
	 * @return Port number
	 */
	public int getPortNumber()
	{
		return mCfgMgr.getInteger("port_number", Redis.PORT_NUMBER_DEFAULT);
	}

	/**
	 * Creates a connection pool to manage concurrent connections to Redis Servers.
	 *
	 * @return JedisPool Pool instance
	 */
	public JedisPool createPool()
	{
		JedisPool jedisPool;
		boolean poolTestOnIdle, poolTestOnBorrow, poolTestOnReturn, poolBlockOnLimit;
		Logger appLogger = mAppCtx.getLogger(this, "createPool");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mExpirationInSeconds = mCfgMgr.getInteger("cache_expiration_time", 0);

// Acknowledging that the default assignment of a secret is bad practice, however this is being done for demo purposes only.

		mSecret = new Secret(mCfgMgr.getString("encrypt_password", Redis.ENCRYPTION_SECRET_DEFAULT));

		mStreamCommandsLimit = mCfgMgr.getLong("stream_command_limit", Redis.STREAM_LIMIT_DEFAULT);
		boolean isSSL = mCfgMgr.isStringTrue("ssl_enabled");
		String dbAccount = mCfgMgr.getString("database_account", StringUtils.EMPTY);
		String dbPassword = mCfgMgr.getString("database_password", StringUtils.EMPTY);
		int operationTimeout = mCfgMgr.getInteger("operation_timeout", Redis.TIMEOUT_DEFAULT);
		int dbId = mCfgMgr.getInteger("database_id", Redis.DBID_DEFAULT);
		String hostName = mCfgMgr.getString("host_name", Redis.HOST_NAME_DEFAULT);
		int portNumber = mCfgMgr.getInteger("port_number", Redis.PORT_NUMBER_DEFAULT);
		int poolMaxConnections = mCfgMgr.getInteger("pool_max_connections", Redis.POOL_MAX_TOTAL_CONNECTIONS);
		int poolMaxIdleConnections = mCfgMgr.getInteger("pool_max_idle_connections", Redis.POOL_MAX_IDLE_CONNECTIONS);
		int poolMinIdleConnections = mCfgMgr.getInteger("pool_min_idle_connections", Redis.POOL_MIN_IDLE_CONNECTIONS);
		if (mCfgMgr.isAssigned("pool_test_on_idle"))
			poolTestOnIdle = mCfgMgr.isStringTrue("pool_test_on_idle");
		else
			poolTestOnIdle = true;
		if (mCfgMgr.isAssigned("pool_test_on_borrow"))
			poolTestOnBorrow = mCfgMgr.isStringTrue("pool_test_on_borrow");
		else
			poolTestOnBorrow = true;
		if (mCfgMgr.isAssigned("pool_test_on_return"))
			poolTestOnReturn = mCfgMgr.isStringTrue("pool_test_on_return");
		else
			poolTestOnReturn = true;
		if (mCfgMgr.isAssigned("pool_block_on_limit"))
			poolBlockOnLimit = mCfgMgr.isStringTrue("pool_block_on_limit");
		else
			poolBlockOnLimit = true;
		String clientName = mCfgMgr.getString("application_prefix", Redis.APPLICATION_PREFIX_DEFAULT);
		appLogger.debug(String.format("hostName = %s, portNumber = %d, dbId = %d, clientName = %s, dbAccount = '%s', dbPassword = '%s', operationTimeout = %d",
									  hostName, portNumber, dbId, clientName, dbAccount, dbPassword, operationTimeout));

		GenericObjectPoolConfig<Jedis> jedisPoolConfig = new GenericObjectPoolConfig<Jedis>();
		jedisPoolConfig.setMaxTotal(poolMaxConnections);
		jedisPoolConfig.setMaxIdle(poolMaxIdleConnections);
		jedisPoolConfig.setMinIdle(poolMinIdleConnections);
		jedisPoolConfig.setTestOnBorrow(poolTestOnBorrow);
		jedisPoolConfig.setTestOnReturn(poolTestOnReturn);
		jedisPoolConfig.setTestWhileIdle(poolTestOnIdle);
		jedisPoolConfig.setBlockWhenExhausted(poolBlockOnLimit);

		if ((StringUtils.isNotEmpty(dbAccount)) && (StringUtils.isNotEmpty(dbPassword)))
			jedisPool = new JedisPool(jedisPoolConfig, hostName, portNumber, operationTimeout, dbAccount, dbPassword, dbId, clientName, isSSL);
		else
			jedisPool = new JedisPool(jedisPoolConfig, hostName, portNumber, operationTimeout, isSSL);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return jedisPool;
	}

	/**
	 * Assigns a connection pool for use by the Redis framework.
	 *
	 * @param aConnectionPool Connection pool instance
	 */
	public void setConnectionPool(JedisPool aConnectionPool)
	{
		mConnectionPool = aConnectionPool;
	}

	/**
	 * Returns the internally managed Jedis connection pool instance.
	 *
	 * @return Jedis connection pool instance
	 */
	public Pool<Jedis> getConnectionPool()
	{
		return mConnectionPool;
	}

	/**
	 * Opens an absolute path/file name to capture any Redis 'redis-cli'
	 * compatible commands while the Redis channel is open.
	 *
	 * @param aPathFileName Absolute path/file name
	 *
	 * @throws IOException Failure to open path/file
	 */
	public void openCaptureWithFile(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "openCaptureWithFile");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mPrintWriter == null)
			mPrintWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a Redis stream using the key name provided to capture
	 * any Redis 'redis-cli' compatible commands while the Redis
	 * channel is open.
	 *
	 * @param aKeyName Key name
	 */
	public void openCaptureWithStream(String aKeyName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "openCaptureWithStream");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mStreamKeyName = aKeyName;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns the internally managed stream key name.
	 *
	 * @return Stream key name
	 */
	public String getStreamKeyName()
	{
		return mStreamKeyName;
	}

	/**
	 * Clears the internal stream key name - thus stopping any further
	 * capturing of Redis commands.
	 */
	public void clearSteamKeyName()
	{
		mStreamKeyName = StringUtils.EMPTY;
	}

	/**
	 * Creates a client instance configured with properties that will be
	 * used to establish one or more connections to a Redis Server.
	 *
	 * @param aHostName Host name where Redis Server is running
	 * @param aPortNumber Port number Redis Server is listening on
	 * @param aDBId Data base id
	 * @param aDBAccountName Database account name
	 * @param aDBAccountPassword Database password
	 * @param aSSLEnabled Is SSL enabled
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void open(String aHostName, int aPortNumber, int aDBId,
					 String aDBAccountName, String aDBAccountPassword,
					 boolean aSSLEnabled)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "open");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".host_name", aHostName);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".port_number", aPortNumber);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".database_id", aDBId);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".database_account", aDBAccountName);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".database_password", aDBAccountPassword);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".ssl_enabled", aSSLEnabled);
		if (mConnectionPool == null)
			setConnectionPool(createPool());
		if (mCmdStream == null)
			mCmdStream = borrowConnection();
		else if (mCmdStream.isBroken())
		{
			mConnectionPool.returnBrokenResource(mCmdStream);
			mCmdStream = mConnectionPool.getResource();
		}
		if (mCmdOperations == null)
			mCmdOperations = borrowConnection();
		else if (mCmdOperations.isBroken())
		{
			mConnectionPool.returnBrokenResource(mCmdOperations);
			mCmdOperations = mConnectionPool.getResource();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a client instance configured with properties that will be
	 * used to establish one or more connections to a Redis Server.
	 *
	 * @param aHostName Host name where Redis Server is running
	 * @param aPortNumber Port number Redis Server is listening on
	 * @param aDBId Data base id
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void open(String aHostName, int aPortNumber, int aDBId)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "open");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".host_name", aHostName);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".port_number", aPortNumber);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".database_id", aDBId);
		setConnectionPool(createPool());
		mCmdStream = borrowConnection();
		mCmdOperations = borrowConnection();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a client instance configured with properties that will be
	 * used to establish one or more connections to a Redis Server.
	 *
	 * @param aHostName Host name where Redis Server is running
	 * @param aPortNumber Port number Redis Server is listening on
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void open(String aHostName, int aPortNumber)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "open");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".host_name", aHostName);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".port_number", aPortNumber);
		setConnectionPool(createPool());
		mCmdStream = borrowConnection();
		mCmdOperations = borrowConnection();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a client instance configured with properties that will be
	 * used to establish one or more connections to a Redis Server.
	 *
	 * @param anApplicationPrefix Application prefix (used for key creation)
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void open(String anApplicationPrefix)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "open");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		setApplicationPrefix(anApplicationPrefix);
		mAppCtx.addProperty(Redis.CFG_PROPERTY_PREFIX + ".application_prefix", anApplicationPrefix);
		setConnectionPool(createPool());
		mCmdStream = borrowConnection();
		mCmdOperations = borrowConnection();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Borrows a Redis connection from the connection pool.
	 *
	 * @return Jedis connection instance
	 *
	 * @throws RedisDSException Redis data source error
	 */
	public Jedis borrowConnection()
		throws RedisDSException
	{
		if (mConnectionPool == null)
			throw new RedisDSException("Connection pool has not been initialized.");
		else
			return mConnectionPool.getResource();
	}

	/**
	 * Returns a (previously borrowed) Redis connection to the
	 * connection pool.
	 *
	 * @param aConnection Jedis connection instance
	 */
	public void returnConnection(Jedis aConnection)
	{
		if (aConnection != null)
			mConnectionPool.returnResource(aConnection);
	}

	/**
	 * Escapes a key name as a parameter for the 'redis-cli' utility.
	 *
	 * @param aName Key Name
	 *
	 * @return Escaped version of the parameter
	 */
	public String escapeKey(String aName)
	{
		if ((StringUtils.containsAny(aName, StrUtl.CHAR_SPACE)) ||
			(StringUtils.containsAny(aName, StrUtl.CHAR_SGLQUOTE)))
			return String.format("\"%s\"", aName);

		return aName;
	}

	/**
	 * Escapes a value as a parameter for the 'redis-cli' utility.
	 *
	 * @param aValue Value string
	 *
	 * @return Escaped version of the parameter
	 */
	public String escapeValue(String aValue)
	{
		String value = StringUtils.replace(aValue, "%", "%%");
		value = StrUtl.escapeChar(value, StrUtl.CHAR_DBLQUOTE, StrUtl.CHAR_BACKSLASH);
		value = value.replaceAll("\\r\\n|\\r|\\n", " ");
		if ((StringUtils.containsAny(value, StrUtl.CHAR_SPACE)) ||
			(StringUtils.containsAny(value, StrUtl.CHAR_SGLQUOTE)))
			return String.format("'%s'", value);
		else
			return value;
	}

	/**
	 * Collapse and encrypt the value as a parameter for the 'redis-cli' utility.
	 *
	 * @param aValue Value string
	 *
	 * @return Collapsed version of the parameter
	 */
	public String collapseEncryptValue(String aValue)
	{
		if (mEncryptionOption != Redis.Encryption.None)
			return mSecret.encrypt(aValue);
		else
			return aValue;
	}

	/**
	 * Collapse the value (mutli-value down to a single string) as a parameter
	 * for the 'redis-cli' utility
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Collapsed version of the parameter
	 */
	public String collapseValue(DataItem aDataItem)
	{
		String itemValue;

		if (aDataItem.isMultiValue())
			itemValue = aDataItem.getValuesCollapsed();
		else
			itemValue = aDataItem.getValue();

		return itemValue;
	}

	/**
	 * Collapse and encrypt the value (mutli-value down to a single string) as a parameter
	 * for the 'redis-cli' utility
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Collapsed and encrypted version of the parameter
	 */
	public String collapseEncryptValue(DataItem aDataItem)
	{
		String itemValue;

		if (aDataItem.isMultiValue())
			itemValue = aDataItem.getValuesCollapsed();
		else
			itemValue = aDataItem.getValue();

		switch (mEncryptionOption)
		{
			case All:
				return mSecret.encrypt(itemValue);
			case Field:
				if (aDataItem.isFeatureTrue(Data.FEATURE_IS_SECRET))
					return mSecret.encrypt(itemValue);
				else
					return itemValue;
			default:
				return itemValue;
		}
	}

	/**
	 * Encrypt an array of string values.
	 *
	 * @param aMultiValues Multi-value array of strings
	 *
	 * @return Encrypted array of string
	 */
	public String[] encryptValues(String[] aMultiValues)
	{
		if (mEncryptionOption != Redis.Encryption.None)
		{
			for (int offset = 0; offset < aMultiValues.length; offset++)
				aMultiValues[offset] = mSecret.encrypt(aMultiValues[offset]);
		}

		return aMultiValues;
	}

	/**
	 * Decrypt and expand a string value.
	 *
	 * @param aValue Encrypted and collapsed string
	 *
	 * @return Decrypted and expanded string
	 */
	public String decryptExpandValue(String aValue)
	{
		if (mEncryptionOption != Redis.Encryption.None)
			return mSecret.decrypt(aValue);
		else
			return aValue;
	}

	/**
	 * Decrypt and expand a string value into a data item.
	 *
	 * @param aDataItem Data item instance
	 * @param aValue Encrypted and collapsed string
	 */
	public void decryptExpandValue(DataItem aDataItem, String aValue)
	{
		String itemValue;

		switch (mEncryptionOption)
		{
			case All:
				itemValue = mSecret.decrypt(aValue);
				break;
			case Field:
				if (aDataItem.isFeatureTrue(Data.FEATURE_IS_SECRET))
					itemValue = mSecret.decrypt(aValue);
				else
					itemValue = aValue;
				break;
			default:
				itemValue = aValue;
				break;
		}

		if (aDataItem.isFeatureTrue(Data.FEATURE_IS_MULTIVALUE))
			aDataItem.expandAndSetValues(itemValue);
		else
			aDataItem.setValue(itemValue);
	}

	/**
	 * Returns an escaped value from the data item instance parameter.
	 *
	 * @param aDataItem Data Item instance
	 *
	 * @return Escaped value
	 */
	public String getValue(DataItem aDataItem)
	{
		return escapeValue(aDataItem.getValue());
	}

	/**
	 * Ensures that the state of the Redis data source is suitable for
	 * Redis command operations.
	 *
	 * @throws RedisDSException Redis data source is not ready
	 */
	public void ensurePreconditions()
		throws RedisDSException
	{
		if (mConnectionPool == null)
			throw new RedisDSException("Connection pool has not been initialized.");
		else if (mCmdOperations == null)
			throw new RedisDSException("Command operation connection is not open.");
		else if (StringUtils.isEmpty(mApplicationPrefix))
			throw new RedisDSException("Application prefix string is unassigned.");

/* The following logic was added when a Redis operation failed with a broken connection,
but the current connection was not recovered.  The logic below attempts to do that. */

		if (mCmdOperations.isBroken())
		{
			mConnectionPool.returnBrokenResource(mCmdOperations);
			mCmdOperations = mConnectionPool.getResource();
		}
	}

	/**
	 * Writes the 'redis-cli' command to the application logger
	 *
	 * @param aLogger Application logger instance
	 * @param aCommand Command string
	 */
	public void saveCommand(Logger aLogger, String aCommand)
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveCommand");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((mIsCommandStreamActive) && (StringUtils.isNotEmpty(aCommand)))
		{
			if (mPrintWriter != null)
				mPrintWriter.printf("%s%n", aCommand);
			if (aLogger != null)
				aLogger.debug(aCommand);
			if (StringUtils.isNotEmpty(mStreamKeyName))
			{
				// Filter Stream and Memory commands for now - may disable in the future
				if ((! StringUtils.startsWith(aCommand, "X")) && (! StringUtils.startsWith(aCommand, "MEMORY")))
				{
					Map<String, String> hashMap = new HashMap<>();
					if (aCommand.indexOf(StrUtl.CHAR_SPACE) > 0)
					{
						String[] commandParameters = aCommand.split(" ", 2);
						hashMap.put("redis_command", commandParameters[0]);
						hashMap.put("redis_parameters", commandParameters[1]);
					}
					else
					{
						hashMap.put("redis_command", aCommand);
						hashMap.put("redis_parameters", StringUtils.EMPTY);
					}
					mCmdStream.xadd(mStreamKeyName, XAddParams.xAddParams().maxLen(mStreamCommandsLimit), hashMap);
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Saves the Redis command to stream for auditing purposes.  This method
	 * is typically leveraged by other Redis module libraries.
	 *
	 * @param aCommand Redis command string
	 */
	public void saveCommand(String aCommand)
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveCommand");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isNotEmpty(aCommand))
		{
			if (StringUtils.isNotEmpty(mStreamKeyName))
			{
				// Filter Stream and Memory commands for now - may disable in the future
				if ((! StringUtils.startsWith(aCommand, "X")) && (! StringUtils.startsWith(aCommand, "MEMORY")))
				{
					Map<String, String> hashMap = new HashMap<>();
					if (aCommand.indexOf(StrUtl.CHAR_SPACE) > 0)
					{
						String[] commandParameters = aCommand.split(" ", 2);
						hashMap.put("redis_command", commandParameters[0]);
						hashMap.put("redis_parameters", commandParameters[1]);
					}
					else
					{
						hashMap.put("redis_command", aCommand);
						hashMap.put("redis_parameters", StringUtils.EMPTY);
					}
					mCmdStream.xadd(mStreamKeyName, XAddParams.xAddParams().maxLen(mStreamCommandsLimit), hashMap);
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Capture the start of a marker name in the log/stream.
	 *
	 * @param aName Marker name
	 */
	public void startMarker(String aName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "startMarker");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		finishMarker();
		if (mPrintWriter != null)
			mPrintWriter.printf(String.format("SET MarkerStart %s EX 1%n", escapeValue(aName)));
		if (StringUtils.isNotEmpty(mStreamKeyName))
		{
			Map<String, String> hashMap = new HashMap<>();
			hashMap.put("redis_marker_start", aName);
			mCmdStream.xadd(mStreamKeyName, XAddParams.xAddParams().maxLen(mStreamCommandsLimit), hashMap);
		}
		mStopWatch.start();
		aMarkerName = aName;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Capture the end of a marker name in the log/stream.  In addition,
	 * if the memory-used-in-bytes parameter is non-zero, then capture
	 * those details too.
	 *
	 * @param aName Marker name
	 * @param aMemoryInBytes Memory in bytes or zero (which will be ignored)
	 */
	public void finishMarker(String aName, long aMemoryInBytes)
	{
		String markerMessage;
		Logger appLogger = mAppCtx.getLogger(this, "finishMarker");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isNotEmpty(aName))
		{
			mStopWatch.stop();
			if (aMemoryInBytes == 0)
				markerMessage = String.format("[%d ms] %s", mStopWatch.getTime(), aName);
			else
			{
				DecimalFormat decimalFormat = new DecimalFormat("#.##");
				decimalFormat.setGroupingUsed(true);
				decimalFormat.setGroupingSize(3);
				markerMessage = String.format("[%d ms - %s bytes] %s", mStopWatch.getTime(), decimalFormat.format(aMemoryInBytes), aName);
			}
			if (mPrintWriter != null)
				mPrintWriter.printf(String.format("SET MarkerFinish %s EX 1%n", escapeValue(markerMessage)));
			if (StringUtils.isNotEmpty(mStreamKeyName))
			{
				Map<String, String> hashMap = new HashMap<>();
				hashMap.put("redis_marker_finish", markerMessage);
				// Fire and forget - reply message
				mCmdStream.xadd(mStreamKeyName, XAddParams.xAddParams().maxLen(mStreamCommandsLimit), hashMap);
			}
			mStopWatch.reset();
			aMarkerName = StringUtils.EMPTY;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Capture the end of a marker name in the log/stream.  In addition,
	 * if the memory-used-in-bytes parameter is non-zero, then capture
	 * those details too.
	 *
	 * @param aMemoryInBytes Memory in bytes or zero (which will be ignored)
	 */
	public void finishMarker(long aMemoryInBytes)
	{
		finishMarker(aMarkerName, aMemoryInBytes);
	}

	/**
	 * Capture the end of a marker name in the log/stream.
	 */
	public void finishMarker()
	{
		finishMarker(aMarkerName, 0);
	}

	private String incrementStreamId(String aStartId)
	{
		int offset = aStartId.indexOf(StrUtl.CHAR_HYPHEN);
		if (offset > 0)
		{
			String timeBase = aStartId.substring(0, offset);
			return String.format("%d-0", Long.parseLong(timeBase)+1);
		}
		else
		{
			long startId = Long.parseLong(aStartId) + 1;
			return Long.toString(startId);
		}
	}

	/**
	 * Save the Redis stream data set identified by the key name to the PrintWriter
	 * stream in a format compatible with 'redis-cli'.
	 *
	 * @param aKeyName Redis stream key name
	 * @param aPW PrintWrite stream instance
	 *
	 * @throws RedisDSException Redis operation exception
	 */
	public void saveStreamAsWriter(String aKeyName, PrintWriter aPW)
		throws RedisDSException
	{
		long loadCount;
		String priorStartId;
		List<DataDoc> dataDocList;
		Logger appLogger = mAppCtx.getLogger(this, "saveStreamAsWriter");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(aKeyName)) && (aPW != null))
		{
			String redisCommand;
			String redisMarkerStart, redisMarkerFinish, redisParameters;
			String endId = Redis.STREAM_START_DEFAULT;
			String startId = Redis.STREAM_FINISH_DEFAULT;

			int curDocCount = 0;
			RedisDoc redisDoc = new RedisDoc(this);
			int maxDocCount = (int) redisDoc.getDocCount(aKeyName);
			int batchCount = Math.min(maxDocCount, Redis.STREAM_BATCH_COUNT);
			while (curDocCount <= maxDocCount)
			{
				dataDocList = redisDoc.loadDocs(aKeyName, Data.Order.ASCENDING, startId, endId, batchCount);
				loadCount = dataDocList.size();
				if (dataDocList.size() == 0)
					break;
				else
				{
					curDocCount += loadCount;
					batchCount = Math.min(maxDocCount-curDocCount, Redis.STREAM_BATCH_COUNT);
					for (DataDoc dataDoc : dataDocList)
					{
						redisCommand  = dataDoc.getValueByName("redis_command");
						redisMarkerStart  = dataDoc.getValueByName("redis_marker_start");
						redisMarkerFinish  = dataDoc.getValueByName("redis_marker_finish");
						if ((StringUtils.isEmpty(redisMarkerStart)) && (StringUtils.isEmpty(redisMarkerFinish)) &&
							(! StringUtils.equals(redisCommand, "PIPELINE")) && (! StringUtils.equals(redisCommand, "SYNC")))
						{
							redisParameters = dataDoc.getValueByName("redis_parameters");
							if (StringUtils.isNotEmpty(redisParameters))
								aPW.printf("%s %s%n", dataDoc.getValueByName("redis_command"), redisParameters);
							else
								aPW.printf("%s%n", dataDoc.getValueByName("redis_command"));
							priorStartId = dataDoc.getValueByName("id");
							startId = incrementStreamId(priorStartId);
						}
					}
				}
			}
		}
		else
		{
			String msgStr = "Unable to save stream as file: undefined parameters";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Save the Redis stream data set identified by the key name to the external
	 * path/file name in a format compatible with 'redis-cli'.
	 *
	 * @param aKeyName Redis stream key name
	 * @param aPathFileName Absolute path/file name to store the commands
	 *
	 * @throws RedisDSException Redis operation exception
	 * @throws IOException File I/O exception
	 */
	public void saveStreamAsFile(String aKeyName, String aPathFileName)
		throws RedisDSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveStreamAsFile");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(aKeyName)) && (StringUtils.isNotEmpty(aPathFileName)))
		{
			try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
			{
				saveStreamAsWriter(aKeyName, printWriter);
			}
		}
		else
		{
			String msgStr = "Unable to save stream as file: undefined parameters";
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Shutdown all inactive connections.  This method should be invoked when
	 * the application is done with communications to the Redis Server.
	 */
	public void shutdown()
	{
		Logger appLogger = mAppCtx.getLogger(this, "shutdown");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		finishMarker();
		if (mPrintWriter != null)
		{
			mPrintWriter.close();
			mPrintWriter = null;
		}
		if (mConnectionPool != null)
		{
			if (mCmdOperations != null)
			{
				returnConnection(mCmdOperations);
				mCmdOperations = null;
			}
			if (mCmdStream != null)
			{
				returnConnection(mCmdStream);
				mCmdStream = null;
			}
			mConnectionPool.close();
			mConnectionPool = null;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a RedisKey class instance to manage key names.
	 *
	 * @return RedisKey instance
	 */
	public RedisKey createKey()
	{
		return new RedisKey(this);
	}

	/**
	 * Creates a RedisKey class instance to manage key names.
	 *
	 * @param aKeyName Key name
	 *
	 * @return RedisKey instance
	 */
	public RedisKey createKey(String aKeyName)
	{
		return new RedisKey(this, aKeyName);
	}

	/**
	 * Creates a RedisCore class instance to manage core redis database operations.
	 *
	 * @return RedisCore instance
	 */
	public RedisCore createCore()
	{
		return new RedisCore(this);
	}

	/**
	 * Creates a RedisItem class instance to manage data items and their redis database operations.
	 *
	 * @return RedisItem instance
	 */
	public RedisItem createItem()
	{
		return new RedisItem(this);
	}

	/**
	 * Creates a RedisDoc class instance to manage data documents and their redis database operations.
	 *
	 * @return RedisDoc instance
	 */
	public RedisDoc createDoc()
	{
		return new RedisDoc(this);
	}

	/**
	 * Creates a RedisDoc class instance to manage data documents and their redis database operations.
	 *
	 * @param anIsFieldEhnanced If <i>true</i> then hash fields will have enhanced names
	 *
	 * @return RedisDoc instance
	 */
	public RedisDoc createDoc(boolean anIsFieldEhnanced)
	{
		return new RedisDoc(this, anIsFieldEhnanced);
	}

	/**
	 * Creates a RedisGrid class instance to manage data grids and their redis database operations.
	 *
	 * @return RedisGrid instance
	 */
	public RedisGrid createGrid()
	{
		return new RedisGrid(this);
	}

	/**
	 * Creates a RedisSearch class instance to manage data documents and their RediSearch database operations.
	 *
	 * @param aDocument Identifies the source of the documents that will be indexed
	 *
	 * @return RedisSearch instance
	 */
	public RedisSearch createSearch(Redis.Document aDocument)
	{
		return new RedisSearch(this, aDocument);
	}

	/**
	 * Creates a RedisSearch class instance to manage data documents and their RediSearch database operations.
	 *
	 * @param aDocument Identifies the source of the documents that will be indexed
	 * @param aDataSchemaDoc Data schema data document
	 * @param aSearchSchemaDoc Search schema data document
	 *
	 * @return RedisSearch instance
	 */
	public RedisSearch createSearch(Redis.Document aDocument,
									DataDoc aDataSchemaDoc, DataDoc aSearchSchemaDoc)
	{
		return new RedisSearch(this, aDocument, aDataSchemaDoc, aSearchSchemaDoc);
	}

	/**
	 * Creates a RedisGraphs class instance to manage data graphs and their RedisGraph database operations.
	 *
	 * @return RedisGraphs instance
	 */
	public RedisGraphs createGraph()
	{
		return new RedisGraphs(this);
	}

	/**
	 * Creates a RedisGraphs class instance to manage data graphs and their RedisGraph database operations.
	 *
	 * @param aVertexSchemaDoc Vertex schema definition
	 * @param anEdgeSchemaDoc Edge schema definition
	 *
	 * @return RedisGraphs instance
	 */
	public RedisGraphs createGraph(DataDoc aVertexSchemaDoc, DataDoc anEdgeSchemaDoc)
	{
		return new RedisGraphs(this, aVertexSchemaDoc, anEdgeSchemaDoc);
	}

	/**
	 * Creates a RedisJson class instance to manage data documents and their RedisJSON database operations.
	 *
	 * @return RedisJson instance
	 */
	public RedisJson createJson()
	{
		return new RedisJson(this);
	}

	/**
	 * Creates a RedisJson class instance to manage data documents and their RedisJSON database operations.
	 *
	 * @param aDataSchemaDoc Data schema data document
	 *
	 * @return RedisJson instance
	 */
	public RedisJson createJson(DataDoc aDataSchemaDoc)
	{
		return new RedisJson(this, aDataSchemaDoc);
	}

	/**
	 * Creates a RedisTimeseries class instance to manage data documents and their RedisTimeSeries database operations.
	 *
	 * @return RedisTimeseries instance
	 */
	public RedisTimeseries createTimeseries()
	{
		return new RedisTimeseries(this);
	}
}
