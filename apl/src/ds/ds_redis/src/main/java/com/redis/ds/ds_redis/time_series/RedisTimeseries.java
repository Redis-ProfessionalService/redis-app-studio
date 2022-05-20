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

package com.redis.ds.ds_redis.time_series;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterionEntry;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.timeseries.*;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.*;

/**
 * The RedisTimeseries class is responsible for accessing the RedisTimeSeries
 * module commands via the Jedis programmatic library.  It designed to
 * simplify the use of core Foundation class objects like items,
 * documents and grids.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * RedisTimeSeries is a Redis Module adding a Time Series data structure to Redis.
 *
 * @see <a href="https://oss.redislabs.com/redistimeseries/">RedisTimeSeries Documentation</a>
 * @see <a href="https://github.com/RedisTimeSeries/RedisTimeSeries">OSS RedisTimeSeries</a>
 * @see <a href="https://github.com/RedisTimeSeries/JRedisTimeSeries">Java RedisTimeSeries</a>
 * @see <a href="https://github.com/redis/jedis">Java Redis</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisTimeseries
{
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private final RedisKey mRedisKey;
	private final UnifiedJedis mCmdConnection;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the time series objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisTimeseries(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
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
	 * Create a key name (using the Redis Workbench standard format) based on the name
	 * of the time series.
	 *
	 * @param aName Name of the time series
	 *
	 * @return Key name
	 */
	public String timeSeriesKeyName(String aName)
	{
		return mRedisKey.moduleTimeSeries().redisTimeSeries().dataName(aName).name();
	}

	private void ensurePreconditions()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "ensurePreconditions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private Map<String, String> dataDocToLabels(DataDoc aDataDoc, StringBuilder aSB)
	{
		String itemName, itemValue;
		Map<String, String> rtsLabels;
		Logger appLogger = mAppCtx.getLogger(this, "dataDocToLabels");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataDoc.stream().findAny().isPresent())
		{
			aSB.append("LABELS ");
			rtsLabels = new HashMap<>();

			for (DataItem dataItem : aDataDoc.getItems())
			{
				if ((dataItem.isFeatureFalse(Data.FEATURE_IS_HIDDEN)) &&
						(dataItem.isFeatureFalse(Redis.FEATURE_IS_TIMESTAMP)) &&
						(dataItem.isFeatureFalse(Redis.FEATURE_IS_VALUE)))
				{
					itemName = dataItem.getName();
					itemValue = dataItem.getValue();
					if ((StringUtils.isNotEmpty(itemValue)))
					{
						rtsLabels.put(itemName, itemValue);
						aSB.append(String.format(" %s %s", mRedisDS.escapeKey(itemName), mRedisDS.escapeValue(itemValue)));
					}
				}
			}
		}
		else
			rtsLabels = null;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rtsLabels;
	}

	/**
	 * Create a time series based on the data document features and labels.
	 *
	 * RedisTS.FEATURE_KEY_NAME             String
	 * RedisTS.FEATURE_RETENTION_TIME       Long
	 * RedisTS.FEATURE_MEMORY_CHUNK_SIZE    Long
	 * RedisTS.FEATURE_IS_UNCOMPRESSED      Boolean
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> on successful creation and <i>false</i> otherwise
	 *
	 * @throws RedisDSException RedisTimeSeries data source exception
	 */
	public boolean create(DataDoc aDataDoc)
		throws RedisDSException
	{
		long memoryChunkSize;
		String compressionString;
		TSCreateParams tsCreateParams;
		Logger appLogger = mAppCtx.getLogger(this, "create");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (aDataDoc == null)
			throw new RedisDSException("Data document is null.");

		if (! aDataDoc.isFeatureAssigned(Redis.FEATURE_KEY_NAME))
			throw new RedisDSException("Data document is missing feature key name.");
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		long retentionTime = Math.max(0, aDataDoc.getFeatureAsLong(Redis.FEATURE_RETENTION_TIME));
		if (aDataDoc.isFeatureAssigned(Redis.FEATURE_MEMORY_CHUNK_SIZE))
			memoryChunkSize = aDataDoc.getFeatureAsLong(Redis.FEATURE_MEMORY_CHUNK_SIZE);
		else
			memoryChunkSize = 4000;
		boolean isUncompressed = aDataDoc.isFeatureTrue(Redis.FEATURE_IS_UNCOMPRESSED);
		if (isUncompressed)
			compressionString = " UNCOMPRESSED";
		else
			compressionString = StringUtils.EMPTY;
		StringBuilder stringBuilder = new StringBuilder(String.format("TS.CREATE %s RETENTION %d%s %d ", mRedisDS.escapeKey(keyName), retentionTime,
																	  compressionString, memoryChunkSize));
		Map<String, String> rtsLabels = dataDocToLabels(aDataDoc, stringBuilder);
		if (isUncompressed)
			tsCreateParams = TSCreateParams.createParams().retention(retentionTime).uncompressed().labels(rtsLabels);
		else
			tsCreateParams = TSCreateParams.createParams().retention(retentionTime).compressed().labels(rtsLabels);
		String msgString = mCmdConnection.tsCreate(keyName, tsCreateParams);
		mRedisDS.saveCommand(appLogger, stringBuilder.toString());
		boolean isOK = StringUtils.equals(msgString, Redis.RESPONSE_OK);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Update an existing time series based on the data document features and labels.
	 *
	 * RedisTS.FEATURE_KEY_NAME             String
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> on successful update and <i>false</i> otherwise
	 *
	 * @throws RedisDSException RedisTimeSeries data source exception
	 */
	public boolean update(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (aDataDoc == null)
			throw new RedisDSException("Data document is null.");

		if (! aDataDoc.isFeatureAssigned(Redis.FEATURE_KEY_NAME))
			throw new RedisDSException("Data document is missing feature key name.");
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		StringBuilder stringBuilder = new StringBuilder(String.format("TS.ALTER %s ", mRedisDS.escapeKey(keyName)));
		Map<String, String> rtsLabels = dataDocToLabels(aDataDoc, stringBuilder);
		String msgString = mCmdConnection.tsAlter(keyName, TSAlterParams.alterParams().labels(rtsLabels));
		mRedisDS.saveCommand(appLogger, stringBuilder.toString());
		boolean isOK = StringUtils.equals(msgString, Redis.RESPONSE_OK);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
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
		long delCount;
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (StringUtils.isNotEmpty(aKeyName))
		{
			String cmdString = String.format("DEL %s", mRedisDS.escapeKey(aKeyName));
			delCount = mRedisDS.createCore().delete(aKeyName);
			mRedisDS.saveCommand(appLogger, cmdString);
		}
		else
			throw new RedisDSException("Key name is null.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return delCount;
	}

	/**
	 * Creates a compaction rule for the time series.  Only new samples that are added into the
	 * source series after creation of the rule will be aggregated.
	 *
	 * <b>Note:</b> Both the source and destination keys must be created prior to calling this method.
	 *
	 * @param aSrcKeyName Source time series key name
	 * @param aDstKeyName Destination time series key name
	 * @param aFunction Aggregation function
	 * @param aBucketSize Time bucket size (in milliseconds)
	 *
	 * @return <i>true</i> on successful rule creation and <i>false</i> otherwise
	 *
	 * @throws RedisDSException RedisTimeSeries operation failure
	 */
	public boolean createRule(String aSrcKeyName, String aDstKeyName, Redis.Function aFunction, long aBucketSize)
		throws RedisDSException
	{
		boolean isOK;
		Logger appLogger = mAppCtx.getLogger(this, "createRule");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if ((StringUtils.isNotEmpty(aSrcKeyName)) && (StringUtils.isNotEmpty(aDstKeyName)))
		{
			AggregationType tsAggregation = AggregationType.valueOf(aFunction.name());
			String cmdString = String.format("TS.CREATERULE %s %s %s %d", mRedisDS.escapeKey(aSrcKeyName), mRedisDS.escapeKey(aDstKeyName),
											 tsAggregation.name(), aBucketSize);
			String msgString = mCmdConnection.tsCreateRule(aSrcKeyName, aDstKeyName, tsAggregation, aBucketSize);
			mRedisDS.saveCommand(appLogger, cmdString);
			isOK = StringUtils.equals(msgString, Redis.RESPONSE_OK);
		}
		else
			throw new RedisDSException("Key name is null.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Deletes a compaction rule for the time series.
	 *
	 * <b>Note:</b> Both the source and destination keys must be created prior to calling this method.
	 *
	 * @param aSrcKeyName Source time series key name
	 * @param aDstKeyName Destination time series key name
	 *
	 * @return <i>true</i> on successful rule deletion and <i>false</i> otherwise
	 *
	 * @throws RedisDSException RedisTimeSeries operation failure
	 */
	public boolean deleteRule(String aSrcKeyName, String aDstKeyName)
		throws RedisDSException
	{
		boolean isOK;
		Logger appLogger = mAppCtx.getLogger(this, "deleteRule");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if ((StringUtils.isNotEmpty(aSrcKeyName)) && (StringUtils.isNotEmpty(aDstKeyName)))
		{
			String cmdString = String.format("TS.DELETERULE %s %s", mRedisDS.escapeKey(aSrcKeyName), mRedisDS.escapeKey(aDstKeyName));
			String msgString = mCmdConnection.tsDeleteRule(aSrcKeyName, aDstKeyName);
			mRedisDS.saveCommand(appLogger, cmdString);
			isOK = StringUtils.equals(msgString, Redis.RESPONSE_OK);
		}
		else
			throw new RedisDSException("Key name is null.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Add a sample value to the time series specified by the key name.
	 * This method assumes that the create() method was used to create
	 * the time series key.
	 *
	 * @param aKeyName Time series key name
	 * @param aValue Sample value
	 *
	 * @return Count of added samples
	 *
	 * @throws RedisDSException RedisTimeSeries operation failure
	 */
	public long add(String aKeyName, double aValue)
		throws RedisDSException
	{
		long addCount;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (StringUtils.isNotEmpty(aKeyName))
		{
			String cmdString = String.format("TS.ADD %s * %s", mRedisDS.escapeKey(aKeyName), aValue);
			addCount = mCmdConnection.tsAdd(aKeyName, aValue);
			mRedisDS.saveCommand(appLogger, cmdString);
		}
		else
			throw new RedisDSException("Key name is null.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return addCount;
	}

	/**
	 * Add a sample value to the time series specified by the key name.
	 * This method assumes that the create() method was used to create
	 * the time series key.
	 *
	 * @param aKeyName Time series key name
	 * @param aValue Sample value
	 * @param aTS Timestamp value
	 *
	 * @return Count of added samples
	 *
	 * @throws RedisDSException RedisTimeSeries operation failure
	 */
	public long add(String aKeyName, long aTS, double aValue)
		throws RedisDSException
	{
		long addCount;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (StringUtils.isNotEmpty(aKeyName))
		{
			String cmdString = String.format("TS.ADD %s %d %s", mRedisDS.escapeKey(aKeyName), aTS, aValue);
			addCount = mCmdConnection.tsAdd(aKeyName, aTS, aValue);
			mRedisDS.saveCommand(appLogger, cmdString);
		}
		else
			throw new RedisDSException("Key name is null.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return addCount;
	}

	/**
	 * Adds a time series sample based on the data document features.
	 *
	 * RedisTS.FEATURE_KEY_NAME             String
	 * RedisTS.FEATURE_IS_TIMESTAMP         Boolean
	 * RedisTS.FEATURE_IS_VALUE             Long
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return Count of samples added
	 *
	 * @throws RedisDSException RedisTimeSeries data source exception
	 */
	public long add(DataDoc aDataDoc)
		throws RedisDSException
	{
		long tsValue;
		DataItem diSampleValue, diSampleTS;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataDoc == null)
			throw new RedisDSException("Data document is null.");
		else if (! aDataDoc.isFeatureAssigned(Redis.FEATURE_KEY_NAME))
			throw new RedisDSException("Data document is missing feature key name.");

		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		Optional<DataItem> optSampleValue = aDataDoc.getItemByFeatureEnabledOptional(Redis.FEATURE_IS_VALUE);
		if (optSampleValue.isPresent())
		{
			diSampleValue = optSampleValue.get();
			if (! Data.isNumber(diSampleValue.getType()))
				throw new RedisDSException(String.format("Item '%s' is not a number.", diSampleValue.getName()));
		}
		else
			throw new RedisDSException(String.format("Unable to locate '%s' feature in data document.", Redis.FEATURE_IS_VALUE));

		Optional<DataItem> optSampleTS = aDataDoc.getItemByFeatureEnabledOptional(Redis.FEATURE_IS_TIMESTAMP);
		if (optSampleTS.isPresent())
		{
			diSampleTS = optSampleTS.get();
			if (Data.isNumber(diSampleTS.getType()))
				tsValue = diSampleTS.getValueAsLong();
			else if (Data.isDateOrTime(diSampleTS.getType()))
				tsValue = diSampleTS.getValueAsDate().getTime();
			else
				throw new RedisDSException(String.format("Item '%s' is not a timestamp value.", diSampleTS.getName()));
		}
		else
			tsValue = System.currentTimeMillis();

		long tsResponse = mCmdConnection.tsAdd(keyName, tsValue, diSampleValue.getValueAsDouble());
		mRedisDS.saveCommand(appLogger, String.format("TS.ADD %s %d %s", mRedisDS.escapeKey(keyName), tsValue, diSampleValue.getValue()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return tsResponse;
	}

	/**
	 * Adds a time series sample based on the data grid features and labels.
	 *
	 * RedisTS.FEATURE_KEY_NAME             String
	 * RedisTS.FEATURE_IS_TIMESTAMP         Boolean
	 * RedisTS.FEATURE_IS_VALUE             Long
	 * RedisTS.FEATURE_RETENTION_TIME       Long
	 * RedisTS.FEATURE_MEMORY_CHUNK_SIZE    Long
	 * RedisTS.FEATURE_IS_UNCOMPRESSED      Boolean
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return <i>true</i> on successful adding of samples and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public boolean add(DataGrid aDataGrid)
		throws RedisDSException
	{
		boolean isOK;
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataGrid == null)
			throw new RedisDSException("Data grid is null.");
		else if (! aDataGrid.isFeatureAssigned(Redis.FEATURE_KEY_NAME))
			throw new RedisDSException("Data grid is missing feature key name.");

		DataDoc ddColumns = aDataGrid.getColumns();
		String keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if (! mRedisDS.createCore().exists(keyName))
		{
			DataDoc childDoc = ddColumns.getFirstChildDoc(Redis.RT_CHILD_LABELS);
			if (childDoc == null)
			{
				ddColumns.addFeature(Redis.FEATURE_KEY_NAME, keyName);
				isOK = create(ddColumns);
			}
			else
			{
				childDoc.addFeature(Redis.FEATURE_KEY_NAME, keyName);
				isOK = create(childDoc);
			}
			if (! isOK)
				throw new RedisDSException("Timeseries key creation failed.");
		}

		long addCount = 0;
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			dataDoc = aDataGrid.getRowAsDoc(row);
			dataDoc.addFeature(Redis.FEATURE_KEY_NAME, keyName);
			add(dataDoc);
			addCount++;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return addCount == rowCount;
	}

	private String[] criteriaToFilters(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		String itemName;
		DataItem ceDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "criteriaToFilters");

		if (aDSCriteria == null)
			throw new RedisDSException("Cannot execute - criteria was not prepared.");

		DataItem diFilters = new DataItem.Builder().name("ts_filters").title("TimeSeries Filters").build();
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			if (StringUtils.startsWith(itemName, Redis.RT_PREFIX))
				continue;

			switch (ce.getLogicalOperator())
			{
				case EQUAL:
					diFilters.addValue(String.format("%s=%s", ceDataItem.getName(), ceDataItem.getValue()));
					break;
				case NOT_EQUAL:
					diFilters.addValue(String.format("%s!=%s", ceDataItem.getName(), ceDataItem.getValue()));
					break;
				case IN:
					if (ceDataItem.valueCount() > 0)
					{
						boolean isFirst = true;
						StringBuilder stringBuilder = new StringBuilder();
						for (String ceValue : ceDataItem.getValues())
						{
							if (isFirst)
							{
								stringBuilder.append(String.format("%s=(%s", ceDataItem.getName(), ceValue));
								isFirst = false;
							}
							else
								stringBuilder.append(String.format(",%s", ceValue));
						}
						stringBuilder.append(")");
						diFilters.addValue(stringBuilder.toString());
					}
					break;
				case NOT_IN:
					if (ceDataItem.valueCount() > 0)
					{
						boolean isFirst = true;
						StringBuilder stringBuilder = new StringBuilder();
						for (String ceValue : ceDataItem.getValues())
						{
							if (isFirst)
							{
								stringBuilder.append(String.format("%s!=(%s", ceDataItem.getName(), ceValue));
								isFirst = false;
							}
							else
								stringBuilder.append(String.format(",%s", ceValue));
						}
						stringBuilder.append(")");
						diFilters.addValue(stringBuilder.toString());
					}
					break;
				case EMPTY:
					diFilters.addValue(String.format("%s!=", ceDataItem.getName()));
					break;
				case NOT_EMPTY:
					diFilters.addValue(String.format("%s=", ceDataItem.getName()));
					break;
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return diFilters.getValuesArray();
	}

	/**
	 * Queries a Redis time series using a timestamp range and label criteria and returns
	 * a data grid containing rows of matching samples.
	 *
	 * RedisTS.FEATURE_KEY_NAME             String
	 * RedisTS.FEATURE_START_TIMESTAMP      Long
	 * RedisTS.FEATURE_FINISH_TIMESTAMP     Long
	 * RedisTS.FEATURE_SAMPLE_COUNT         Integer
	 * FEATURE_SORT_ORDER                   ASCENDING / DESCENDING
	 * FEATURE_FUNCTION_NAME                Function
	 * FEATURE_TIME_BUCKET                  Long
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Data grid instance
	 *
	 * @throws RedisDSException RedisTimeSeries data source exception
	 */
	public DataGrid queryRange(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		int sampleCount;
		DataGrid dataGrid;
		Data.Order sortOrder;
		long startTS, finishTS;
		List<TSElement> tsValues;
		List<TSKeyedElements> tsRanges;
		String functionName = StringUtils.EMPTY;
		long timeBucket = DateUtils.MILLIS_PER_HOUR;
		Logger appLogger = mAppCtx.getLogger(this, "queryRange");

		ensurePreconditions();
		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDSCriteria == null)
			throw new RedisDSException("Cannot execute - criteria was not prepared.");

		String keyName = aDSCriteria.getFeature(Redis.FEATURE_KEY_NAME);
		if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_START_TIMESTAMP))
			startTS = aDSCriteria.getFeatureAsLong(Redis.FEATURE_START_TIMESTAMP);
		else
			startTS = 0;
		if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_FINISH_TIMESTAMP))
			finishTS = aDSCriteria.getFeatureAsLong(Redis.FEATURE_FINISH_TIMESTAMP);
		else
			finishTS = Long.MAX_VALUE;
		if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_SORT_ORDER))
			sortOrder = Data.Order.valueOf(aDSCriteria.getFeature(Redis.FEATURE_SORT_ORDER));
		else
			sortOrder = Data.Order.ASCENDING;
		if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_SAMPLE_COUNT))
			sampleCount = Math.max(1, aDSCriteria.getFeatureAsInt(Redis.FEATURE_SAMPLE_COUNT));
		else
			sampleCount = 0;

		StringBuilder stringBuilder = new StringBuilder();
		if (StringUtils.isNotEmpty(keyName))
		{
			if (sortOrder == Data.Order.ASCENDING)
			{
				stringBuilder.append(String.format("TS.RANGE %s %d %d", mRedisDS.escapeKey(keyName), startTS, finishTS));
				if (sampleCount > 0)
					stringBuilder.append(String.format(" COUNT %d", sampleCount));
			}
			else
			{
				stringBuilder.append(String.format("TS.REVRANGE %s %d %d", mRedisDS.escapeKey(keyName), startTS, finishTS));
				if (sampleCount > 0)
					stringBuilder.append(String.format(" COUNT %d", sampleCount));
			}
			if (sortOrder == Data.Order.ASCENDING)
			{
				if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_FUNCTION_NAME))
				{
					functionName = aDSCriteria.getFeature(Redis.FEATURE_FUNCTION_NAME);
					AggregationType tsAggregation = AggregationType.valueOf(functionName);
					if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_TIME_BUCKET))
						timeBucket = aDSCriteria.getFeatureAsLong(Redis.FEATURE_TIME_BUCKET);
					stringBuilder.append(String.format(" AGGREGATION %s %d", functionName, timeBucket));
					if (sampleCount == 0)
						tsValues = mCmdConnection.tsRange(keyName, TSRangeParams.rangeParams(startTS, finishTS).aggregation(tsAggregation, timeBucket));
					else
						tsValues = mCmdConnection.tsRange(keyName, TSRangeParams.rangeParams(startTS, finishTS).aggregation(tsAggregation, sampleCount));
				}
				else
				{
					if (sampleCount == 0)
						tsValues = mCmdConnection.tsRange(keyName, startTS, finishTS);
					else
						tsValues = mCmdConnection.tsRange(keyName, TSRangeParams.rangeParams(startTS, finishTS).aggregation(AggregationType.COUNT, sampleCount));
				}
			}
			else
			{
				if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_FUNCTION_NAME))
				{
					functionName = aDSCriteria.getFeature(Redis.FEATURE_FUNCTION_NAME);
					AggregationType tsAggregation = AggregationType.valueOf(functionName);
					if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_TIME_BUCKET))
						timeBucket = aDSCriteria.getFeatureAsLong(Redis.FEATURE_TIME_BUCKET);
					stringBuilder.append(String.format(" AGGREGATION %s %d", functionName, timeBucket));
					if (sampleCount == 0)
						tsValues = mCmdConnection.tsRevRange(keyName, TSRangeParams.rangeParams(startTS, finishTS).aggregation(tsAggregation, timeBucket));
					else
						tsValues = mCmdConnection.tsRevRange(keyName, TSRangeParams.rangeParams(startTS, finishTS).aggregation(tsAggregation, sampleCount));
				}
				else
				{
					if (sampleCount == 0)
						tsValues = mCmdConnection.tsRevRange(keyName, startTS, finishTS);
					else
						tsValues = mCmdConnection.tsRevRange(keyName, TSRangeParams.rangeParams(startTS, finishTS).aggregation(AggregationType.COUNT, sampleCount));
				}
			}

// Create a data grid and copy the time series response	into it.

			dataGrid = new DataGrid("TimeSeries Time & Values");
			dataGrid.addCol(new DataItem.Builder().type(Data.Type.Long).name("time").title("Time in Milliseconds").build());
			dataGrid.addCol(new DataItem.Builder().type(Data.Type.DateTime).name("date_time").title("Date & Timestamp").build());
			dataGrid.addCol(new DataItem.Builder().type(Data.Type.Double).name("value").title("Sample Value").build());
			for (TSElement tsValue : tsValues)
			{
				dataGrid.newRow();
				dataGrid.setValueByName("time", tsValue.getTimestamp());
				dataGrid.setValueByName("date_time", new Date(tsValue.getTimestamp()));
				dataGrid.setValueByName("value", tsValue.getValue());
				dataGrid.addRow();
				if ((sampleCount > 0) && (dataGrid.rowCount() >= sampleCount))
					break;	// TS.RANGE "ASRT:RT:TS:DD:MN:nasdaq:ebay:price_open" 0 1649072437860 AGGREGATION COUNT 25 (bug - not enforced properly)
			}
		}
		else	// no key name specified, so this applies to all ts keys
		{
			if (sortOrder == Data.Order.ASCENDING)
			{
				stringBuilder.append(String.format("TS.MRANGE %d %d", startTS, finishTS));
				if (sampleCount > 0)
					stringBuilder.append(String.format("COUNT %d", sampleCount));
			}
			else
			{
				stringBuilder.append(String.format("TS.MREVRANGE %d %d", startTS, finishTS));
				if (sampleCount > 0)
					stringBuilder.append(String.format("COUNT %d", sampleCount));
			}
			if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_FUNCTION_NAME))
			{
				functionName = aDSCriteria.getFeature(Redis.FEATURE_FUNCTION_NAME);
				if (aDSCriteria.isFeatureAssigned(Redis.FEATURE_TIME_BUCKET))
					timeBucket = aDSCriteria.getFeatureAsLong(Redis.FEATURE_TIME_BUCKET);
				stringBuilder.append(String.format(" AGGREGATION %s %d", functionName, timeBucket));
			}
			String[] tsFilters = criteriaToFilters(aDSCriteria);
			if (tsFilters.length > 0)
			{
				stringBuilder.append(" WITHLABELS FILTER");
				tsFilters = criteriaToFilters(aDSCriteria);
				for (String filter : tsFilters)
					stringBuilder.append(String.format(" \"%s\"", filter));
			}
			if (sortOrder == Data.Order.ASCENDING)
			{
				if (StringUtils.isNotEmpty(functionName))
				{
					AggregationType tsAggregation = AggregationType.valueOf(functionName);
					if (tsFilters.length == 0)
						tsRanges = mCmdConnection.tsMRange(TSMRangeParams.multiRangeParams(startTS, finishTS).aggregation(tsAggregation, timeBucket));
					else
						tsRanges = mCmdConnection.tsMRange(TSMRangeParams.multiRangeParams(startTS, finishTS).aggregation(tsAggregation, timeBucket).filter(tsFilters));
				}
				else
				{
					if (tsFilters.length == 0)
						tsRanges = mCmdConnection.tsMRange(startTS, finishTS);
					else
						tsRanges = mCmdConnection.tsMRange(TSMRangeParams.multiRangeParams(startTS, finishTS).filter(tsFilters));
				}
			}
			else
			{
				if (StringUtils.isNotEmpty(functionName))
				{
					AggregationType tsAggregation = AggregationType.valueOf(functionName);
					if (tsFilters.length == 0)
						tsRanges = mCmdConnection.tsMRevRange(TSMRangeParams.multiRangeParams(startTS, finishTS).aggregation(tsAggregation, timeBucket));
					else
						tsRanges = mCmdConnection.tsMRevRange(TSMRangeParams.multiRangeParams(startTS, finishTS).aggregation(tsAggregation, timeBucket).filter(tsFilters));
				}
				else
				{
					if (tsFilters.length == 0)
						tsRanges = mCmdConnection.tsMRevRange(startTS, finishTS);
					else
						tsRanges = mCmdConnection.tsMRevRange(TSMRangeParams.multiRangeParams(startTS, finishTS).filter(tsFilters));
				}
			}

// Create a data grid and copy the time series response	into it.

			ArrayList<String> timeList = new ArrayList<>();
			ArrayList<String> valueList = new ArrayList<>();
			ArrayList<String> labelList = new ArrayList<>();
			ArrayList<String> dateTimeList = new ArrayList<>();
			dataGrid = new DataGrid("TimeSeries Ranges");
			dataGrid.addCol(new DataItem.Builder().name("key").title("Key Name").build());
			dataGrid.addCol(new DataItem.Builder().name("labels").title("Labels").build());
			dataGrid.addCol(new DataItem.Builder().type(Data.Type.Long).name("time").title("Time in Milliseconds").build());
			dataGrid.addCol(new DataItem.Builder().type(Data.Type.DateTime).name("date_time").title("Date & Timestamp").build());
			dataGrid.addCol(new DataItem.Builder().type(Data.Type.Double).name("value").title("Sample Value").build());
			for (TSKeyedElements tsRange : tsRanges)
			{
				dataGrid.newRow();
				dataGrid.setValueByName("key", tsRange.getKey());
				labelList.clear();
				tsRange.getLabels().forEach((k, v) -> labelList.add(String.format("%s=%s", k, v)));
				dataGrid.setValuesByName("labels", labelList);
				dateTimeList.clear();
				valueList.clear();
				for (TSElement tsValue : tsRange.getValue())
				{
					timeList.add(Long.toString(tsValue.getTimestamp()));
					dateTimeList.add(Data.dateValueFormatted(new Date(tsValue.getTimestamp()), Data.FORMAT_DATETIME_DEFAULT));
					valueList.add(Double.toString(tsValue.getValue()));
				}
				dataGrid.setValuesByName("time", timeList);
				dataGrid.setValuesByName("date_time", dateTimeList);
				dataGrid.setValuesByName("value", valueList);
				dataGrid.addRow();
			}
		}
		mRedisDS.saveCommand(appLogger, stringBuilder.toString());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Queries a Redis time series using a label criteria and returns
	 * a data grid containing rows of matching keys.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Data grid instance
	 *
	 * @throws RedisDSException RedisTimeSeries data source exception
	 */
	public DataGrid queryKeys(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "queryKeys");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (aDSCriteria == null)
			throw new RedisDSException("Cannot execute - criteria was not prepared.");

		DataGrid dataGrid = new DataGrid("TimeSeries Keys");
		dataGrid.addCol(new DataItem.Builder().name("key").title("Key Name").build());
		StringBuilder stringBuilder = new StringBuilder("TS.QUERYINDEX");
		String[] tsFilters = criteriaToFilters(aDSCriteria);
		if (tsFilters.length > 0)
		{
			tsFilters = criteriaToFilters(aDSCriteria);
			for (String filter : tsFilters)
				stringBuilder.append(String.format(" \"%s\"", filter));
			List<String> matchingKeys = mCmdConnection.tsQueryIndex(tsFilters);
			mRedisDS.saveCommand(appLogger, stringBuilder.toString());

			for (String keyName : matchingKeys)
			{
				dataGrid.newRow();
				dataGrid.setValueByName("key", keyName);
				dataGrid.addRow();
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}
}
