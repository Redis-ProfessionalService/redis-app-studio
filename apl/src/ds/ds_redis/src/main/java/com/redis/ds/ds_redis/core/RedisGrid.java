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
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.xml.sax.SAXException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * The RedisGrid class handles Redis database operations centered around
 * {@link com.redis.foundation.data.DataGrid} objects.  The Redis operations
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
public class RedisGrid
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
	public RedisGrid(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mCmdConnection = aRedisDS.getCmdConnection();
	}

	private String columnSchemaToString(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "columnSchemaToString");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = aDataGrid.getColumns();
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
			String gridKeyName = mRedisKey.moduleCore().redisSortedSet().dataObject(aDataGrid).name();
			String keyValue = columnSchemaToString(aDataGrid);
			mCmdConnection.zadd(gridKeyName, 0, keyValue);
			mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), 0, mRedisDS.escapeValue(keyValue)));
			mRedisDS.createCore().expire(gridKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	private void addRow(Pipeline aPipeline, DataGrid aDataGrid, DataDoc aDataDoc, long aRowNumber)
		throws RedisDSException
	{
		String rowKeyName;
		Logger appLogger = mAppCtx.getLogger(this, "addRow");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String gridKeyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		RedisDoc redisDoc = mRedisDS.createDoc();
		redisDoc.add(aPipeline, aDataDoc);
		rowKeyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(rowKeyName))
		{
			aPipeline.zadd(gridKeyName, aRowNumber, rowKeyName);
			mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), aRowNumber, mRedisDS.escapeValue(rowKeyName)));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/* A row insert is always after the row number. List and Sorted Sets require strings to
	   be unique due to commands that require matching. */
	private void writeRow(DataGrid aDataGrid, DataDoc aDataDoc, long aRowNumber)
		throws RedisDSException
	{
		String rowKeyName;
		Logger appLogger = mAppCtx.getLogger(this, "writeRow");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String gridKeyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		RedisDoc redisDoc = mRedisDS.createDoc();
		redisDoc.add(aDataDoc);
		rowKeyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(rowKeyName))
		{
			if (aRowNumber == Redis.GRID_RANGE_FINISH)
			{
				long sortedSetLength = mCmdConnection.zcard(gridKeyName);
				mCmdConnection.zadd(gridKeyName, sortedSetLength, rowKeyName);
				mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), sortedSetLength, mRedisDS.escapeValue(rowKeyName)));
			}
			else
			{
				long rowNumber = Math.max(1, aRowNumber);
				List<String> listValues = mCmdConnection.zrange(gridKeyName, rowNumber, Redis.GRID_RANGE_FINISH);
				mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(gridKeyName), Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH));
				if (listValues.size() > 0)
				{
					Transaction cmdTransaction = mCmdConnection.multi();
					mRedisDS.saveCommand(appLogger, "PIPELINE");
					for (String listValue : listValues)
					{
						cmdTransaction.zincrby(gridKeyName, 1, listValue);
						mRedisDS.saveCommand(appLogger, String.format("ZINCRBY %s 1 %s", mRedisDS.escapeKey(gridKeyName), mRedisDS.escapeValue(listValue)));
					}
					cmdTransaction.exec();
					mRedisDS.saveCommand(appLogger, "SYNC");
				}
				mCmdConnection.zadd(gridKeyName, rowNumber, rowKeyName);
				mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), rowNumber, mRedisDS.escapeValue(rowKeyName)));
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void writeRows(DataGrid aDataGrid, boolean aPipelineEnabled)
		throws RedisDSException
	{
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "writeRows");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		int rowCount = aDataGrid.rowCount();
		String gridKeyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if ((StringUtils.isNotEmpty(gridKeyName)) && (rowCount > 0))
		{
			if (aPipelineEnabled)
			{
				int multiCount = 0;
				Pipeline commandPipeline = null;
				for (int row = 0; row < rowCount; row++)
				{
					if (multiCount == 0)
					{
						commandPipeline = mCmdConnection.pipelined();
						mRedisDS.saveCommand(appLogger, "PIPELINE");
					}
					dataDoc = aDataGrid.getRowAsDoc(row);
					dataDoc.setName(String.format("%s [Row %d]", aDataGrid.getName(), row+1));
					addRow(commandPipeline, aDataGrid, dataDoc, row+1);
					multiCount += 2;
					if (multiCount >= Redis.PIPELINE_BATCH_COUNT)
					{
						if (commandPipeline != null)
						{
							commandPipeline.sync();
							commandPipeline = null;
							mRedisDS.saveCommand(appLogger, "SYNC");
							multiCount = 0;
						}
					}
				}
				if (multiCount > 0)
				{
					if (commandPipeline != null)
					{
						commandPipeline.sync();
						mRedisDS.saveCommand(appLogger, "SYNC");
					}
				}
			}
			else
			{
				for (int row = 0; row < rowCount; row++)
					writeRow(aDataGrid, aDataGrid.getRowAsDoc(row), Redis.GRID_RANGE_FINISH);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Determines if a data grid key already exists in the database.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return <i>true</i> if it exists and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public boolean exists(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "exists");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String gridKeyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isEmpty(gridKeyName))
		{
			gridKeyName = mRedisKey.moduleCore().redisSortedSet().dataObject(aDataGrid).name();
			aDataGrid.addFeature(Redis.FEATURE_KEY_NAME, gridKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return mRedisDS.createCore().exists(gridKeyName);
	}

	/**
	 * Adds a data grid to the Redis database using hashes for each row and
	 * a sorted set to capture the row order.
	 *
	 * The logic automatically handles data items with multiple values (when
	 * hashes are used) and performs encryption on the values if the
	 * Data.FEATURE_IS_SECRET is enabled.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		DataDoc dataDoc = aDataGrid.getColumns();
		int colCount = dataDoc.count();
		int rowCount = aDataGrid.rowCount();
		if ((colCount > 0) && (rowCount > 0))
		{
			String gridKeyName = mRedisKey.moduleCore().redisSortedSet().dataObject(aDataGrid).name();
			aDataGrid.addFeature(Redis.FEATURE_KEY_NAME, gridKeyName);
			String keyValue = columnSchemaToString(aDataGrid);
			mCmdConnection.zadd(gridKeyName, 0, keyValue);
			mRedisDS.saveCommand(appLogger, String.format("ZADD %s %d %s", mRedisDS.escapeKey(gridKeyName), 0, mRedisDS.escapeValue(keyValue)));
			writeRows(aDataGrid, true);
			mRedisDS.createCore().expire(gridKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the data document as a row to the end of the data grid
	 * identified by the key name in the Redis database.
	 *
	 * @param aKeyName Key name of data grid
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(String aKeyName, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if (StringUtils.isNotEmpty(aKeyName))
		{
			Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
			if (optDataGrid.isPresent())
			{
				DataGrid dataGrid = optDataGrid.get();
				long rowCount = mRedisDS.createCore().sortedSetCount(aKeyName);
				aDataDoc.setName(String.format("%s - Row %d", dataGrid.getName(), rowCount+1));
				writeRow(dataGrid, aDataDoc, Redis.GRID_RANGE_FINISH);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the data document as a row to the end of the data grid
	 * in the Redis database.  The key name is obtained from the
	 * feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * <b>Note:</b> This method does not add the data document
	 * instance to the end of the data grid instance.  You must
	 * reload the data grid from the Redis database.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataGrid aDataGrid, DataDoc aDataDoc)
		throws RedisDSException
	{
		add(aDataGrid.getFeature(Redis.FEATURE_KEY_NAME), aDataDoc);
	}

	private Optional<DataDoc> listValueToRow(String aListValue)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "listValueToRow");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		RedisDoc redisDoc = mRedisDS.createDoc();
		Optional<DataDoc> optDataDoc = redisDoc.getDoc(aListValue);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
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
	 * Returns an optional data grid with row data populated from the
	 * Redis database based on the key name.
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
				if (schemaDataDoc.count() > 0)
				{
					dataGrid.setColumns(schemaDataDoc);
					for (int row = 1; row < valueCount; row++)
					{
						optDataDoc = listValueToRow(listValues.get(row));
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
				optDataDoc = listValueToRow(listValues.get(row));
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
	@SuppressWarnings("unchecked")
	public void loadGridPipeline(DataGrid aDataGrid, long aRowNumberStart, long aRowNumberFinish)
		throws RedisDSException
	{
		String keyName;
		DataDoc dataDoc;
		List<String> listValues;
		Optional<DataDoc> optDataDoc;
		Map<String,String> fieldValues;
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
			RedisDoc redisDoc = mRedisDS.createDoc();
			for (int row = 0; row < valueCount; row++)
			{
				if (multiCount == 0)
				{
					commandPipeline = new Pipeline(mRedisDS.getCmdConnection());
					mRedisDS.saveCommand(appLogger, "PIPELINE");
				}
				keyName = listValues.get(row);
				commandPipeline.hgetAll(keyName);
				mRedisDS.saveCommand(appLogger, String.format("HGETALL %s", mRedisDS.escapeKey(keyName)));
				multiCount++;
				if (multiCount >= Redis.PIPELINE_BATCH_COUNT)
				{
					pipelineResponseList = commandPipeline.syncAndReturnAll();
					mRedisDS.saveCommand(appLogger, "SYNC");
					for (Object responseObject : pipelineResponseList)
					{
						fieldValues = (Map<String,String>) responseObject;
						if ((fieldValues != null) && (! fieldValues.isEmpty()))
						{
							optDataDoc = redisDoc.mapToDataDoc(fieldValues);
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
					fieldValues = (Map<String,String>) responseObject;
					if ((fieldValues != null) && (! fieldValues.isEmpty()))
					{
						optDataDoc = redisDoc.mapToDataDoc(fieldValues);
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
	 * Loads the data grid rows and columns from the Redis database.  The
	 * key name is obtained from the feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 *  <b>Note:</b> Any existing rows in the grid will be emptied prior
	 * 	to the load operation. Since streams storage could consist of
	 * 	events with different member fields, it is not supported by this
	 * 	method. Also, geo location is best loaded with specialize distance
	 * 	queries and therefore unsupported with this method.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void loadGrid(DataGrid aDataGrid)
		throws RedisDSException
	{
		DataDoc dataDoc;
		List<String> listValues;
		Optional<DataDoc> optDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			aDataGrid.emptyRows();
			listValues = mCmdConnection.zrange(keyName, Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH);
			mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(keyName), Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH));
			int valueCount = listValues.size();
			if (valueCount > 0)
			{
				DataDoc schemaDataDoc = stringToColumnSchema(listValues.get(0));
				if (schemaDataDoc.count() > 0)
				{
					aDataGrid.setColumns(schemaDataDoc);
					for (int row = 1; row < valueCount; row++)
					{
						optDataDoc = listValueToRow(listValues.get(row));
						if (optDataDoc.isPresent())
						{
							dataDoc = optDataDoc.get();
							aDataGrid.addRow(dataDoc);
						}
					}
				}
			}
			aDataGrid.addFeature(DS.FEATURE_TOTAL_DOCUMENTS, aDataGrid.rowCount());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns an optional data document instance that represents the row of
	 * cells identified by the row number.
	 *
	 * @param aKeyName Key name for data grid
	 * @param aRowNumber Row number (1- N)
	 *
	 * @return Optional data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataDoc> getRowAsDoc(String aKeyName, long aRowNumber)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getRowAsDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		long rowNumber = Math.max(aRowNumber, 1);
		Optional<DataDoc> optDataDoc = Optional.empty();
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
		if (optDataGrid.isPresent())
		{
			DataGrid dataGrid = optDataGrid.get();
			rowNumber = Math.min(rowNumber, getRowCount(aKeyName));
			List<String> listValues = mCmdConnection.zrange(aKeyName, 0, 0);
			if (listValues.size() > 0)
			{
				DataDoc schemaDataDoc = stringToColumnSchema(listValues.get(0));
				if (schemaDataDoc.count() > 0)
				{
					dataGrid.setColumns(schemaDataDoc);
					listValues = mCmdConnection.zrange(aKeyName, rowNumber, rowNumber);
					if (listValues.size() > 0)
					{
						optDataDoc = listValueToRow(listValues.get(0));
						if (optDataDoc.isPresent())
							optDataDoc.get().addFeature(Redis.FEATURE_KEY_NAME, listValues.get(0));
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	/**
	 * Returns an optional data document instance that represents the row of
	 * cells identified by the row number.  The key name is obtained from the
	 * feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aRowNumber Row number (1- N)
	 *
	 * @return Optional data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public Optional<DataDoc> getRowAsDoc(DataGrid aDataGrid, int aRowNumber)
		throws RedisDSException
	{
		return getRowAsDoc(aDataGrid.getFeature(Redis.FEATURE_KEY_NAME), aRowNumber);
	}

	private String cleanCommandParameters(String aCmdParameter)
	{
		if (StringUtils.contains(aCmdParameter, "<DataDoc"))
		{
			String cmdParameter = StringUtils.replaceChars(aCmdParameter, StrUtl.CHAR_LESSTHAN, StrUtl.CHAR_PAREN_OPEN);
			return StringUtils.replaceChars(cmdParameter, StrUtl.CHAR_GREATERTHAN, StrUtl.CHAR_PAREN_CLOSE);
		}
		else
			return aCmdParameter;
	}

	private DataDoc expandCommandWithDocumentation(GridDS aDocCmdDS, DataDoc aCommandDoc)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "expandCommandWithDocumentation");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String redisCommand = aCommandDoc.getValueByName("redis_command");
		DataDoc redisCmdDoc = new DataDoc(String.format("'%s' Details", redisCommand));
		redisCmdDoc.add(new DataItem.Builder().name("id").title("Id").isPrimary(true).value(aCommandDoc.getValueByName("id")).build());
		redisCmdDoc.add(new DataItem.Builder().name("redis_command").title("Command Name").value(redisCommand).build());
		redisCmdDoc.add(new DataItem.Builder().name("redis_parameters").title("Command Parameters").value(cleanCommandParameters(aCommandDoc.getValueByName("redis_parameters"))).build());
		redisCmdDoc.add(new DataItem.Builder().name("command_link").title("Documentation Link").build());
		redisCmdDoc.add(new DataItem.Builder().name("command_description").title("Description").build());
		DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", redisCommand));
		dsCriteria.add("command_name", Data.Operator.EQUAL, redisCommand);
		DataGrid dataGrid = aDocCmdDS.fetch(dsCriteria);
		int rowCount = dataGrid.rowCount();
		if (rowCount > 0)
		{
			DataDoc redisDoc = dataGrid.getRowAsDoc(0);
			redisCmdDoc.setValueByName("command_link", redisDoc.getValueByName("command_link"));
			redisCmdDoc.setValueByName("command_description", redisDoc.getValueByName("command_description"));
		}
		else
		{
			dsCriteria.reset();
			int offset = redisCommand.indexOf(StrUtl.CHAR_DOT);
			if (offset == -1)
			{
				dsCriteria.add("command_name", Data.Operator.EQUAL, "*");
				dataGrid = aDocCmdDS.fetch(dsCriteria);
				rowCount = dataGrid.rowCount();
				if (rowCount == 0)
				{
					redisCmdDoc.setValueByName("command_link", "https://redis.io/commands");
					redisCmdDoc.setValueByName("command_description", "Redis database commands reference documentation.");
				}
				else
				{
					DataDoc redisDoc = dataGrid.getRowAsDoc(0);
					redisCmdDoc.setValueByName("command_link", redisDoc.getValueByName("command_link"));
					redisCmdDoc.setValueByName("command_description", redisDoc.getValueByName("command_description"));
				}
			}
			else
			{
				String commandMatch = String.format("%s*", redisCommand.substring(0, offset+1));
				dsCriteria.add("command_name", Data.Operator.STARTS_WITH, commandMatch);
				dataGrid = aDocCmdDS.fetch(dsCriteria);
				rowCount = dataGrid.rowCount();
				if (rowCount == 0)
				{
					redisCmdDoc.setValueByName("command_link", "https://redis.io/commands");
					redisCmdDoc.setValueByName("command_description", "Redis database commands reference documentation.");
				}
				else
				{
					DataDoc redisDoc = dataGrid.getRowAsDoc(0);
					redisCmdDoc.setValueByName("command_link", redisDoc.getValueByName("command_link"));
					redisCmdDoc.setValueByName("command_description", redisDoc.getValueByName("command_description"));
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return redisCmdDoc;
	}

	/**
	 * Convenience method that loads Redis commands from a stream and augments
	 * the information with Redis command documentation.
	 *
	 * @param aKeyName Key name of the command stream
	 * @param aStartId Id of start time range
	 * @param anEndId Id of end time range
	 * @param aBatchCount Batch count size
	 * @param aDocCmdDS Grid data source capturing command documentation
	 *
	 * @return Data grid instance
	 *
	 * @throws RedisDSException Redis Data Source exception
	 * @throws DSException Data source exception
	 */
	public DataGrid loadGridCommandsFromStream(String aKeyName, String aStartId, String anEndId,
											   int aBatchCount, GridDS aDocCmdDS)
		throws RedisDSException, DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadGridCommandsFromStream");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = new DataGrid("Redis Commands Documented");
		dataGrid.addCol(new DataItem.Builder().name("id").title("Id").isPrimary(true).build());
		dataGrid.addCol(new DataItem.Builder().name("redis_command").title("Command Name").build());
		dataGrid.addCol(new DataItem.Builder().name("redis_parameters").title("Command Parameters").build());
		dataGrid.addCol(new DataItem.Builder().name("command_link").title("Documentation Link").build());
		dataGrid.addCol(new DataItem.Builder().name("command_description").title("Description").build());

		RedisDoc redisDoc = mRedisDS.createDoc();
		List<DataDoc> dataDocList = redisDoc.loadDocs(aKeyName, Data.Order.DESCENDING, aStartId, anEndId, aBatchCount);
		for (DataDoc dataDoc : dataDocList)
		{
			if (StringUtils.isNotEmpty(dataDoc.getValueByName("redis_command")))
				dataGrid.addRow(expandCommandWithDocumentation(aDocCmdDS, dataDoc));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Inserts the data document instance in the Redis database after the
	 * row number specified.
	 *
	 * <b>Note:</b> This method does not add the data document
	 * instance to the end of the data grid instance.  You must
	 * reload the data grid from the Redis database.
	 *
	 * @param aKeyName Key name for data grid
	 * @param aAfterRowNumber Row number (1 - N) after which the data
	 *                        document should be added
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void insert(String aKeyName, int aAfterRowNumber, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "insert");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if ((StringUtils.isNotEmpty(aKeyName)) && (aDataDoc.count() > 0))
		{
			Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
			if (optDataGrid.isPresent())
			{
				DataGrid dataGrid = optDataGrid.get();
				writeRow(dataGrid, aDataDoc, aAfterRowNumber);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Inserts the data document instance in the Redis database after the
	 * row number specified.  The key name is obtained from the
	 * feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * <b>Note:</b> This method does not insert the data document
	 * instance within the data grid instance.  You must reload
	 * the data grid from the Redis database.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aAfterRowNumber Row number (1 - N) after which the data
	 *                        document should be added
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void insert(DataGrid aDataGrid, int aAfterRowNumber, DataDoc aDataDoc)
		throws RedisDSException
	{
		insert(aDataGrid.getFeature(Redis.FEATURE_KEY_NAME), aAfterRowNumber, aDataDoc);
	}

	/**
	 * Updates the row in the data grid identified by the key name and row
	 * number with the field/values contained within the data document
	 * instance.
	 *
	 * <b>Note:</b> This method does not update the data document
	 * instance within the data grid instance.  You must reload
	 * the data grid from the Redis database.
	 *
	 * @param aKeyName Key name for data grid
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(String aKeyName, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
		if (optDataGrid.isPresent())
		{
			RedisDoc redisDoc = mRedisDS.createDoc();
			redisDoc.update(aDataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the row in the data grid identified by the key name and row
	 * number with the field/values contained within the data document
	 * instance.  The key name is obtained from the feature
	 * Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * <b>Note:</b> This method does not update the data document
	 * instance within the data grid instance.  You must reload
	 * the data grid from the Redis database.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aRowNumber Row number (1 - N)
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void update(DataGrid aDataGrid, int aRowNumber, DataDoc aDataDoc)
		throws RedisDSException
	{
		mRedisDS.ensurePreconditions();
		Optional<DataDoc> optDataDoc = getRowAsDoc(aDataGrid, aRowNumber);
		if (optDataDoc.isPresent())
		{
			DataDoc dataDoc = optDataDoc.get();
			String keyName = dataDoc.getFeature(Redis.FEATURE_KEY_NAME);
			aDataDoc.addFeature(Redis.FEATURE_KEY_NAME, keyName);
			RedisDoc redisDoc = mRedisDS.createDoc();
			redisDoc.update(dataDoc, aDataDoc);
		}
		else
			throw new RedisDSException(String.format("%s: Unable to load row number %d for update.", aDataGrid.getName(), aRowNumber));
	}

	/**
	 * Queries Redis for the memory usage (in bytes) of the value
	 * data structure identified by the data document.
	 *
	 * @see <a href="https://redis.io/commands/memory-usage">Redis Command</a>
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Memory usage in bytes
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public long memoryUsage(DataGrid aDataGrid)
		throws RedisDSException
	{
		List<String> listValues;
		Logger appLogger = mAppCtx.getLogger(this, "memoryUsage");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		long memorySizeInBytes = 0;
		String keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(keyName);
		if (optDataGrid.isPresent())
		{
			RedisCore redisCore = mRedisDS.createCore();
			listValues = mCmdConnection.zrange(keyName, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
			mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(keyName), Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH));
			for (String listValue : listValues)
				memorySizeInBytes += redisCore.memoryUsage(listValue);
			memorySizeInBytes += redisCore.memoryUsage(keyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return memorySizeInBytes;
	}

	/**
	 * Deletes the row identified by the row number from the Redis database.
	 * The key name is obtained from the feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aKeyName Key name for data grid
	 * @param aRowNumber Row number (1 - N)
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(String aKeyName, long aRowNumber)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(aKeyName);
		if ((optDataGrid.isPresent()) && (aRowNumber > 0))
		{
			List<String> listValues = mCmdConnection.zrange(aKeyName, aRowNumber, aRowNumber);
			if (listValues.size() > 0)
			{
				RedisCore redisCore = mRedisDS.createCore();
				String listValue = listValues.get(0);
				redisCore.delete(listValue);
				long deleteCount = mCmdConnection.zrem(aKeyName, listValue);
				mRedisDS.saveCommand(appLogger, String.format("ZREM %s %s", mRedisDS.escapeKey(aKeyName), mRedisDS.escapeValue(listValue)));
				if (deleteCount == 1)
				{
					listValues = mCmdConnection.zrange(aKeyName, aRowNumber, Redis.GRID_RANGE_FINISH);
					mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(aKeyName), Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH));
					if (listValues.size() > 0)
					{
						Transaction cmdTransaction = mCmdConnection.multi();
						mRedisDS.saveCommand(appLogger, "MULTI");
						for (String lValue : listValues)
						{
							cmdTransaction.zincrby(aKeyName, -1, lValue);
							mRedisDS.saveCommand(appLogger, String.format("ZINCRBY %s -1 %s", mRedisDS.escapeKey(aKeyName), mRedisDS.escapeValue(lValue)));
						}
						cmdTransaction.exec();
						mRedisDS.saveCommand(appLogger, "EXEC");
					}
				}
				else
					appLogger.warn(String.format("Key '%s': Delete count is %d.", aKeyName, deleteCount));
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes the row identified by the row number from the Redis database.
	 * The key name is obtained from the feature Redis.REDIS_FEATURE_KEY_NAME.
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
	 * Deletes the row identified by the data document instance from the Redis database.
	 * The key name is obtained from the feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(DataGrid aDataGrid, DataDoc aDataDoc)
		throws RedisDSException
	{
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Brute force logic below - load entire grid in memory to identify our row number.

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataItem diPrimaryKey = optDataItem.get();
			String gridKeyName = mRedisKey.moduleCore().redisSortedSet().dataObject(aDataGrid).name();
			DataGrid dataGrid = new DataGrid(aDataGrid.getName());
			dataGrid.addFeature(Redis.FEATURE_KEY_NAME, gridKeyName);
			mRedisDS.setCommandStreamActiveFlag(false);
			loadGridPipeline(dataGrid, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
			mRedisDS.setCommandStreamActiveFlag(true);
			int delRowNumber = -1;
			int rowCount = dataGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = dataGrid.getRowAsDoc(row);
				if (dataDoc.getValueByName(diPrimaryKey.getName()).equals(diPrimaryKey.getValue()))
				{
					delRowNumber = row + 1;
					break;
				}
			}
			if (delRowNumber == -1)
			{
				String msgStr = String.format("Unable to locate a item by primary key ('%s' = '%s').", diPrimaryKey.getName(), diPrimaryKey.getValue());
				appLogger.error(msgStr);
				throw new RedisDSException(msgStr);
			}
			delete(dataGrid, delRowNumber);
		}
		else
		{
			String msgStr = String.format("[%s]: Unable to locate a primary key data item.", aDataDoc.getName());
			appLogger.error(msgStr);
			throw new RedisDSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes the data grid instance from the Redis database.  The key name
	 * is obtained from the * feature Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void delete(DataGrid aDataGrid)
		throws RedisDSException
	{
		long deleteCount;
		List<String> listValues;
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		String keyName = aDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
		Optional<DataGrid> optDataGrid = mRedisKey.toDataGrid(keyName);
		if (optDataGrid.isPresent())
		{
			RedisCore redisCore = mRedisDS.createCore();
			listValues = mCmdConnection.zrange(keyName, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
			mRedisDS.saveCommand(appLogger, String.format("ZRANGE %s %d %d", mRedisDS.escapeKey(keyName), Redis.GRID_RANGE_SCHEMA, Redis.GRID_RANGE_FINISH));
			deleteCount = redisCore.delete(listValues);
			if (deleteCount != listValues.size())
				appLogger.warn(String.format("Key '%s': Delete list count requested was %d and response count is %d.", keyName, listValues.size(), deleteCount));
			deleteCount = redisCore.delete(keyName);
			if (deleteCount != 1)
				appLogger.warn(String.format("Key '%s': Delete count is %d.", keyName, deleteCount));
			aDataGrid.emptyRows();
			aDataGrid.disableFeature(Redis.FEATURE_KEY_NAME);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
