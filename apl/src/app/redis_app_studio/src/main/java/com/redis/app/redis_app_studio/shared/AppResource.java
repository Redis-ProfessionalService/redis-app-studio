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

package com.redis.app.redis_app_studio.shared;

import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_json.JsonDS;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.ds.ds_redis.graph.RedisGraphs;
import com.redis.ds.ds_redis.json.RedisJson;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.CfgMgr;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGraph;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

/* Referring to Redis App Studio grid fetch, add form, edit form, delete operations.
   You need to determine how to declare that saved a data source snapshot during
   the fetch operation and can use that for a restore to handle add, update and
   delete.  Just work through the logic.
   Build and Establish methods - that's it

   Every SmartClient data source that gets generated needs to have a hidden
   field added to the end of it called 'ras_context' and it will be used for
   add, update, delete operations to save the prefix, ds, and title.

   let lgRecord = graphGridRel.getSelectedRecord();
   lgRecord.ras_context = appPrefix|dataStructure|dsTitle
   scForm.setValue("ras_context", "appPrefix|dataStructure|dsTitle");

   Note: The data target is implied by the SC data sources that are invoked.
 */

/**
 * The AppResource is responsible for managing data resources
 * (both source and target) for the parent application.
 */
public class AppResource
{
	private String mTitle;									// Data source title
	private String mPrefix;									// Application prefix
	private String mStructure;								// Data structure ('flat' or 'hierarchy')
	private GridDS mGridDS;									// Foundation grid data source (file system)
	private JsonDS mJsonDS;									// Foundation JSON data source (file system)
	private GraphDS mGraphDS;								// Foundation graph data source (file system)
	private GridDS mResGridDS;								// Foundation grid data source (holding search results)
	private DataGraph mDataGraph;							// Foundation data graph (holding search results)
	private DataDoc mGraphNodeDoc;							// Foundation data document (holding graph node doc)
	private RedisDS mRedisDS;								// Redis core data source
	private GridDS mSchemaGridDS;							// Active schema data source grid
	private RedisJson mRedisJson;							// Redis JSON data source
	private GridDS mRedisDocGridDS;							// Foundation grid data source (Redis cmd doc links)
	private RedisGraphs mRedisGraph;						// Redis graph data source
	private RedisSearch mRedisSearch;						// Redis search data source
	private GridDS mStreamCmdDocGridDS;						// Foundation grid data source (holding stream cmd with docs)
	private final AppCtx mAppCtx;							// Application context
	private Constants.DataType mDataType;					// Data type focus of the application

	/**
	 * Constructs an application resource
	 *
	 * @param anAppCtx Application context instance
	 * @param aPrefix Application prefix
	 * @param aStructure Data structure ('flat' or 'hierarchy')
	 * @param aTitle Application title
	 */
	public AppResource(AppCtx anAppCtx, String aPrefix, String aStructure, String aTitle)
	{
		mAppCtx = anAppCtx;
		setPrefix(aPrefix);
		setStructure(aStructure);
		setTitle(aTitle);
		setDataType(Constants.DataType.Undefined);
	}

	/**
	 * Assigns the application prefix.
	 *
	 * @param anAppPrefix Application prefix string
	 */
	public void setPrefix(String anAppPrefix)
	{
		mPrefix = anAppPrefix;
	}

	/**
	 * Retrieves the application prefix.
	 *
	 * @return Application prefix string
	 */
	public String getPrefix()
	{
		return mPrefix;
	}

	/**
	 * Assigns the data structure.
	 *
	 * @param aStructure Data structure string ('flat' or 'hierarchy')
	 */
	public void setStructure(String aStructure)
	{
		mStructure = aStructure;
	}

	/**
	 * Retrieves the data structure.
	 *
	 * @return Data structure string ('flat' or 'hierarchy')
	 */
	public String getStructure()
	{
		return mStructure;
	}

	/**
	 * Assigns the application title.
	 *
	 * @param anAppTitle Application title string
	 */
	public void setTitle(String anAppTitle)
	{
		mTitle = anAppTitle;
	}

	/**
	 * Retrieves the application title.
	 *
	 * @return Application title
	 */
	public String getTitle()
	{
		return mTitle;
	}

	/**
	 * Assigns the data type.
	 *
	 * @param aType Data type
	 */
	public void setDataType(Constants.DataType aType)
	{
		mDataType = aType;
	}

	/**
	 * Retrieves the application data type.
	 *
	 * @return Application data type
	 */
	public Constants.DataType getDataType()
	{
		return mDataType;
	}

	/**
	 * Assigns the data grid instance to a grid data source.
	 *
	 * @param aDataGrid Data grid instance
	 */
	public void setGridDS(DataGrid aDataGrid)
	{
		mGridDS = new GridDS(mAppCtx, aDataGrid);
		mGridDS.getDataGrid().setName(mTitle);
		mGridDS.getDataGrid().getColumns().setName(mTitle);
	}

	/**
	 * Assigns the main grid data source.
	 *
	 * @param aGridDS Grid data source instance
	 */
	public void setGridDS(GridDS aGridDS)
	{
		mGridDS = aGridDS;
	}

	/**
	 * Returns the grid data source instance.
	 *
	 * @return Grid data source instance
	 */
	public GridDS getGridDS()
	{
		return mGridDS;
	}

	/**
	 * Assigns the result grid data source.
	 *
	 * @param aDataGrid Data grid instance
	 */
	public void setResultDS(DataGrid aDataGrid)
	{
		mResGridDS = new GridDS(mAppCtx, aDataGrid);
	}

	/**
	 * Assigns the result grid data source.
	 *
	 * @param aGridDS Grid data source instance
	 */
	public void setResultDS(GridDS aGridDS)
	{
		mResGridDS = aGridDS;
	}

	/**
	 * Returns the result grid data source instance.
	 *
	 * @return Grid data source instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public GridDS getResultDS()
		throws RedisDSException
	{
		if (mResGridDS == null)
			throw new RedisDSException("Result Grid DS is undefined - session likely expired - please refresh application.");
		else
			return mResGridDS;
	}

	/**
	 * Assigns the JSON data source instance.
	 *
	 * @param aJsonDS JSON data source instance
	 */
	public void setJsonDS(JsonDS aJsonDS)
	{
		mJsonDS = aJsonDS;
	}

	/**
	 * Returns the JSON data source instance.
	 *
	 * @return JSON data source instance
	 */
	public JsonDS getJsonDS()
	{
		return mJsonDS;
	}

	/**
	 * Assigns the RedisJson data source instance.
	 *
	 * @param aRedisJson RedisJson data source instance
	 */
	public void setRedisJsonDS(RedisJson aRedisJson)
	{
		mRedisJson = aRedisJson;
	}

	/**
	 * Returns the RedisJson data source instance.
	 *
	 * @return Redis RedisJson source instance
	 */
	public RedisJson getRedisJson()
	{
		return mRedisJson;
	}

	/**
	 * Assigns the RediSearch data source instance.
	 *
	 * @param aRedisSearch RediSearch data source instance
	 */
	public void setRedisSearchDS(RedisSearch aRedisSearch)
	{
		mRedisSearch = aRedisSearch;
	}

	/**
	 * Returns the RediSearch data source instance.
	 *
	 * @return RediSearch data source instance.
	 */
	public RedisSearch getRedisSearch()
	{
		return mRedisSearch;
	}

	/**
	 * Assigns a main graph data source instance.
	 *
	 * @param aGraphDS Graph data source instance
	 */
	public void setGraphDS(GraphDS aGraphDS)
	{
		mGraphDS = aGraphDS;
	}

	/**
	 * Returns a main graph data source instance.
	 *
	 * @return Graph data source instance
	 */
	public GraphDS getGraphDS()
	{
		return mGraphDS;
	}

	/**
	 * Assigns a result graph data source instance.
	 *
	 * @param aDataGraph Data graph instance
	 */
	public void setResultDataGraph(DataGraph aDataGraph)
	{
		mDataGraph = aDataGraph;
	}

	/**
	 * Returns a result data graph instance.
	 *
	 * @return Data graph instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGraph getResultDataGraph()
		throws RedisDSException
	{
		return mDataGraph;
	}

	/**
	 * Assigns a graph node data document instance.
	 *
	 * @param aGraphNodeDoc Data document instance
	 */
	public void setGraphNodeDocument(DataDoc aGraphNodeDoc)
	{
		mGraphNodeDoc = aGraphNodeDoc;
	}

	/**
	 * Returns a graph node data document instance.
	 *
	 * @return Data document instance
	 */
	public DataDoc getGraphNodeDocument()
	{
		return mGraphNodeDoc;
	}

	/**
	 * Assigns the RedisGraph data source instance.
	 *
	 * @param aRedisGraph RedisGraph data source instance
	 */
	public void setRedisGraphDS(RedisGraphs aRedisGraph)
	{
		mRedisGraph = aRedisGraph;
	}

	/**
	 * Returns the RedisGraph data source instance.
	 *
	 * @return Redis RedisGraph source instance
	 */
	public RedisGraphs getRedisGraph()
	{
		return mRedisGraph;
	}

	/**
	 * Assigns a schema data source instance.
	 *
	 * @param aGridDS Graph data source instance
	 */
	public void setSchemaDS(GridDS aGridDS)
	{
		mSchemaGridDS = aGridDS;
	}

	/**
	 * Returns the schema data source instance.
	 *
	 * @return Grid data source instance
	 */
	public GridDS getSchemaDS()
	{
		return mSchemaGridDS;
	}

	/**
	 * Assigns the Redis document grid data source instance.
	 *
	 * @param aGridDS Grid data source instance
	 */
	public void setRedisDocGridDS(GridDS aGridDS)
	{
		mRedisDocGridDS = aGridDS;
	}

	/**
	 * Return the Redis document grid data source instance.
	 *
	 * @return Grid data source instance
	 */
	public GridDS getRedisDocGridDS()
	{
		return mRedisDocGridDS;
	}

	/**
	 * Assigns the Redis stream of commands grid data source.
	 *
	 * @param aGridDS Grid data source instance
	 */
	public void setStreamCmdDocGridDS(GridDS aGridDS)
	{
		mStreamCmdDocGridDS = aGridDS;
	}

	/**
	 * Returns the Redis stream of commands grid data source.
	 *
	 * @return Grid data source instance
	 */
	public GridDS getStreamCmdDocGridDS()
	{
		return mStreamCmdDocGridDS;
	}

	/**
	 * Assigns the main Redis data source instance.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public void setRedisDS(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
	}

	/**
	 * Returns the Redis data source instance.
	 *
	 * @return Redis data source instance
	 */
	public RedisDS getRedisDS()
	{
		return mRedisDS;
	}

	/**
	 * Returns <i>true</i> if the current data type represents a Document object or <i>false</i> otherwise.
	 *
	 * @return <i>true</i> if the current data type represents a Document object or <i>false</i> otherwise
	 */
	public boolean isDocumentDataType()
	{
		return mDataType == Constants.DataType.Document;
	}

	/**
	 * Returns <i>true</i> if the current data type represents a Search object or <i>false</i> otherwise.
	 *
	 * @return <i>true</i> if the current data type represents a Search object or <i>false</i> otherwise
	 */
	public boolean isSearchDataType()
	{
		return mDataType == Constants.DataType.Search;
	}

	/**
	 * Returns <i>true</i> if the current data type represents a JSON object or <i>false</i> otherwise.
	 *
	 * @return <i>true</i> if the current data type represents a JSON object or <i>false</i> otherwise
	 */
	public boolean isJsonDataType()
	{
		return mDataType == Constants.DataType.JSON;
	}

	/**
	 * Returns <i>true</i> if the current data type represents a Graph object or <i>false</i> otherwise.
	 *
	 * @return <i>true</i> if the current data type represents a Graph object or <i>false</i> otherwise
	 */
	public boolean isGraphDataType()
	{
		return mDataType == Constants.DataType.Graph;
	}

	/**
	 * Returns <i>true</i> if the current data type represents a TimeSeries object or <i>false</i> otherwise.
	 *
	 * @return <i>true</i> if the current data type represents a TimeSeries object or <i>false</i> otherwise
	 */
	public boolean isTimeSeriesDataType()
	{
		return mDataType == Constants.DataType.TimeSeries;
	}

	public GridDS loadRedisCommandsDS()
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadRedisCommandsDS");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mRedisDocGridDS == null)
		{
			CfgMgr cfgMgr = mRedisDS.getCfgMgr();
			String dcFileName = cfgMgr.getString("doccommands.file_name", Constants.DS_REDIS_COMMANDS_NAME);
			String dcPathFileName = String.format("%s%c%s", mAppCtx.getProperty(mAppCtx.APP_PROPERTY_CFG_PATH), File.separatorChar, dcFileName);
			try
			{
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				mRedisDocGridDS = new GridDS(mAppCtx);
				mRedisDocGridDS.loadData(dcPathFileName, true);
				mRedisDocGridDS.getSchema().getItemByName("command_name").enableFeature(Data.FEATURE_IS_PRIMARY);
				stopWatch.stop();
				DataGrid dataGrid = mRedisDocGridDS.getDataGrid();
				appLogger.debug(String.format("'%s': %d columns and %d rows in %d milliseconds.", dcPathFileName,
											  dataGrid.colCount(), dataGrid.rowCount(), stopWatch.getTime()));
			}
			catch (IOException e)
			{
				String errMsg = String.format("[%s]: Unable to load content type file '%s': %s", mPrefix, dcPathFileName, e.getMessage());
				appLogger.error(errMsg);
				throw new DSException(errMsg);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return mRedisDocGridDS;
	}

	private void populateRedisCore()
		throws RedisDSException
	{
		String propertyPrefix = "rc.redis";
		Logger appLogger = mAppCtx.getLogger(this, "populateRedisCore");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		CfgMgr cfgMgr = mRedisDS.getCfgMgr();
		if (cfgMgr.isStringTrue("data_load_at_startup"))
		{
			DataGrid dataGrid = mGridDS.getDataGrid();
			dataGrid.setName(mTitle);
			dataGrid.getColumns().setName(mTitle);
			RedisKey redisKey = mRedisDS.getRedisKey();
			RedisGrid redisGrid = mRedisDS.createGrid();
			RedisCore redisCore = mRedisDS.createCore();
			String dataGridKeyName = redisKey.moduleCore().redisSortedSet().dataObject(dataGrid).name();
			if (! redisCore.exists(dataGridKeyName))
			{
				appLogger.debug(String.format("[%s]: Redis core data grid key does not exist.", dataGridKeyName));
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				mRedisDS.startMarker(String.format("[%s] Add Data Grid", mTitle));
				redisGrid.add(dataGrid);
				mRedisDS.finishMarker();
				stopWatch.stop();
				appLogger.debug(String.format("[%s] Added %d rows in %d milliseconds.", dataGridKeyName, dataGrid.rowCount(), stopWatch.getTime()));
			}
			else
				appLogger.debug(String.format("[%s] Redis core data grid key exists in Redis DB.", dataGridKeyName));
		}
		else
			appLogger.debug(String.format("[%s]: Auto loading of data into Redis DB is 'false'.", propertyPrefix));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void openPopulateRedisCore()
		throws RedisDSException
	{
		String propertyPrefix = "rc.redis";
		Logger appLogger = mAppCtx.getLogger(this, "openPopulateRedisCore");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS = new RedisDS(mAppCtx);
		mRedisDS.open(mPrefix);
		mRedisDS.setCfgPropertyPrefix(propertyPrefix);
		mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
		populateRedisCore();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void populateRediSearchHash()
		throws RedisDSException
	{
		RedisSearch redisSearch;
		String propertyPrefix = "rs.redis";
		Logger appLogger = mAppCtx.getLogger(this, "populateRediSearchHash");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = mGridDS.getDataGrid();
		dataGrid.setName(mTitle);
		dataGrid.getColumns().setName(mTitle);
		String indexName = Data.titleToName(mTitle);
		if (mRedisSearch == null)
		{
			redisSearch = mRedisDS.createSearch(Redis.Document.Hash);
			DataDoc dataSchemaDoc = dataGrid.getColumns();
			redisSearch.setDataSchema(dataSchemaDoc);
			mRedisDS.startMarker(String.format("[%s] Create Search Index & Schema for Hashes", indexName));
			DataDoc searchSchemaDoc = redisSearch.createSchemaDoc(dataSchemaDoc, indexName);
			mRedisDS.finishMarker();
			redisSearch.setSearchSchema(searchSchemaDoc);
			setRedisSearchDS(redisSearch);
		}
		else
			redisSearch = mRedisSearch;

		CfgMgr cfgMgr = mRedisDS.getCfgMgr();
		if (cfgMgr.isStringTrue("data_load_at_startup"))
		{
			if (! redisSearch.schemaExists())
			{
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				mRedisDS.startMarker(String.format("[%s] Adding Data Grid Documents", indexName));
				redisSearch.add(dataGrid);
				mRedisDS.finishMarker();
				stopWatch.stop();
				appLogger.debug(String.format("[%s] Added %d hash documents in %d milliseconds.", indexName, dataGrid.rowCount(), stopWatch.getTime()));
			}
			else
				appLogger.debug(String.format("[%s] Schema key exists in Redis DB.", indexName));
		}
		else
			appLogger.debug(String.format("[%s]: Auto loading of data into Redis DB is 'false'.", propertyPrefix));


		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void openPopulateRediSearchHash()
		throws RedisDSException
	{
		String propertyPrefix = "rs.redis";
		Logger appLogger = mAppCtx.getLogger(this, "openPopulateRediSearchHash");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS = new RedisDS(mAppCtx);
		mRedisDS.open(mPrefix);
		mRedisDS.setCfgPropertyPrefix(propertyPrefix);
		mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
		populateRediSearchHash();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void populateRediSearchJson()
		throws RedisDSException
	{
		RedisSearch redisSearch;
		String propertyPrefix = "rs.redis";
		Logger appLogger = mAppCtx.getLogger(this, "populateRediSearchJson");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mRedisJson == null)
			setRedisJsonDS(mRedisDS.createJson(mJsonDS.getSchema()));
		DataGrid dataGrid = mJsonDS.getDataGrid();;
		dataGrid.setName(mTitle);
		dataGrid.getColumns().setName(mTitle);
		String indexName = Data.titleToName(mTitle);
		if (mRedisSearch == null)
		{
			redisSearch = mRedisDS.createSearch(Redis.Document.JSON);
			DataDoc dataSchemaDoc = dataGrid.getColumns();
			redisSearch.setDataSchema(dataSchemaDoc);
			mRedisDS.startMarker(String.format("[%s] Create Search Index & Schema for JSON Documents", indexName));
			DataDoc searchSchemaDoc = redisSearch.createSchemaDoc(dataSchemaDoc, indexName);
			mRedisDS.finishMarker();
			redisSearch.setSearchSchema(searchSchemaDoc);
			setRedisSearchDS(redisSearch);
		}
		else
			redisSearch = mRedisSearch;
		CfgMgr cfgMgr = mRedisDS.getCfgMgr();
		if (cfgMgr.isStringTrue("data_load_at_startup"))
		{
			if (! redisSearch.schemaExists())
			{
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				mRedisDS.startMarker("Adding JSON documents to Redis and search index.");
				redisSearch.add(dataGrid);
				mRedisDS.finishMarker();
				stopWatch.stop();
				String keyName = dataGrid.getFeature(Redis.FEATURE_KEY_NAME);
				appLogger.debug(String.format("[%s] Added %d documents in %d milliseconds.", keyName, dataGrid.rowCount(), stopWatch.getTime()));
			}
			else
				appLogger.debug(String.format("[%s] Schema key exists in Redis DB.", indexName));
		}
		else
			appLogger.debug(String.format("[%s]: Auto loading of data into Redis DB is 'false'.", propertyPrefix));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void openPopulateRediSearchJson()
		throws RedisDSException
	{
		String propertyPrefix = "rs.redis";
		Logger appLogger = mAppCtx.getLogger(this, "openPopulateRediSearchJson");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS = new RedisDS(mAppCtx);
		mRedisDS.open(mPrefix);
		mRedisDS.setCfgPropertyPrefix(propertyPrefix);
		mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
		populateRediSearchJson();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * This method purposely stores a subset of the original JSON objects into
	 * RedisJSON because that is all that RediSearch will be able to query
	 * against.  You can change this logic to store the complete JSON objects
	 * (tested and validated via 'ds_redis') however, you will not be able to
	 * perform date/time queries because shadow fields are not supported with
	 * the full JSON objects.
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	private void populateRedisJson()
		throws RedisDSException
	{
		RedisJson redisJson;
		String propertyPrefix = "rj.redis";
		Logger appLogger = mAppCtx.getLogger(this, "populateRedisJson");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mRedisJson == null)
		{
			redisJson = mRedisDS.createJson(mJsonDS.getSchema());
			setRedisJsonDS(redisJson);
		}
		else
			redisJson = mRedisJson;
		CfgMgr cfgMgr = mRedisDS.getCfgMgr();
		if (cfgMgr.isStringTrue("data_load_at_startup"))
		{
			DataGrid dataGrid = mJsonDS.getDataGrid();;
			dataGrid.setName(mTitle);
			dataGrid.getColumns().setName(mTitle);
			RedisKey redisKey = mRedisDS.getRedisKey();
			RedisCore redisCore = mRedisDS.createCore();
			String dataGridKeyName = redisKey.moduleJson().redisJsonDocument().dataObject(dataGrid).name();
			if (! redisCore.exists(dataGridKeyName))
			{
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				mRedisDS.startMarker("Adding JSON documents.");
				redisJson.add(dataGrid);
				mRedisDS.finishMarker();
				stopWatch.stop();
				String keyName = dataGrid.getFeature(Redis.FEATURE_KEY_NAME);
				appLogger.debug(String.format("[%s] Added %d documents in %d milliseconds.", keyName, dataGrid.rowCount(), stopWatch.getTime()));
			}
			else
				appLogger.debug(String.format("[%s] Redis JSON data grid key exists in Redis DB.", dataGridKeyName));
		}
		else
			appLogger.debug(String.format("[%s]: Auto loading of data into Redis DB is 'false'.", propertyPrefix));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void openPopulateRedisJson()
		throws RedisDSException
	{
		String propertyPrefix = "rj.redis";
		Logger appLogger = mAppCtx.getLogger(this, "openPopulateRedisJson");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS = new RedisDS(mAppCtx);
		mRedisDS.open(mPrefix);
		mRedisDS.setCfgPropertyPrefix(propertyPrefix);
		mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
		populateRedisJson();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void populateRedisGraph()
		throws FCException
	{
		RedisGraphs redisGraph;
		String propertyPrefix = "rg.redis";
		Logger appLogger = mAppCtx.getLogger(this, "populateRedisGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mRedisGraph == null)
		{
			redisGraph = mRedisDS.createGraph(mGraphDS.getVertexSchema(), mGraphDS.getEdgeSchema());
			setRedisGraphDS(redisGraph);
		}
		else
			redisGraph = mRedisGraph;
		CfgMgr cfgMgr = mRedisDS.getCfgMgr();
		if (cfgMgr.isStringTrue("data_load_at_startup"))
		{
			DataGraph dataGraph = mGraphDS.createDataGraph(mTitle);
			String graphKeyName = redisGraph.graphKeyName(dataGraph.getName());
			RedisCore redisCore = mRedisDS.createCore();
			if (! redisCore.exists(graphKeyName))
			{
				StopWatch stopWatch = new StopWatch();
				stopWatch.start();
				mRedisDS.startMarker("Adding data graph nodes and relationships.");
				redisGraph.add(dataGraph);
				mRedisDS.finishMarker();
				stopWatch.stop();
				appLogger.debug(String.format("[%s] Added %d nodes and %d relationships in %d milliseconds.", graphKeyName, mGraphDS.vertexRowCount(), mGraphDS.edgeRowCount(), stopWatch.getTime()));
			}
			else
				appLogger.debug(String.format("[%s] Redis graph key exists in Redis DB.", graphKeyName));
		}
		else
			appLogger.debug(String.format("[%s]: Auto loading of data into Redis DB is 'false'.", propertyPrefix));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void openPopulateRedisGraph()
		throws FCException
	{
		String propertyPrefix = "rg.redis";
		Logger appLogger = mAppCtx.getLogger(this, "openPopulateRedisGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS = new RedisDS(mAppCtx);
		mRedisDS.open(mPrefix);
		mRedisDS.setCfgPropertyPrefix(propertyPrefix);
		mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
		populateRedisGraph();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void populateRedisTimeSeries()
		throws RedisDSException
	{
		String propertyPrefix = "rt.redis";
		Logger appLogger = mAppCtx.getLogger(this, "populateRedisTimeSeries");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void openPopulateRedisTimeSeries()
		throws RedisDSException
	{
		String propertyPrefix = "rt.redis";
		Logger appLogger = mAppCtx.getLogger(this, "openPopulateRedisTimeSeries");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS = new RedisDS(mAppCtx);
		mRedisDS.open(mPrefix);
		mRedisDS.setCfgPropertyPrefix(propertyPrefix);
		mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
		populateRedisTimeSeries();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Create the data sources, loads data and populates the Redis database.
	 *
	 * @param aDataSource Data source name
	 * @param aDataTarget Data target name
	 *
	 * @throws IOException I/O exception
	 * @throws DSException Data source exception
	 * @throws RedisDSException Redis data source exception
	 */
	public void create(String aDataSource, String aDataTarget)
		throws FCException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "create");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataLoader dataLoader = new DataLoader(mAppCtx, mStructure, mTitle);
		setDataType(dataLoader.deriveDataType());
		switch (getDataType())
		{
			case Document:
				mGridDS = dataLoader.retrieveGridDSFromFileSystem();
				break;
			case Graph:
				mGraphDS = dataLoader.retrieveGraphDSFromFileSystem();
				break;
			case JSON:
				mJsonDS = dataLoader.retrieveJsonDSFromFileSystem();
				break;
			case TimeSeries:
			default:
				throw new DSException(String.format("[%s] Application resource data source is not supported.", aDataSource));
		}

		switch (aDataTarget)
		{
			case Constants.APPSES_TARGET_MEMORY:
				// Nothing to do here - we have what we need with the GridDS
				break;
			case Constants.APPSES_TARGET_REDIS_CORE:
				openPopulateRedisCore();
				break;
			case Constants.APPSES_TARGET_REDIS_JSON:
				openPopulateRedisJson();
				break;
			case Constants.APPRES_TARGET_REDIS_SEARCH_HASH:
				openPopulateRediSearchHash();
				break;
			case Constants.APPRES_TARGET_REDIS_SEARCH_JSON:
				openPopulateRediSearchJson();
				break;
			case Constants.APPSES_TARGET_REDIS_GRAPH:
				openPopulateRedisGraph();
				break;
			case Constants.APPSES_TARGET_REDIS_TIMESERIES:
				openPopulateRedisTimeSeries();
			default:
				throw new DSException(String.format("[%s] Application resource data target is not supported.", aDataTarget));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Refreshes (reload) the data set in the Redis database if auto-loading is enabled.
	 *
	 * @param aDataTarget Data target name
	 *
	 * @throws IOException I/O exception
	 * @throws DSException Data source exception
	 * @throws RedisDSException Redis data source exception
	 */
	public void refreshData(String aDataTarget)
		throws FCException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "refreshData");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		switch (aDataTarget)
		{
			case Constants.APPSES_TARGET_MEMORY:
				// Nothing to do here - we have what we need with the GridDS
				break;
			case Constants.APPSES_TARGET_REDIS_CORE:
				populateRedisCore();
				break;
			case Constants.APPSES_TARGET_REDIS_JSON:
				populateRedisJson();
				break;
			case Constants.APPSES_TARGET_REDIS_GRAPH:
				populateRedisGraph();
				break;
			case Constants.APPRES_TARGET_REDIS_SEARCH_HASH:
				populateRediSearchHash();
				break;
			case Constants.APPRES_TARGET_REDIS_SEARCH_JSON:
				populateRediSearchJson();
				break;
			case Constants.APPSES_TARGET_REDIS_TIMESERIES:
				populateRedisTimeSeries();
			default:
				throw new DSException(String.format("[%s] Application resource data target is not supported.", aDataTarget));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Logs the state of the application resource.
	 *
	 * @param aLogger Application logger instance
	 * @param aState Resource state
	 */
	public void logDebug(Logger aLogger, String aState)
	{
		String entryId = String.format("%s:AppResource[%s|%s|%s]", aState, mPrefix, mStructure, mTitle);
		StringBuilder stringBuilder = new StringBuilder(entryId);
		try
		{
			int entryCount = 0;
			if (mGridDS != null)
			{
				stringBuilder.append(String.format("GridDS(%d)", mGridDS.getDataGrid().rowCount()));
				entryCount++;
			}
			if (mJsonDS != null)
			{
				if (entryCount > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				stringBuilder.append(String.format("JsonDS(%d)", mJsonDS.getDataGrid().rowCount()));
				entryCount++;
			}
			if (mGraphDS != null)
			{
				if (entryCount > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				stringBuilder.append(String.format("GraphDS(%d,%d)", mGraphDS.vertexRowCount(), mGraphDS.edgeRowCount()));
				entryCount++;
			}
			if (mResGridDS != null)
			{
				if (entryCount > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				stringBuilder.append(String.format("ResGridDS(%d)", mResGridDS.getDataGrid().rowCount()));
				entryCount++;
			}
			if (mDataGraph != null)
			{
				if (entryCount > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				stringBuilder.append(String.format("DataGraph(%d)", mDataGraph.getVertexDataGrid().rowCount()));
				entryCount++;
			}
			if (mRedisDS != null)
			{
				if (entryCount > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				stringBuilder.append(String.format("RedisDS(%s,%d)", mRedisDS.getHostName(), mRedisDS.getPortNumber()));
				entryCount++;
			}
			if (mSchemaGridDS != null)
			{
				if (entryCount > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				stringBuilder.append(String.format("SchemaGridDS(%d)", mSchemaGridDS.getDataGrid().rowCount()));
			}
		}
		catch (FCException e)
		{
			stringBuilder.append(String.format("FCException: %s", e.getMessage()));
		}
		aLogger.debug(stringBuilder.toString());
	}
}
