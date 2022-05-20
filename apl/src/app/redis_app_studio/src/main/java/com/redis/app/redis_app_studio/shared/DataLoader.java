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
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DSCriteriaLogger;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.FilUtl;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * The Data loader is responsible to loading data files from the
 * filesystem, establishing Redis connections and bootstrapping
 * the databases with data.  In addition, it will assign connection
 * resources to the application context object.
 */
public class DataLoader
{
	private final String mTitle;							// Data source title
	private final String mStructure;						// Data structure ('flat' or 'hierarchy')
	private final AppCtx mAppCtx;							// Application context

	/**
	 * Constructs an data loader instance
	 *
	 * @param anAppCtx Application context instance
	 */
	public DataLoader(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
		mStructure = "Undefined";
		mTitle = "Undefined";
	}

	/**
	 * Constructs an data loader instance
	 *
	 * @param anAppCtx Application context instance
	 * @param aStructure Data structure name
	 */
	public DataLoader(AppCtx anAppCtx, String aStructure)
	{
		mAppCtx = anAppCtx;
		mStructure = aStructure;
		mTitle = "Undefined";
	}

	/**
	 * Constructs an data loader instance
	 *
	 * @param anAppCtx Application context instance
	 * @param aStructure Data structure name
	 * @param aTitle Title of the data source
	 */
	public DataLoader(AppCtx anAppCtx, String aStructure, String aTitle)
	{
		mAppCtx = anAppCtx;
		mStructure = aStructure;
		mTitle = aTitle;
	}

	/**
	 * Derives the storage path file name from the current web server file
	 * system configuration.
	 *
	 * @param aPathName Path name where CSV folder file resides
	 * @param aFileName File name of the CSV file
	 *
	 * @return Complete storage path/file name
	 */
	public String deriveStoragePathFileName(String aPathName, String aFileName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "deriveStoragePathFileName");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String dataPathName = mAppCtx.getProperty(mAppCtx.APP_PROPERTY_DAT_PATH).toString();
		String dataPattern = String.format("%cdata", File.separatorChar);
		String storagePattern = String.format("%c%s", File.separatorChar, aPathName);
		String storagePathName = StringUtils.replace(dataPathName, dataPattern, storagePattern);
		String storagePathFileName = String.format("%s%c%s", storagePathName, File.separatorChar, aFileName);
		appLogger.debug(storagePathFileName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return storagePathFileName;
	}

	private String structureToDataPathName(String aStructure)
	{
		String dataPathName;

		if (StringUtils.isEmpty(aStructure))
			dataPathName = Constants.DS_DATA_FLAT_PATH_NAME;
		else
		{
			if (StringUtils.equals(aStructure, Constants.DS_DATA_STRUCTURE_HIERARCHY_NAME))
				dataPathName = Constants.DS_DATA_HIERARCHY_PATH_NAME;
			else
				dataPathName = Constants.DS_DATA_FLAT_PATH_NAME;
		}

		return dataPathName;
	}

	private boolean isDataHierarchyTitleJSON(GridDS aStorageGridDS, String aStructure, String aDSTitle)
		throws DSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "isDataHierarchyTitleJSON");

		boolean isJSON = false;
		if (StringUtils.equals(aStructure, Constants.DS_DATA_STRUCTURE_HIERARCHY_NAME))
		{
			DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", aDSTitle));
			dsCriteria.add(Constants.DS_STORAGE_DOCUMENT_TITLE, Data.Operator.EQUAL, aDSTitle);
			DataGrid storageGrid = aStorageGridDS.fetch(dsCriteria);
			int rowCount = storageGrid.rowCount();
			if (rowCount == 0)
			{
				DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
				dsCriteriaLogger.writeFull(dsCriteria);
				String errMsg = String.format("Unable to match '%s' with '%s'", Constants.DS_STORAGE_DOCUMENT_TITLE, aDSTitle);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
			}
			DataDoc dataDoc = storageGrid.getRowAsDoc(0);
			String docType = dataDoc.getValueByName("document_type");
			if ((StringUtils.startsWith(docType, "JSON")) || (StringUtils.startsWith(docType, "Schema")))
				isJSON = true;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isJSON;
	}

	/**
	 * Since hierarchy files can have multiple types (JSON, graph), this method determines
	 * if the data set identified by the data source title is JSON related.
	 *
	 * @param aStructure Data structure
	 * @param aDSTitle Data source title
	 *
	 * @return <i>true</i> if the data files are for JSON and <i>false</i> otherwise
	 *
	 * @throws DSException Data source exception
	 * @throws IOException I/O exception
	 */
	public boolean isDataHierarchyJSON(String aStructure, String aDSTitle)
		throws DSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "isDataHierarchyJSON");

		GridDS storageGridDS = loadStorageGridFromFileSystem(aStructure);
		boolean isJSON = isDataHierarchyTitleJSON(storageGridDS, aStructure, aDSTitle);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isJSON;
	}

	private boolean isDataHierarchyAGraph(GridDS aStorageGridDS, String aStructure, String aDSTitle)
		throws DSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "isDataHierarchyAGraph");

		boolean isAGraph = false;
		if (StringUtils.equals(aStructure, Constants.DS_DATA_STRUCTURE_HIERARCHY_NAME))
		{
			DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", aDSTitle));
			dsCriteria.add(Constants.DS_STORAGE_DOCUMENT_TITLE, Data.Operator.EQUAL, aDSTitle);
			DataGrid storageGrid = aStorageGridDS.fetch(dsCriteria);
			int rowCount = storageGrid.rowCount();
			if (rowCount == 0)
			{
				DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
				dsCriteriaLogger.writeFull(dsCriteria);
				String errMsg = String.format("Unable to match '%s' with '%s'", Constants.DS_STORAGE_DOCUMENT_TITLE, aDSTitle);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
			}
			DataDoc dataDoc = storageGrid.getRowAsDoc(0);
			String docType = dataDoc.getValueByName("document_type");
			if ((StringUtils.startsWith(docType, "Graph")) || (StringUtils.startsWith(docType, "Node")) ||
				(StringUtils.startsWith(docType, "Edge")))
				isAGraph = true;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isAGraph;
	}

	/**
	 * Since hierarchy files can have multiple types (JSON, graph), this method determines
	 * if the data set identified by the data source title is a graph.
	 *
	 * @param aStructure Data structure
	 * @param aDSTitle Data source title
	 *
	 * @return <i>true</i> if the data files are for a graph and <i>false</i> otherwise
	 *
	 * @throws DSException Data source exception
	 * @throws IOException I/O exception
	 */
	public boolean isDataHierarchyAGraph(String aStructure, String aDSTitle)
		throws DSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "isDataHierarchyAGraph");

		GridDS storageGridDS = loadStorageGridFromFileSystem(aStructure);
		boolean isAGraph = isDataHierarchyAGraph(storageGridDS, aStructure, aDSTitle);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isAGraph;
	}

	/**
	 * Redis App Studio uses simple storage folders for flat and hierarchical data sets.
	 * This method loads an in-memory grid based on the data structure parameter.
	 *
	 * @param aStructure  Data structure ('flat' or 'hierarchy')
	 *
	 * @return Grid data source instance
	 *
	 * @throws IOException I/O exception processing data file
	 * @throws DSException Data source configuration error
	 */
	private GridDS loadStorageGridFromFileSystem(String aStructure)
		throws IOException, DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadStorageGridFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String dataPathName = structureToDataPathName(aStructure);
		String storageCSVPathFileName = deriveStoragePathFileName(dataPathName, Constants.DS_STORAGE_DETAILS_NAME);
		GridDS storageGridDS = new GridDS(mAppCtx);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		storageGridDS.loadData(storageCSVPathFileName, true);
		storageGridDS.getSchema().getItemByName(Constants.DS_STORAGE_DOCUMENT_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);
		stopWatch.stop();
		DataGrid dataGrid = storageGridDS.getDataGrid();
		appLogger.debug(String.format("'%s': %d columns and %d rows in %d milliseconds.", storageCSVPathFileName,
									  dataGrid.colCount(), dataGrid.rowCount(), stopWatch.getTime()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return storageGridDS;
	}

	/**
	 * Redis App Studio uses a simple CSV file to track its list of supported data and
	 * schema files.  This method will locate those files (by type and title) and load
	 * them into an in-memory grid data source.
	 *
	 * @param aStructure  Data structure ('flat' or 'hierarchy')
	 * @param aDSTitle Data source title
	 *
	 * @return Grid data source instance
	 *
	 * @throws IOException I/O exception processing data file
	 * @throws DSException Data source configuration error
	 * @throws ParserConfigurationException Schema file parsing error
	 * @throws SAXException Schema file parsing error
	 */
	public GridDS getGridDSFromStorageFile(String aStructure, String aDSTitle)
		throws IOException, DSException, ParserConfigurationException, SAXException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getGridDSFromStorageFile");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// "auto-load" assumed - the logic below will now locate and load the flat data files into a GridDS

		GridDS storageGridDS = loadStorageGridFromFileSystem(aStructure);
		GridDS gridDS = getSchemaDataFileFromFileSystem(storageGridDS, aStructure, aDSTitle);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return gridDS;
	}

	/**
	 * This method loads an in-memory grid of release history information.
	 *
	 * @return Grid data source instance
	 *
	 * @throws IOException I/O exception processing data file
	 * @throws DSException Data source configuration error
	 */
	public GridDS loadReleaseGridFromFileSystem()
		throws IOException, DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadReleaseGridFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String dataPathName = structureToDataPathName(Constants.DS_DATA_STRUCTURE_FLAT_NAME);
		String storageCSVPathFileName = deriveStoragePathFileName(dataPathName, Constants.DS_RELEASE_FILE_NAME);
		GridDS releaseGridDS = new GridDS(mAppCtx);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		releaseGridDS.loadData(storageCSVPathFileName, true);
		releaseGridDS.getSchema().getItemByName(Constants.DS_RELEASE_NUMBER_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);
		stopWatch.stop();
		DataGrid dataGrid = releaseGridDS.getDataGrid();
		appLogger.debug(String.format("'%s': %d columns and %d rows in %d milliseconds.", storageCSVPathFileName,
									  dataGrid.colCount(), dataGrid.rowCount(), stopWatch.getTime()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return releaseGridDS;
	}

	public GridDS getRedisCmdsWithDocsFromStream(AppResource anAppResource, boolean aIsReload)
		throws DSException, RedisDSException
	{
		RedisDS redisDS = anAppResource.getRedisDS();
		AppCtx appCtx = redisDS.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "getRedisCmdsWithDocsFromStream");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		GridDS cmdDocGridDS = anAppResource.getStreamCmdDocGridDS();
		if ((cmdDocGridDS == null) || (aIsReload))
		{
			RedisGrid redisGrid = redisDS.createGrid();
			GridDS redisDocGridDS = anAppResource.loadRedisCommandsDS();
			String redisCaptureStreamKeyName = redisDS.getStreamKeyName();
			DataGrid redisCmdDocGrid = redisGrid.loadGridCommandsFromStream(redisCaptureStreamKeyName, Redis.STREAM_START_DEFAULT, Redis.STREAM_FINISH_DEFAULT,
																			Constants.REDIS_STREAM_COMMAND_LENGTH, redisDocGridDS);
			cmdDocGridDS = new GridDS(appCtx, redisCmdDocGrid);
			anAppResource.setStreamCmdDocGridDS(cmdDocGridDS);
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return cmdDocGridDS;
	}

	public JsonDS getJsonDSFromStorageFile(String aStructure, String aDSTitle)
		throws FCException, IOException, ParserConfigurationException, SAXException
	{
		JsonDS jsonDS;
		Logger appLogger = mAppCtx.getLogger(this, "getJsonDSFromStorageFile");

		GridDS storageGridDS = loadStorageGridFromFileSystem(aStructure);
		jsonDS = getJsonFileFromFileSystem(storageGridDS, aStructure, aDSTitle);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return jsonDS;
	}

	public GraphDS getGraphDSFromStorageFile(String aStructure, String aDSTitle)
		throws FCException, IOException, ParserConfigurationException, SAXException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getGraphDSFromStorageFile");

		GridDS storageGridDS = loadStorageGridFromFileSystem(aStructure);
		GraphDS graphDS = getGraphFilesFromFileSystem(storageGridDS, aStructure, aDSTitle);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return graphDS;
	}

	private GridDS getSchemaDataFileFromFileSystem(GridDS aStorageGridDS, String aStructure, String aDSTitle)
		throws IOException, DSException, ParserConfigurationException, SAXException
	{
		DataDoc schemaDoc;
		Logger appLogger = mAppCtx.getLogger(this, "getSchemaDataFileFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", aDSTitle));
		dsCriteria.add(Constants.DS_STORAGE_DOCUMENT_TITLE, Data.Operator.EQUAL, aDSTitle);
		DataGrid storageGrid = aStorageGridDS.fetch(dsCriteria);
		int rowCount = storageGrid.rowCount();
		if (rowCount == 0)
		{
			DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
			dsCriteriaLogger.writeFull(dsCriteria);
			String errMsg = String.format("Unable to match '%s' with '%s'", Constants.DS_STORAGE_DOCUMENT_TITLE, aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		if (rowCount != 2)
		{
			String errMsg = String.format("[%s] Flat data types require both a data and schema file.", aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataDoc dataDoc = storageGrid.getRowAsDoc(0);
		if (dataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE).equals(Constants.DS_STORAGE_TYPE_SCHEMA_DEFINITION))
		{
			schemaDoc = dataDoc;
			dataDoc = storageGrid.getRowAsDoc(1);
		}
		else
			schemaDoc = storageGrid.getRowAsDoc(1);

		String dataPathName = structureToDataPathName(aStructure);
		String dataPathFileName = deriveStoragePathFileName(dataPathName, dataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(dataPathFileName))
			throw new DSException(String.format("[%s] Data file not found.", dataPathFileName));
		String schemaPathFileName = deriveStoragePathFileName(dataPathName, schemaDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(schemaPathFileName))
			throw new DSException(String.format("[%s] Schema file not found.", schemaPathFileName));
		GridDS gridDS = new GridDS(mAppCtx);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		gridDS.loadSchema(schemaPathFileName);
		appLogger.debug(String.format("'%s': schema successfully loaded.", schemaPathFileName));
		gridDS.loadData(dataPathFileName, false);
		stopWatch.stop();
		DataGrid dataGrid = gridDS.getDataGrid();
		appLogger.debug(String.format("'%s': %d columns and %d rows in %d milliseconds.", dataPathFileName,
									  dataGrid.colCount(), dataGrid.rowCount(), stopWatch.getTime()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return gridDS;
	}

	private JsonDS getJsonFileFromFileSystem(GridDS aStorageGridDS, String aStructure, String aDSTitle)
		throws IOException, DSException, ParserConfigurationException, SAXException
	{
		DataDoc jsonDataDoc, jsonSchemaDataDoc;

		Logger appLogger = mAppCtx.getLogger(this, "getJsonFileFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", aDSTitle));
		dsCriteria.add(Constants.DS_STORAGE_DOCUMENT_TITLE, Data.Operator.EQUAL, aDSTitle);
		DataGrid storageGrid = aStorageGridDS.fetch(dsCriteria);
		int rowCount = storageGrid.rowCount();
		if (rowCount == 0)
		{
			DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
			dsCriteriaLogger.writeFull(dsCriteria);
			String errMsg = String.format("Unable to match '%s' with '%s'", Constants.DS_STORAGE_DOCUMENT_TITLE, aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		if (rowCount != 2)
		{
			String errMsg = String.format("[%s] JSON data require data and schema files.", aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}

// The following logic sorts out the order the files may have been defined in the storage file.

		DataDoc dataDoc1 = storageGrid.getRowAsDoc(0);
		DataDoc dataDoc2 = storageGrid.getRowAsDoc(1);
		String docType = dataDoc1.getValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE);
		if (StringUtils.equalsIgnoreCase(docType, Constants.DS_STORAGE_TYPE_JSON_DATA))
		{
			jsonDataDoc = dataDoc1;
			jsonSchemaDataDoc = dataDoc2;
		}
		else
		{
			jsonDataDoc = dataDoc2;
			jsonSchemaDataDoc = dataDoc1;
		}
		String dataPathName = structureToDataPathName(aStructure);
		String jsonDataPathFileName = deriveStoragePathFileName(dataPathName, jsonDataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(jsonDataPathFileName))
			throw new DSException(String.format("[%s] JSON data file not found.", jsonDataPathFileName));
		String jsonSchemaPathFileName = deriveStoragePathFileName(dataPathName, jsonSchemaDataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(jsonSchemaPathFileName))
			throw new DSException(String.format("[%s] JSON schema file not found.", jsonSchemaPathFileName));

// Now we can load the JSON data source and return it.

		JsonDS jsonDS = new JsonDS(mAppCtx);
		jsonDS.setErrorTrackingFlag(false);
		jsonDS.setName(aDSTitle);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		jsonDS.loadSchema(jsonSchemaPathFileName);
		if (jsonDS.loadDataEvaluatePath(jsonDataPathFileName))
		{
			appLogger.debug(String.format("'%s': Loaded %d JSON documents in %d milliseconds.", aDSTitle,
										  jsonDS.getDataGrid().rowCount(), stopWatch.getTime()));
		}
		else
			throw new DSException(String.format("[%s] Unable to load JSON data file.", jsonDataPathFileName));
		stopWatch.stop();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return jsonDS;
	}

	private GraphDS getGraphFilesFromFileSystem(GridDS aStorageGridDS, String aStructure, String aDSTitle)
		throws IOException, FCException, ParserConfigurationException, SAXException
	{
		DataDoc graphDataDoc, vertexSchemaDoc, edgeSchemaDoc;

		Logger appLogger = mAppCtx.getLogger(this, "getGraphFilesFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", aDSTitle));
		dsCriteria.add(Constants.DS_STORAGE_DOCUMENT_TITLE, Data.Operator.EQUAL, aDSTitle);
		DataGrid storageGrid = aStorageGridDS.fetch(dsCriteria);
		int rowCount = storageGrid.rowCount();
		if (rowCount == 0)
		{
			DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
			dsCriteriaLogger.writeFull(dsCriteria);
			String errMsg = String.format("Unable to match '%s' with '%s'", Constants.DS_STORAGE_DOCUMENT_TITLE, aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		if (rowCount != 3)
		{
			String errMsg = String.format("[%s] Graph data require vertex and edge schema files.", aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}

// The following logic sorts out the order the files may have been defined in the storage file.

		graphDataDoc = null;
		vertexSchemaDoc = null;
		edgeSchemaDoc = null;
		DataDoc dataDoc1 = storageGrid.getRowAsDoc(0);
		String docType = dataDoc1.getValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE);
		switch (docType)
		{
			case Constants.DS_STORAGE_TYPE_GRAPH_NODES_SCHEMA:
				vertexSchemaDoc = dataDoc1;
				break;
			case Constants.DS_STORAGE_TYPE_GRAPH_EDGES_SCHEMA:
				edgeSchemaDoc = dataDoc1;
				break;
			default:
				graphDataDoc = dataDoc1;
				break;
		}
		DataDoc dataDoc2 = storageGrid.getRowAsDoc(1);
		docType = dataDoc2.getValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE);
		switch (docType)
		{
			case Constants.DS_STORAGE_TYPE_GRAPH_NODES_SCHEMA:
				vertexSchemaDoc = dataDoc2;
				break;
			case Constants.DS_STORAGE_TYPE_GRAPH_EDGES_SCHEMA:
				edgeSchemaDoc = dataDoc2;
				break;
			default:
				graphDataDoc = dataDoc2;
				break;
		}
		DataDoc dataDoc3 = storageGrid.getRowAsDoc(2);
		docType = dataDoc3.getValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE);
		switch (docType)
		{
			case Constants.DS_STORAGE_TYPE_GRAPH_NODES_SCHEMA:
				vertexSchemaDoc = dataDoc3;
				break;
			case Constants.DS_STORAGE_TYPE_GRAPH_EDGES_SCHEMA:
				edgeSchemaDoc = dataDoc3;
				break;
			default:
				graphDataDoc = dataDoc3;
				break;
		}
		if ((graphDataDoc == null) || (vertexSchemaDoc == null) || (edgeSchemaDoc == null))
		{
			String errMsg = String.format("[%s] Graph file set is incomplete.", aDSTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		String dataPathName = structureToDataPathName(aStructure);
		String graphDataPathFileName = deriveStoragePathFileName(dataPathName, graphDataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(graphDataPathFileName))
			throw new DSException(String.format("[%s] Graph data file not found.", graphDataPathFileName));
		String vertexSchemaPathFileName = deriveStoragePathFileName(dataPathName, vertexSchemaDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(vertexSchemaPathFileName))
			throw new DSException(String.format("[%s] Graph vertex schema file not found.", vertexSchemaPathFileName));
		String edgeSchemaPathFileName = deriveStoragePathFileName(dataPathName, edgeSchemaDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_NAME));
		if (! FilUtl.exists(edgeSchemaPathFileName))
			throw new DSException(String.format("[%s] Graph edge schema file not found.", edgeSchemaPathFileName));

// Now we can load the graph data source and return it.

		GraphDS graphDS = new GraphDS(mAppCtx, aDSTitle);
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		graphDS.loadFromCSV(vertexSchemaPathFileName, edgeSchemaPathFileName, graphDataPathFileName);
		stopWatch.stop();

		DataGraph dataGraph = graphDS.createDataGraph(aDSTitle);

		appLogger.debug(String.format("'%s': Data source has %d nodes and %d edges in %d milliseconds.", aDSTitle,
									  graphDS.getVertexGridDS().getDataGrid().rowCount(),
									  graphDS.getEdgeGridDS().getDataGrid().rowCount(),
									  stopWatch.getTime()));

		appLogger.debug(String.format("'%s': DataGraph has %d nodes and %d edges in %d milliseconds.", dataGraph.getName(),
									  dataGraph.getVertexDocSet().size(), dataGraph.getEdgeSet().size(),
									  stopWatch.getTime()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return graphDS;
	}

	/**
	 * Retrieves a grid data source from the file system based on the
	 * data structure and title members.
	 *
	 * @return Grid data source instance
	 *
	 * @throws DSException Data source exception
	 */
	public GridDS retrieveGridDSFromFileSystem()
		throws DSException
	{
		GridDS gridDS;
		Logger appLogger = mAppCtx.getLogger(this, "retrieveGridDSFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		try
		{
			GridDS storageGridDS = loadStorageGridFromFileSystem(mStructure);
			gridDS = getSchemaDataFileFromFileSystem(storageGridDS, mStructure, mTitle);
		}
		catch (Exception e)
		{
			throw new DSException(e.getMessage());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return gridDS;
	}

	/**
	 * Retrieves a JSON data source from the file system based on the
	 * data structure and title members.
	 *
	 * @return JSON data source instance
	 *
	 * @throws DSException Data source exception
	 */
	public JsonDS retrieveJsonDSFromFileSystem()
		throws DSException
	{
		JsonDS jsonDS;
		Logger appLogger = mAppCtx.getLogger(this, "retrieveJsonDSFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		try
		{
			GridDS storageGridDS = loadStorageGridFromFileSystem(mStructure);
			jsonDS = getJsonFileFromFileSystem(storageGridDS, mStructure, mTitle);
		}
		catch (Exception e)
		{
			throw new DSException(e.getMessage());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return jsonDS;
	}

	/**
	 * Retrieves a graph data source from the file system based on the
	 * data structure and title members.
	 *
	 * @return Graph data source instance
	 *
	 * @throws DSException Data source exception
	 */
	public GraphDS retrieveGraphDSFromFileSystem()
		throws DSException
	{
		GraphDS graphDS;
		Logger appLogger = mAppCtx.getLogger(this, "retrieveGraphDSFromFileSystem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		try
		{
			GridDS storageGridDS = loadStorageGridFromFileSystem(mStructure);
			graphDS = getGraphFilesFromFileSystem(storageGridDS, mStructure, mTitle);
		}
		catch (Exception e)
		{
			throw new DSException(e.getMessage());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return graphDS;
	}

	private Constants.DataType convertDocToDataType(String aFileType)
	{
		Constants.DataType dataType;
		Logger appLogger = mAppCtx.getLogger(this, "convertDocToDataType");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isNotEmpty(aFileType))
		{
			switch (aFileType)
			{
				case "Data Records":
				case "Schema Definition":
					dataType = Constants.DataType.Document;
					break;
				case "JSON Data":
				case "JSON Schema":
					dataType = Constants.DataType.JSON;
					break;
				case "Graph Data":
				case "Node Schema":
				case "Relationship Schema":
					dataType = Constants.DataType.Graph;
					break;
				case "TimeSeries Data":
				case "TimeSeries Definition":
					dataType = Constants.DataType.TimeSeries;
					break;
				default:
					dataType = Constants.DataType.Undefined;
					appLogger.error(String.format("[%s]: Unknown file type.", aFileType));
					break;
			}
		}
		else
			dataType = Constants.DataType.Undefined;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataType;
	}

	public Constants.DataType deriveDataType()
		throws DSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "deriveDataType");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		GridDS storageGridDS = loadStorageGridFromFileSystem(mStructure);
		DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", mTitle));
		dsCriteria.add(Constants.DS_STORAGE_DOCUMENT_TITLE, Data.Operator.EQUAL, mTitle);
		DataGrid storageGrid = storageGridDS.fetch(dsCriteria);
		int rowCount = storageGrid.rowCount();
		if (rowCount == 0)
		{
			DSCriteriaLogger dsCriteriaLogger = new DSCriteriaLogger(appLogger);
			dsCriteriaLogger.writeFull(dsCriteria);
			String errMsg = String.format("Unable to match '%s' with '%s'", Constants.DS_STORAGE_DOCUMENT_TITLE, mTitle);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataDoc dataDoc = storageGrid.getRowAsDoc(0);
		String docType = dataDoc.getValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE);
		Constants.DataType dataType = convertDocToDataType(docType);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataType;
	}

}
