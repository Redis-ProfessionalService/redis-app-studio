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

package com.redis.ds.ds_redis.graph;

import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DGCriteria;
import com.redis.foundation.ds.DGCriterion;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.xml.sax.SAXException;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.graph.Header;
import redis.clients.jedis.graph.Record;
import redis.clients.jedis.graph.ResultSet;
import redis.clients.jedis.graph.Statistics;
import redis.clients.jedis.graph.entities.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * The RedisGraphs class is responsible for accessing the RedisGraph
 * module commands via the Jedis programmatic library.  It designed to
 * simplify the use of core Foundation class objects like items,
 * documents, grids and graphs.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * RedisGraph is the first queryable Property Graph database to use sparse matrices
 * to represent the adjacency matrix in graphs and linear algebra to query the graph.
 *
 * Data Model Details
 *         Node Label = DataDoc.Name
 *  Relationship Type = DataGraphEdge.Name
 *
 * All DataDoc instances must have Data.FEATURE_IS_PRIMARY enabled with one of their items.
 *
 * @see <a href="https://oss.redislabs.com/redisgraph">OSS RedisGraph</a>
 * @see <a href="https://github.com/RedisGraph/JRedisGraph">Java RedisGraph</a>
 * @see <a href="https://github.com/opencypher/openCypher">The Cypher Property Graph Query Language</a>
 * @see <a href="https://github.com/redis/jedis">Java Redis</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisGraphs
{
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private DataDoc mEdgeSchemaDoc;
	private final RedisKey mRedisKey;
	private DataDoc mVertexSchemaDoc;
	private final UnifiedJedis mCmdConnection;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the graph objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisGraphs(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mRedisKey = mRedisDS.getRedisKey();
		mRedisDS.setEncryptionOption(Redis.Encryption.None);
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
	}

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the graph objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 * @param aVertexSchemaDoc Vertex schema definition
	 * @param anEdgeSchemaDoc Edge schema definition
	 */
	public RedisGraphs(RedisDS aRedisDS, DataDoc aVertexSchemaDoc, DataDoc anEdgeSchemaDoc)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mEdgeSchemaDoc = anEdgeSchemaDoc;
		mRedisKey = mRedisDS.getRedisKey();
		mVertexSchemaDoc = aVertexSchemaDoc;
		mRedisDS.setEncryptionOption(Redis.Encryption.None);
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
	}

	/**
	 * Assigns the vertex schema document definition.
	 *
	 * @param aVertexSchemaDoc Vertex schema definition
	 */
	public void setVertexSchema(DataDoc aVertexSchemaDoc)
	{
		mVertexSchemaDoc = aVertexSchemaDoc;
	}

	/**
	 * Returns the internally managed graph vertex schema data doc instance.
	 *
	 * @return Vertex schema data document instance
	 */
	public DataDoc getVertexSchema()
	{
		return mVertexSchemaDoc;
	}

	/**
	 * Assigns the edge schema document definition.
	 *
	 * @param anEdgeSchemaDoc Edge schema definition
	 */
	public void setEdgeSchema(DataDoc anEdgeSchemaDoc)
	{
		mEdgeSchemaDoc = anEdgeSchemaDoc;
	}

	/**
	 * Returns the internally managed graph edge schema data doc instance.
	 *
	 * @return Edge schema data document instance
	 */
	public DataDoc getEdgeSchema()
	{
		return mEdgeSchemaDoc;
	}

	/**
	 * Create a key name (using the Redis App Studio standard format) based on the name
	 * of the graph.
	 *
	 * @param aName Name of the graph
	 *
	 * @return String representing the Redis database key name
	 */
	public String graphKeyName(String aName)
	{
		return mRedisKey.moduleGraph().redisGraph().dataName(aName).name();
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
				case Data.FEATURE_IS_SEARCH:
				case Data.FEATURE_IS_PRIMARY:
				case Data.FEATURE_IS_REQUIRED:
				case Data.FEATURE_IS_VISIBLE:
				case Redis.FEATURE_IS_PROPERTY_SEARCH:
				case Redis.FEATURE_IS_FULLTEXT_SEARCH:
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
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Redis.FEATURE_IS_PROPERTY_SEARCH).title("Is Property Search").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Redis.FEATURE_IS_FULLTEXT_SEARCH).title("Is Full Text Search").build());
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
							{
								featureType = Data.Type.Text;
								if (featureKey.equals(Data.FEATURE_IS_SEARCH))
									featureKey = Redis.FEATURE_IS_FULLTEXT_SEARCH;
							}
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

	private boolean isDataDocValid(DataDoc aDataDoc)
	{
		if (aDataDoc != null)
		{
			Optional<DataItem> optPrimaryItem = aDataDoc.getPrimaryKeyItemOptional();
			return optPrimaryItem.isPresent();
		}
		else
			return false;
	}

	private boolean isDataEdgeValid(DataGraphEdge aDataGraphEdge)
	{
		if (aDataGraphEdge != null)
			return isDataDocValid(aDataGraphEdge.getDoc());
		else
			return false;
	}

	private void ensurePreconditions()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "ensurePreconditions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void ensurePreconditions(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "ensurePreconditions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (! isDataDocValid(aDataDoc))
			throw new RedisDSException(String.format("[%s] Data document is missing a primary item.", aDataDoc.getName()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void ensurePreconditions(DataDoc aSrcNode, DataDoc aDstNode, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "ensurePreconditions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		if (! isDataDocValid(aSrcNode))
			throw new RedisDSException(String.format("[%s] Source data document is missing a primary item.", aDataDoc.getName()));
		if (! isDataDocValid(aDstNode))
			throw new RedisDSException(String.format("[%s] Destination data document is missing a primary item.", aDataDoc.getName()));
		if (! isDataDocValid(aDataDoc))
			throw new RedisDSException(String.format("[%s] Relationship data document is missing a primary item.", aDataDoc.getName()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Generates and returns a unique key name for the graph index
	 * that can be used to store/retrieve the definition in Redis.
	 *
	 * @param anIsNode If <i>true</i>, then this key will be for
	 *                 a node schema.
	 *
	 * @return Search schema key name
	 */
	public String getSchemaKeyName(boolean anIsNode)
	{
		String schemaName;

		if (anIsNode)
			schemaName = "Node";
		else
			schemaName = "Edge";
		return mRedisKey.moduleGraph().redisGraphSchema().dataName(schemaName).name();
	}

	/**
	 * Stores the graph schema definitions in the Redis database.  This
	 * schema definition is one that could be used by a parent application
	 * to describe the current search schema.
	 *
	 * @param aGraphSchemaDoc Graph schema data document instance
	 * @param anIsNode If <i>true</i>, then this key will be for
	 *                 a node schema.
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void saveSchemaDefinition(DataDoc aGraphSchemaDoc, boolean anIsNode)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveSchemaDefinition");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String schemaKeyName = getSchemaKeyName(anIsNode);
		DataDocXML dataDocXML = new DataDocXML(aGraphSchemaDoc);
		dataDocXML.setHeaderSaveFlag(true);
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
		String xmlSchemaString = stringWriter.toString().replaceAll("\\n", StringUtils.EMPTY);
		mRedisDS.createCore().set(schemaKeyName, xmlSchemaString);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the schema definition from the Redis DB into a data document
	 * instance.  If the load fails or the key does not exist, then the
	 * optional data document will not be present.
	 *
	 * @param anIsNode If <i>true</i>, then this key will be for
	 *                 a node schema.
	 *
	 * @return Optional data document instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public Optional<DataDoc> loadSchema(boolean anIsNode)
		throws RedisDSException
	{
		DataDoc schemaDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String schemaKeyName = getSchemaKeyName(anIsNode);
		RedisCore redisCore = mRedisDS.createCore();
		if (redisCore.exists(schemaKeyName))
		{
			String schemaString = redisCore.get(schemaKeyName);
			DataDocXML dataDocXML = new DataDocXML();
			try
			{
				InputStream inputStream = IOUtils.toInputStream(schemaString, StrUtl.CHARSET_UTF_8);
				dataDocXML.load(inputStream);
			}
			catch (ParserConfigurationException | SAXException | IOException e)
			{
				throw new RedisDSException(e.getMessage());
			}
			schemaDoc = dataDocXML.getDataDoc();
		}
		else
			schemaDoc = null;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(schemaDoc);
	}

	/**
	 * Identifies if the schema definition for the graph has been
	 * stored in the Redis DB.
	 *
	 * @param anIsNode If <i>true</i>, then this key will be for
	 *                 a node schema.
	 *
	 * @return <i>true</i> if it exists and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public boolean schemaExists(boolean anIsNode)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "schemaExists");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String schemaKeyName = getSchemaKeyName(anIsNode);
		boolean schemaExists = mRedisDS.createCore().exists(schemaKeyName);
		mRedisDS.saveCommand(appLogger, String.format("EXISTS %s", mRedisDS.escapeKey(schemaKeyName)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return schemaExists;
	}

	private String escapeCypherValue(String aValue)
	{
		String value = StringUtils.remove(aValue, "\\'");
		return StringUtils.remove(value, StrUtl.CHAR_DBLQUOTE);
	}

	/**
	 * Stores the data document as a node in the graph database identified
	 * by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aDataDoc Node data document instance
	 *
	 * @return <i>true</i> if store operation succeeded and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Graph database exception
	 */
	public boolean addNode(DataGraph aDataGraph, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "addNode");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aDataDoc);
		StringBuilder sbCypher = new StringBuilder();
		String graphName = graphKeyName(aDataGraph.getName());
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		redisCypher.addNode(sbCypher, aDataDoc);
		String cypherCommand = String.format("CREATE %s", sbCypher.toString());
		String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
		ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
		Statistics rgStatistics = resultSet.getStatistics();
		mRedisDS.saveCommand(appLogger, redisCommand);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rgStatistics.nodesCreated() == 1;
	}

	/**
	 * Stores the data document as a relationship between the source and destination
	 * data documents in the graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aSrcNode Source node of the relationship
	 * @param aDstNode Destination node of the relationship
	 * @param aRelDoc Relationship data document instance
	 *
	 * @return <i>true</i> if store operation succeeded and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Graph database exception
	 */
	public boolean addRelationship(DataGraph aDataGraph, DataDoc aSrcNode, DataDoc aDstNode, DataDoc aRelDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "addRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aRelDoc);
		if (! isDataDocValid(aSrcNode))
			throw new RedisDSException(String.format("[%s] Source node is missing a primary item.", aSrcNode.getName()));
		if (! isDataDocValid(aDstNode))
			throw new RedisDSException(String.format("[%s] Destination node is missing a primary item.", aDstNode.getName()));

		StringBuilder sbCypher = new StringBuilder();
		String graphName = graphKeyName(aDataGraph.getName());
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		redisCypher.addRelationship(sbCypher, aSrcNode, aDstNode, aRelDoc);
		String cypherCommand = sbCypher.toString();
		String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
		ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
		Statistics rgStatistics = resultSet.getStatistics();
		mRedisDS.saveCommand(appLogger, redisCommand);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rgStatistics.relationshipsCreated() == 1;
	}

	/**
	 * Creates a default relationship and stores it between the source and destination
	 * data documents in the graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aSrcNode Source node of the relationship
	 * @param aDstNode Destination node of the relationship
	 *
	 * @return <i>true</i> if store operation succeeded and <i>false</i> otherwise
	 *
	 * @throws FCException Graph database exception
	 */
	public boolean addRelationship(DataGraph aDataGraph, DataDoc aSrcNode, DataDoc aDstNode)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "addRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String relationshipName = String.format("%s->%s Relationship", aSrcNode.getName(), aDstNode.getName());
		DataDoc dataDoc = new DataDoc(relationshipName);
		dataDoc.add(new DataItem.Builder().name("id").title("Id").value(relationshipName).isPrimary(true).build());
		boolean isOK = addRelationship(aDataGraph, aSrcNode, aDstNode, dataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Stores all the nodes and relationships captured in the data graph
	 * instance into the database.
	 *
	 * Note: The method assumes that Cypher strings can grow to the 512 MB
	 * size limit of a Redis String data structure, so no size limit checks
	 * are enforced.
	 *
	 * @param aDataGraph Data graph instance
	 *
	 * @throws FCException Graph database exception
	 */
	public void add(DataGraph aDataGraph)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();

// First, we will add the vertex data documents as nodes to the Redis graph.

		Set<DataDoc> vertexDataDocSet = aDataGraph.getVertexDocSet();
		if (vertexDataDocSet.size() > 0)
		{
			int itemCount = 0;
			StringBuilder sbCypher = new StringBuilder();
			String graphName = graphKeyName(aDataGraph.getName());
			RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
			for (DataDoc dataDoc : vertexDataDocSet)
			{
				if (! isDataDocValid(dataDoc))
					throw new RedisDSException(String.format("[%s] Vertex data document is missing a primary item.", dataDoc.getName()));
				if (itemCount > 0)
					sbCypher.append(StrUtl.CHAR_COMMA);
				redisCypher.addNode(sbCypher, dataDoc);
				itemCount++;
			}
			String cypherCommand = String.format("CREATE %s", sbCypher.toString());
			String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			mRedisDS.saveCommand(appLogger, redisCommand);
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			Statistics rgStatistics = resultSet.getStatistics();
			int nodesCreated = rgStatistics.nodesCreated();
			if (nodesCreated != vertexDataDocSet.size())
				throw new RedisDSException(String.format("%s: Redis nodes created (%d) does not match graph node count (%d).",
													  aDataGraph.getName(), nodesCreated, vertexDataDocSet.size()));

// Next, we will add our edges as relationships to Redis graph.

			Set<DataGraphEdge> dataGraphEdgeSet = aDataGraph.getEdgeSet();
			if (dataGraphEdgeSet.size() > 0)
			{
				int relationshipsCreated = 0;
				for (DataGraphEdge dge : dataGraphEdgeSet)
				{
					if (! isDataEdgeValid(dge))
						throw new RedisDSException(String.format("[%s] Edge data document is missing a primary item.", dge.getName()));
					sbCypher.setLength(0);
					redisCypher.addRelationship(sbCypher, dge);
					cypherCommand = sbCypher.toString();
					redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
					resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
					mRedisDS.saveCommand(appLogger, redisCommand);
					rgStatistics = resultSet.getStatistics();
					relationshipsCreated += rgStatistics.relationshipsCreated();
				}
				if (relationshipsCreated != dataGraphEdgeSet.size())
					throw new RedisDSException(String.format("%s: Redis relationships created (%d) does not match graph relationship count (%d).",
														  aDataGraph.getName(), relationshipsCreated, dataGraphEdgeSet.size()));
			}
		}

// Finally, if schema documents were provided, then we will persist them and create property and full text indexes.

		if (mVertexSchemaDoc != null)
			saveSchemaDefinition(mVertexSchemaDoc, true);
		if (mEdgeSchemaDoc != null)
			saveSchemaDefinition(mEdgeSchemaDoc, false);

		if (mVertexSchemaDoc != null)
		{
			List<DataDoc> dataDocList = expandUnifiedNodeSchema(mVertexSchemaDoc);
			for (DataDoc dataDoc : dataDocList)
			{
				createPropertyIndex(aDataGraph, dataDoc);
				createFullTextIndex(aDataGraph, dataDoc);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a property index for each data item with the RedisGraphModule.FEATURE_IS_PROPERTY_SEARCH
	 * feature enabled.  Property indexes are limited to nodes.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aSchemaDoc Schema definition data document instance
	 *
	 * @return Count of indices created
	 *
	 * @throws RedisDSException RedisGraph data source exception
	 */
	public int createPropertyIndex(DataGraph aDataGraph, DataDoc aSchemaDoc)
		throws RedisDSException
	{
		String cypherCommand, redisCommand;
		Logger appLogger = mAppCtx.getLogger(this, "createPropertyIndex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aSchemaDoc);
		int indicesAdded = 0;
		String graphName = graphKeyName(aDataGraph.getName());
		if (aSchemaDoc.getItemByFeatureEnabledOptional(Redis.FEATURE_IS_PROPERTY_SEARCH).isPresent())
		{
			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				if (dataItem.isFeatureAssigned(Redis.FEATURE_IS_PROPERTY_SEARCH))
				{
					cypherCommand = String.format("CREATE INDEX ON :%s(%s)", aSchemaDoc.getName(), dataItem.getName());
					ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
					Statistics rgStatistics = resultSet.getStatistics();
					indicesAdded = rgStatistics.indicesCreated();
					redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
					mRedisDS.saveCommand(appLogger, redisCommand);
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return indicesAdded;
	}

	/**
	 * Drops the property indexes for each data item with the RedisGraphModule.FEATURE_IS_PROPERTY_SEARCH
	 * feature enabled.  Full text indexes are limited to nodes.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aSchemaDoc Schema definition data document instance
	 *
	 * @return Count of indices dropped
	 *
	 * @throws RedisDSException Graph database exception
	 */
	public int dropPropertyIndex(DataGraph aDataGraph, DataDoc aSchemaDoc)
		throws RedisDSException
	{
		String cypherCommand, redisCommand;
		Logger appLogger = mAppCtx.getLogger(this, "dropPropertyIndex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aSchemaDoc);
		int indicesDropped = 0;
		String graphName = graphKeyName(aDataGraph.getName());
		if (aSchemaDoc.getItemByFeatureEnabledOptional(Redis.FEATURE_IS_PROPERTY_SEARCH).isPresent())
		{
			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				if (dataItem.isFeatureAssigned(Redis.FEATURE_IS_PROPERTY_SEARCH))
				{
					cypherCommand = String.format("DROP INDEX ON :%s(%s)", aSchemaDoc.getName(), dataItem.getName());
					ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
					Statistics rgStatistics = resultSet.getStatistics();
					indicesDropped = rgStatistics.indicesDeleted();
					redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
					mRedisDS.saveCommand(appLogger, redisCommand);
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return indicesDropped;
	}

	/**
	 * Creates a full text index for each data item with the RedisGraphModule.FEATURE_IS_FULLTEXT_SEARCH
	 * feature enabled.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aSchemaDoc Schema definition data document instance
	 *
	 * @return Count of indices created
	 *
	 * @throws RedisDSException RedisGraph data source exception
	 */
	public int createFullTextIndex(DataGraph aDataGraph, DataDoc aSchemaDoc)
		throws RedisDSException
	{
		String cypherCommand, redisCommand;
		Logger appLogger = mAppCtx.getLogger(this, "createFullTextIndex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aSchemaDoc);
		int indicesAdded = 0;
		if (aSchemaDoc.getItemByFeatureEnabledOptional(Redis.FEATURE_IS_FULLTEXT_SEARCH).isPresent())
		{
			String graphName = graphKeyName(aDataGraph.getName());
			StringBuilder sbCypher = new StringBuilder(String.format("CALL db.idx.fulltext.createNodeIndex('%s'", aSchemaDoc.getName()));
			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				if (dataItem.isFeatureAssigned(Redis.FEATURE_IS_FULLTEXT_SEARCH))
					sbCypher.append(String.format(",'%s'", dataItem.getName()));
			}
			sbCypher.append(')');
			cypherCommand = sbCypher.toString();
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			Statistics rgStatistics = resultSet.getStatistics();
			indicesAdded = rgStatistics.indicesCreated();
			redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			mRedisDS.saveCommand(appLogger, redisCommand);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return indicesAdded;
	}

	private List<DataDoc> expandUnifiedNodeSchema(DataDoc aSchemaDoc)
	{
		List<DataDoc> dataDocList = new ArrayList<>();
		Optional<DataItem> optDataItem = aSchemaDoc.getItemByNameOptional(Data.GRAPH_VERTEX_LABEL_NAME);
		if (optDataItem.isPresent())
		{
			DataItem diVertexLabel = optDataItem.get();
			DataRange dataRange = diVertexLabel.getRange();
			List<String> labelNames = dataRange.getItems();
			for (String labelName : labelNames)
			{
				String itemPrefix = labelName.toLowerCase() + "_";
				DataDoc dataDoc = new DataDoc(labelName);
				for (DataItem dataItem : aSchemaDoc.getItems())
				{
					if ((dataItem.getName().startsWith(itemPrefix)) ||
						(dataItem.getName().startsWith(Data.GRAPH_COMMON_PREFIX)))
						dataDoc.add(new DataItem(dataItem));
				}
				if (dataDoc.count() > 0)
					dataDocList.add(dataDoc);
			}
		}

		return dataDocList;
	}

	@SuppressWarnings("rawtypes")
	private void addRecordNodeToDataGraph(DataGraph aDataGraph, Node aNode)
		throws FCException
	{
		DataItem dataItem;
		DataDoc vertexDoc;
		Property nodeProperty;
		Set<String> propertyNames;
		Logger appLogger = mAppCtx.getLogger(this, "addRecordNodeToDataGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		StringBuilder labelName = new StringBuilder();
		labelName.setLength(0);
		int labelCount = aNode.getNumberOfLabels();
		for (int i = 0; i < labelCount; i++)
		{
			if (i > 0)
				labelName.append(StrUtl.CHAR_PIPE);
			labelName.append(aNode.getLabel(i));
		}
		propertyNames = aNode.getEntityPropertyNames();
		if (mVertexSchemaDoc != null)
		{
			vertexDoc = new DataDoc(mVertexSchemaDoc);
			vertexDoc.setName(labelName.toString());
			for (String propertyName : propertyNames)
			{
				nodeProperty = aNode.getProperty(propertyName);
				vertexDoc.setValueByName(propertyName, nodeProperty.getValue().toString());
			}
		}
		else
		{
			vertexDoc = new DataDoc(labelName.toString());
			for (String propertyName : propertyNames)
			{
				nodeProperty = aNode.getProperty(propertyName);
				dataItem = new DataItem(propertyName, nodeProperty.getValue());
				vertexDoc.add(dataItem);
			}
		}
		vertexDoc.addFeature(Data.FEATURE_GRAPH_ID, aNode.getId());

		aDataGraph.addVertex(vertexDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	@SuppressWarnings("rawtypes")
	private void addRecordEdgeToList(ArrayList<DataDoc> anEdgeDataDocList, Edge anEdge)
	{
		DataDoc edgeDoc;
		DataItem dataItem;
		Property edgeProperty;
		Set<String> propertyNames;
		String srcVertexId, dstVertexId;
		Logger appLogger = mAppCtx.getLogger(this, "addRecordEdgeToList");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		propertyNames = anEdge.getEntityPropertyNames();
		if (mEdgeSchemaDoc != null)
		{
			edgeDoc = new DataDoc(mEdgeSchemaDoc);
			edgeDoc.setName(anEdge.getRelationshipType());
			for (String propertyName : propertyNames)
			{
				edgeProperty = anEdge.getProperty(propertyName);
				edgeDoc.setValueByName(propertyName, edgeProperty.getValue().toString());
			}
		}
		else
		{
			edgeDoc = new DataDoc(anEdge.getRelationshipType());
			for (String propertyName : propertyNames)
			{
				edgeProperty = anEdge.getProperty(propertyName);
				dataItem = new DataItem(propertyName, edgeProperty.getValue());
				edgeDoc.add(dataItem);
			}
		}
		edgeDoc.addFeature(Data.FEATURE_GRAPH_ID, anEdge.getId());
		srcVertexId = Long.toString(anEdge.getSource());
		edgeDoc.addFeature(Data.FEATURE_GRAPH_SRC_ID, srcVertexId);
		dstVertexId = Long.toString(anEdge.getDestination());
		edgeDoc.addFeature(Data.FEATURE_GRAPH_DST_ID, dstVertexId);
		anEdgeDataDocList.add(edgeDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	// This logic works best when vertex and edge schemas are provided
	private void graphResultToDataGraph(DataGraph aDataGraph, ResultSet aResultSet)
		throws FCException
	{
		Node graphNode;
		Edge graphEdge;
		Object graphObject;
		Record redisRecord;
		StringBuilder labelName;
		ArrayList<Node> nodeList;
		ArrayList<Edge> edgeList;
		Optional<DataItem> optDataItem;
		Iterator<Record> recordIterator;
		DataDoc srcVertexDoc, dstVertexDoc;
		DataItem srcPrimaryKey, dstPrimaryKey;
		String schemaName, srcVertexId, dstVertexId, edgeTitle;
		Logger appLogger = mAppCtx.getLogger(this, "graphResultToDataGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Statistics rgStatistics = aResultSet.getStatistics();
		appLogger.debug(rgStatistics.toString());

// Process the result set in multiple passes: 1) add all nodes; 2) add edges to a list; 3) add all edges

		int resultSetSize = aResultSet.size();
		if (resultSetSize > 0)
		{
			labelName = new StringBuilder();
			recordIterator = aResultSet.iterator();
			Header redisHeader = aResultSet.getHeader();
			ArrayList<DataDoc> edgeDataDocList = new ArrayList<>();
			List<String> schemaNames = redisHeader.getSchemaNames();
			List<ResultSet.ColumnType> schemaTypes = redisHeader.getSchemaTypes();
			for (int resultOffset = 0; resultOffset < resultSetSize; resultOffset++)
			{
				int stOffset = 0;
				redisRecord = recordIterator.next();
				for (ResultSet.ColumnType schemaType : schemaTypes)
				{
					schemaName = schemaNames.get(stOffset);
					if (schemaType == ResultSet.ColumnType.NODE)
					{
						appLogger.debug(String.format("[%d/%d] Adding graph node.", resultOffset, resultSetSize));
						labelName.setLength(0);
						graphNode = redisRecord.getValue(schemaName);
						addRecordNodeToDataGraph(aDataGraph, graphNode);
					}
					else if (schemaType == ResultSet.ColumnType.RELATION)
					{
						appLogger.debug(String.format("[%d/%d] Adding graph relation.", resultOffset, resultSetSize));
						graphEdge = redisRecord.getValue(schemaName);
						addRecordEdgeToList(edgeDataDocList, graphEdge);
					}
					else if (schemaType == ResultSet.ColumnType.SCALAR)
					{
						graphObject = redisRecord.getValue(schemaName);
						if (graphObject instanceof Node)
						{
							graphNode = redisRecord.getValue(schemaName);
							addRecordNodeToDataGraph(aDataGraph, graphNode);
							appLogger.debug(String.format("[%d/%d] Adding graph object node: %s", resultOffset, resultSetSize, graphNode.toString()));
						}
						else if (graphObject instanceof Edge)
						{
							graphEdge = redisRecord.getValue(schemaName);
							addRecordEdgeToList(edgeDataDocList, graphEdge);
							appLogger.debug(String.format("[%d/%d] Adding graph object edge: %s", resultOffset, resultSetSize, graphEdge.toString()));
						}
						else if (graphObject instanceof Path)
						{
							appLogger.debug(String.format("[%d/%d] Adding graph object - path.", resultOffset, resultSetSize));
							Path graphPath = redisRecord.getValue(schemaName);
							List<Node> gpNodes = graphPath.getNodes();
							List<Edge> gpEdges = graphPath.getEdges();
							for (Node gpNode : gpNodes)
								addRecordNodeToDataGraph(aDataGraph, gpNode);
							for (Edge gpEdge : gpEdges)
								addRecordEdgeToList(edgeDataDocList, gpEdge);
						}
						else if ((StringUtils.startsWith(schemaName, "nodes(")) && (graphObject instanceof ArrayList))
						{
							appLogger.debug(String.format("[%d/%d] Adding graph object - nodes().", resultOffset, resultSetSize));
							nodeList = redisRecord.getValue(schemaName);
							for (Node node : nodeList)
								addRecordNodeToDataGraph(aDataGraph, node);
						}
						else if ((StringUtils.startsWith(schemaName, "relationships(")) && (graphObject instanceof ArrayList))
						{
							appLogger.debug(String.format("[%d/%d] Adding graph object - relationships().", resultOffset, resultSetSize));
							edgeList = redisRecord.getValue(schemaName);
							for (Edge edge : edgeList)
								addRecordEdgeToList(edgeDataDocList, edge);
						}
						else
							throw new RedisDSException(String.format("[%s] Unable to process schema name '%s' for type '%s'",
																	 aDataGraph.getName(), schemaName, schemaType.name()));
					}
					else
						throw new RedisDSException(String.format("[%s] Unable to process schema type '%s' for name '%s'",
																 aDataGraph.getName(), schemaType.name(), schemaName));
					stOffset++;
				}
			}

/* Uniquely add edges to graph now that all nodes have been processed in the result set.
   A cypher query in RedisGraph will produce a list of duplicate edges in the result
   set (e.g. src->dst and dst->src) - we only need one of those edges for our in-memory
   graph data structure. */

			for (DataDoc ed : edgeDataDocList)
			{
				srcVertexId = ed.getFeature(Data.FEATURE_GRAPH_SRC_ID);
				srcVertexDoc = aDataGraph.getVertexDocByFeatureValue(Data.FEATURE_GRAPH_ID, srcVertexId);
				if (srcVertexDoc == null)
					throw new RedisDSException(String.format("[%s] Unable to locate source vertex in data graph by id '%s'",
														  aDataGraph.getName(), srcVertexId));
				dstVertexId = ed.getFeature(Data.FEATURE_GRAPH_DST_ID);
				dstVertexDoc = aDataGraph.getVertexDocByFeatureValue(Data.FEATURE_GRAPH_ID, dstVertexId);
				if (dstVertexDoc == null)
					throw new RedisDSException(String.format("[%s] Unable to locate destination vertex in data graph by id '%s'",
														  aDataGraph.getName(), dstVertexDoc));
				edgeTitle = ed.getTitle();
				appLogger.debug(String.format("Relationship: (%s:%s)-[%s:%s]->(%s:%s)", srcVertexDoc.getName(), srcVertexId,
											  ed.getName(), edgeTitle, dstVertexDoc.getName(), dstVertexId));

// If there are primary keys assigned, then update the source and destination ids for visual path matching.

				optDataItem = srcVertexDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
				srcPrimaryKey = optDataItem.orElse(null);
				optDataItem = dstVertexDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
				dstPrimaryKey = optDataItem.orElse(null);
				if ((srcPrimaryKey != null) && (dstPrimaryKey != null))
				{
					ed.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, srcPrimaryKey.getValue());
					ed.setValueByName(Data.GRAPH_DST_VERTEX_ID_NAME, dstPrimaryKey.getValue());
				}
				aDataGraph.addEdgeUnique(srcVertexDoc, dstVertexDoc, ed);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Queries the graph database for a graph path based on the data
	 * graph criteria provided.  If you intend to visualize the graph
	 * after the query, then you must ensure you assign node and
	 * relationship schemas prior to executing this method - they are
	 * used to identify data items with primary ids.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return A data graph instance populated with matching nodes and relationships
	 *
	 * @throws FCException Graph database exception
	 */
	public DataGraph queryPattern(DGCriteria aDGCriteria)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "queryPattern");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String graphName = graphKeyName(aDGCriteria.getName());
		DataGraph dataGraph = new DataGraph(aDGCriteria.getName(), Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);
		StringBuilder sbCypher = new StringBuilder();
		RedisCypher redisCypher = new RedisCypher(mAppCtx, dataGraph);
		redisCypher.matchCriteriaWithReturn(sbCypher, aDGCriteria);
		if (sbCypher.length() > 0)
		{
			String cypherCommand = sbCypher.toString();
			String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			mRedisDS.saveCommand(appLogger, redisCommand);
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			graphResultToDataGraph(dataGraph, resultSet);
			dataGraph.addFeature(Redis.FEATURE_CYPHER_QUERY, cypherCommand);
			dataGraph.addFeature(Redis.FEATURE_REDISGRAPH_QUERY, redisCommand);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGraph;
	}

	/**
	 * Queries the graph database for a single node based on the data
	 * graph criteria provided.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Optional data document instance representing a node
	 *
	 * @throws FCException Graph database exception
	 */
	public Optional<DataDoc> queryNode(DGCriteria aDGCriteria)
		throws FCException
	{
		Optional<DataDoc> optDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "queryNode");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		DataGraph dataGraph = queryPattern(aDGCriteria);
		Set<DataDoc> dataDocSet = dataGraph.getVertexDocSet();
		optDataDoc = dataDocSet.stream().findFirst();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	/**
	 * Queries the graph database for a graph path based on the cypher
	 * statement provided.
	 *
	 * @param aGraphDBName Name of the graph database
	 * @param aCypherStatement Cypher "MATCH ... RETURN" statement
	 *
	 * @return A data graph instance populated with matching nodes and relationships
	 *
	 * @throws FCException Graph database exception
	 */
	public DataGraph queryCypher(String aGraphDBName, String aCypherStatement)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "queryPattern");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isEmpty(aGraphDBName)) || (StringUtils.isEmpty(aCypherStatement)))
			throw new RedisDSException("Undefined graph database name and/or cypher statement.");

		ensurePreconditions();
		String graphName = graphKeyName(aGraphDBName);
		DataGraph dataGraph = new DataGraph(aGraphDBName, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);
		String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(aGraphDBName), aCypherStatement);
		ResultSet resultSet = mCmdConnection.graphQuery(graphName, aCypherStatement);
		graphResultToDataGraph(dataGraph, resultSet);
		mRedisDS.saveCommand(appLogger, redisCommand);
		dataGraph.addFeature(Redis.FEATURE_CYPHER_QUERY, aCypherStatement);
		dataGraph.addFeature(Redis.FEATURE_REDISGRAPH_QUERY, redisCommand);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGraph;
	}

	/**
	 * Performs a fulltext query against the nodes of the graph database based on
	 * the RedisGraphModule.FEATURE_IS_FULLTEXT_SEARCH being enabled in the
	 * vertex schema data document instance and the node label identified.
	 *
	 * @param aName Name of the graph database
	 * @param aNodeLabel Label of node (used for index id)
	 * @param aQueryString Query string
	 *
	 * @return A data graph instance populated with mating nodes
	 *
	 * @throws FCException Graph database exception
	 */
	public DataGraph queryNodeText(String aName, String aNodeLabel, String aQueryString)
			throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "queryNodeText");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String graphName = graphKeyName(aName);
		DataGraph dataGraph = new DataGraph(aName, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);
		if ((StringUtils.isNotEmpty(aNodeLabel)) && (StringUtils.isNotEmpty(aQueryString)))
		{
			String cypherCommand = String.format("CALL db.idx.fulltext.queryNodes('%s','%s') YIELD node RETURN node", aNodeLabel, aQueryString);
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			graphResultToDataGraph(dataGraph, resultSet);
			String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			mRedisDS.saveCommand(appLogger, redisCommand);
			dataGraph.addFeature(Redis.FEATURE_CYPHER_QUERY, cypherCommand);
			dataGraph.addFeature(Redis.FEATURE_REDISGRAPH_QUERY, redisCommand);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGraph;
	}

	/**
	 * Performs a fulltext query against the nodes of the graph database based on
	 * the RedisGraphModule.FEATURE_IS_FULLTEXT_SEARCH being enabled in the
	 * vertex schema data document instance and all node labels.
	 *
	 * @param aName Name of the graph database
	 * @param aVertexSchemaDoc Vertex schema data document instance
	 * @param aQueryString Query string
	 * @param anOffset Starting offset into the matching content rows
	 * @param aLimit Limit on the total number of rows to extract from
	 *               the content source during this fetch operation.
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the content source (based on the offset and limit values).
	 *
	 * @throws FCException Redis Labs exception
	 */
	public DataGrid queryNodeText(String aName, DataDoc aVertexSchemaDoc, String aQueryString,
								  int anOffset, int aLimit)
			throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "queryNodeText");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		DataGrid dataGrid = new DataGrid(aName, aVertexSchemaDoc);
		DataItem dataItem = aVertexSchemaDoc.getItemByName(Data.GRAPH_VERTEX_LABEL_NAME);
		if (dataItem != null)
		{
			DataRange dataRange = dataItem.getRange();
			if (dataRange != null)
			{
				GraphDS graphDS;
				DataGraph dataGraph;

				for (String vertexLabel : dataRange.getItems())
				{
					dataGraph = queryNodeText(aName, vertexLabel, aQueryString);
					if (dataGraph.getVertexDocSet().size() > 0)
					{
						graphDS = new GraphDS(mAppCtx, dataGraph, aVertexSchemaDoc, null);
						if (dataGrid.rowCount() == 0)
							dataGrid = graphDS.getVertexGridDS().getDataGrid();
						else
							dataGrid.addRows(graphDS.getVertexGridDS().getDataGrid());
					}
				}

				if (dataGrid.rowCount() > 0)
				{
					GridDS gridDS = new GridDS(mAppCtx, dataGrid);
					dataGrid = gridDS.fetch(anOffset, aLimit);
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Queries the graph database for all nodes and relationships.
	 *
	 * @param aName Name of the graph database
	 *
	 * @return A data graph instance populated with all nodes and relationships
	 *
	 * @throws FCException Graph database exception
	 */
	public DataGraph queryAll(String aName)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "queryAll");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String graphName = graphKeyName(aName);
		DataGraph dataGraph = new DataGraph(aName, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);
		StringBuilder sbCypher = new StringBuilder();
		RedisCypher redisCypher = new RedisCypher(mAppCtx, dataGraph);
		redisCypher.loadAllNodesRelationships(sbCypher);
		String cypherCommand = sbCypher.toString();
		String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
		ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
		graphResultToDataGraph(dataGraph, resultSet);
		mRedisDS.saveCommand(appLogger, redisCommand);
		dataGraph.addFeature(Redis.FEATURE_CYPHER_QUERY, cypherCommand);
		dataGraph.addFeature(Redis.FEATURE_REDISGRAPH_QUERY, redisCommand);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGraph;
	}

	/**
	 * Updates the properties of the node based on the data document
	 * in the graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aDataDoc Node data document instance
	 *
	 * @return Count of properties updated
	 *
	 * @throws FCException Graph database exception
	 */
	public int updateNode(DataGraph aDataGraph, DataDoc aDataDoc)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "updateNode");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aDataDoc);
		int propertiesSet = 0;
		StringBuilder sbCypher = new StringBuilder();
		String graphName = graphKeyName(aDataGraph.getName());
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		DGCriteria dgCriteria = new DGCriteria(aDataGraph.getName());
		dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, aDataDoc));
		Optional<DataDoc> optDataDoc = queryNode(dgCriteria);
		if (optDataDoc.isPresent())
		{
			DataDoc rgDoc = optDataDoc.get();
			DataDocDiff dataDocDiff = new DataDocDiff();
			dataDocDiff.compare(rgDoc, aDataDoc);
			if (! dataDocDiff.isEqual())
			{
				sbCypher.setLength(0);
				redisCypher.matchCriteria(sbCypher, dgCriteria);
				boolean propertiesUpdated = redisCypher.updateNode(sbCypher, "n1", dataDocDiff, aDataDoc);
				if (propertiesUpdated)
				{
					String cypherCommand = sbCypher.toString();
					String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
					ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
					Statistics rgStatistics = resultSet.getStatistics();
					mRedisDS.saveCommand(appLogger, redisCommand);
					propertiesSet = rgStatistics.propertiesSet();
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return propertiesSet;
	}

	/**
	 * Updates the properties of the relationship based on the data document
	 * in the graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aDataDoc Relationship data document instance
	 *
	 * @return Count of relationship properties updated
	 *
	 * @throws RedisDSException Graph database exception
	 */
	public int updateRelationship(DataGraph aDataGraph, DataDoc aDataDoc)
		throws RedisDSException
	{
		String cypherCommand;
		Logger appLogger = mAppCtx.getLogger(this, "updateRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aDataDoc);
		int propertiesSet = 0;
		StringBuilder sbCypher = new StringBuilder();
		String graphName = graphKeyName(aDataGraph.getName());
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		Optional<DataItem> optPrimaryItem = aDataDoc.getPrimaryKeyItemOptional();
		if (optPrimaryItem.isPresent())
		{
			DataItem dataItem = optPrimaryItem.get();
			boolean propertiesUpdated = redisCypher.updateRelationship(sbCypher, "r1", aDataDoc);
			if (propertiesUpdated)
			{
				cypherCommand = String.format("MATCH ()-[r1:%s]-() WHERE r1.%s = %s %s", aDataDoc.getName(),
											  dataItem.getName(), redisCypher.getValue(dataItem), sbCypher);
				String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
				ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
				Statistics rgStatistics = resultSet.getStatistics();
				mRedisDS.saveCommand(appLogger, redisCommand);
				propertiesSet = rgStatistics.propertiesSet();
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return propertiesSet;
	}

	/**
	 * Deletes the node identified by the data document instance from the
	 * graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aDataDoc Node data document instance
	 *
	 * @return Count of nodes deleted
	 *
	 * @throws FCException Graph database exception
	 */
	public int deleteNode(DataGraph aDataGraph, DataDoc aDataDoc)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "deleteNode");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aDataDoc);
		int nodesDeleted = 0;
		String graphName = graphKeyName(aDataGraph.getName());
		StringBuilder sbCypher = new StringBuilder();
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		DGCriteria dgCriteria = new DGCriteria(aDataGraph.getName());
		dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, aDataDoc));
		redisCypher.matchCriteria(sbCypher, dgCriteria);
		if (sbCypher.length() > 0)
		{
			String cypherCommand = String.format("%s DELETE n1", sbCypher.toString());
			String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			Statistics rgStatistics = resultSet.getStatistics();
			mRedisDS.saveCommand(appLogger, redisCommand);
			nodesDeleted = rgStatistics.nodesDeleted();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return nodesDeleted;
	}

	/**
	 * Deletes the relationship identified by the data document instance and its
	 * relationship between the source and destination nodes from the
	 * graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aSrcNode Source node of the relationship
	 * @param aDstNode Destination node of the relationship
	 * @param aDataDoc Relationship data document instance
	 *
	 * @return Count of relationships deleted
	 *
	 * @throws RedisDSException Graph database exception
	 */
	public int deleteRelationship(DataGraph aDataGraph, DataDoc aSrcNode, DataDoc aDstNode, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "deleteRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aSrcNode, aDstNode, aDataDoc);
		int relationshipsDeleted = 0;
		String graphName = graphKeyName(aDataGraph.getName());
		StringBuilder sbCypher = new StringBuilder();
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		DGCriteria dgCriteria = new DGCriteria(aDataGraph.getName(), aSrcNode, aDstNode, aDataDoc);
		redisCypher.matchCriteria(sbCypher, dgCriteria);
		if (sbCypher.length() > 0)
		{
			String cypherCommand = String.format("%s DELETE r1", sbCypher.toString());
			String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			Statistics rgStatistics = resultSet.getStatistics();
			mRedisDS.saveCommand(appLogger, redisCommand);
			relationshipsDeleted = rgStatistics.relationshipsDeleted();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return relationshipsDeleted;
	}

	/**
	 * Deletes the relationship identified by the data document instance from the
	 * graph database identified by the data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aDataDoc Relationship data document instance
	 *
	 * @return Count of relationships deleted
	 *
	 * @throws RedisDSException Graph database exception
	 */
	public int deleteRelationship(DataGraph aDataGraph, DataDoc aDataDoc)
		throws RedisDSException
	{
		String cypherCommand;
		Logger appLogger = mAppCtx.getLogger(this, "deleteRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(aDataDoc);
		int relationshipsDeleted = 0;
		String graphName = graphKeyName(aDataGraph.getName());
		RedisCypher redisCypher = new RedisCypher(mAppCtx, aDataGraph);
		Optional<DataItem> optPrimaryItem = aDataDoc.getPrimaryKeyItemOptional();
		if (optPrimaryItem.isPresent())
		{
			DataItem dataItem = optPrimaryItem.get();
			cypherCommand = String.format("MATCH ()-[r1:%s]-() WHERE r1.%s = %s DELETE r1", aDataDoc.getName(),
										  dataItem.getName(), redisCypher.getValue(dataItem));
			String redisCommand = String.format("GRAPH.QUERY %s \"%s\"", mRedisDS.escapeKey(graphName), escapeCypherValue(cypherCommand));
			ResultSet resultSet = mCmdConnection.graphQuery(graphName, cypherCommand);
			Statistics rgStatistics = resultSet.getStatistics();
			mRedisDS.saveCommand(appLogger, redisCommand);
			relationshipsDeleted = rgStatistics.relationshipsDeleted();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return relationshipsDeleted;
	}

	/**
	 * Deletes the graph database identified by the data graph instance.  This logic
	 * assumes that secondary property and text indices are removed upon graph deletion.
	 *
	 * @param aDataGraph Data graph instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void delete(DataGraph aDataGraph)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions();
		String graphName = graphKeyName(aDataGraph.getName());
		String redisCommand = String.format("GRAPH.DELETE %s", mRedisDS.escapeKey(graphName));
		String resultMessage = mCmdConnection.graphDelete(graphName);
		if (StringUtils.startsWith(resultMessage, "Graph removed"))
			mRedisDS.saveCommand(appLogger, redisCommand);
		else
			throw new RedisDSException(String.format("[%s] Graph deletion failed: %s", graphName, resultMessage));
		RedisCore redisCore = mRedisDS.createCore();
		if (schemaExists(true))
		{
			String schemaKeyName = getSchemaKeyName(true);
			redisCore.delete(schemaKeyName);
		}
		if (schemaExists(false))
		{
			String schemaKeyName = getSchemaKeyName(false);
			redisCore.delete(schemaKeyName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
