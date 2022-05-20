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

package com.redis.ds.ds_graph;

import com.redis.ds.ds_grid.GridDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridCSV;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The graph data source manages an in-memory graph of vertexes
 * and edges using an underlying GridDS to handle CRUD+S methods,
 * storage/retrieval of the data sets via CSV files and the
 * visualization of the current graph.
 *
 * Presentation UIs like SmartClient favor grid presentations, so
 * this package will focus on normalizing the data set behind a
 * DataGraph into vertex and edge grids where CRUD+S operations
 * can be easily managed at the expense of duplicate memory
 * usage.
 *
 * NOTES:
 * 1) The vertex and edge GridDS instances are the "source of truth"
 *    in this package.  A DataGraph can be provided to seed the
 *    vertex and edge GridDS instance, but an internal one is not
 *    maintained.  Instead, methods are provided to create a new
 *    DataGraph based on the vertex and edge GridDS instances.
 * 2) Unlike the DataGraph package which will only accept
 * 	  Vertex and Edge data document instances uniquely, this
 *    package will align them with unified schema definitions
 *    to support simplified UI presentations.
 *
 * @see <a href="https://visjs.github.io/vis-network/docs/network/">VisJS Network Documentation</a>
 * @see <a href="https://visjs.github.io/vis-network/docs/network/nodes.html">VisJS Node Documentation</a>
 * @see <a href="https://visjs.github.io/vis-network/docs/network/edges.html">VisJS Edges Documentation</a>
 * @see <a href="https://visjs.github.io/vis-data/data/index.html">VisJS Data Documentation</a>
 * @see <a href="https://visjs.github.io/vis-data/data/dataset.html">VisJS DataSet Documentation</a>
 * @see <a href="https://jsfiddle.net/api/post/library/pure/">VisJS Interactive Playground</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class GraphDS
{
	private final String ROW_TYPE_EDGE = "E";
	private final String ROW_TYPE_VERTEX = "V";
	private final int QUERY_LIMIT_DEFAULT = 10000;			// Unlikely to impact result set size

	private String mName;
	private GridDS mEdgeGridDS;
	private GridDS mVertexGridDS;
	private final AppCtx mAppCtx;
	private DataDoc mEdgeSchemaDoc;
	private DataDoc mVertexSchemaDoc;
	private char mDelimiterChar = StrUtl.CHAR_PIPE;
	private transient HashMap<String, Object> mProperties;

	/**
     * Constructor accepts an application context and graph name parameters
	 * and initializes the class accordingly.  Use this constructor when
	 * you are fully building the underlying data graph and its related
	 * grid data sources.
	 *
	 * @param anAppCtx Application context
	 * @param aName Name of the data graph
	*/
	public GraphDS(AppCtx anAppCtx, String aName)
	{
		mAppCtx = anAppCtx;
		mEdgeGridDS = new GridDS(anAppCtx);
		mVertexGridDS = new GridDS(anAppCtx);
		setName(aName);
	}

	/**
	 * Constructor accepts an application context and graph name parameters
	 * and initializes the class accordingly.  Use this when you have
	 * previously defined the data graph vertex and edge schemas (e.g. CSV)
	 * and can assign them prior to adding items.
	 *
	 * @param anAppCtx Application context
	 * @param aName Name of the data graph
	 * @param aVertexSchemaDoc Vertex schema definition
	 * @param anEdgeSchemaDoc Edge schema definition
	 */
	public GraphDS(AppCtx anAppCtx, String aName, DataDoc aVertexSchemaDoc, DataDoc anEdgeSchemaDoc)
	{
		mAppCtx = anAppCtx;
		mEdgeSchemaDoc = anEdgeSchemaDoc;
		mEdgeGridDS = new GridDS(anAppCtx, anEdgeSchemaDoc);
		mVertexSchemaDoc = aVertexSchemaDoc;
		mVertexGridDS = new GridDS(anAppCtx, aVertexSchemaDoc);
		setName(aName);
	}

	/**
	 * Constructor accepts an application context and a data graph parameters
	 * and initializes the class accordingly. Use this when you intend to
	 * save the data graph as a CSV file where during that process the
	 * unified vertex and edge schemas will be defined.
	 *
	 * @param anAppCtx Application context
	 * @param aDataGraph Data graph instance
	 * @param aVertexSchemaDoc Vertex schema definition (can be null)
	 * @param anEdgeSchemaDoc Edge schema definition (can be null)
	 *
	 * @throws DSException Data source exception
	 * @throws FCException Data model mismatch
	 */
	public GraphDS(AppCtx anAppCtx, DataGraph aDataGraph, DataDoc aVertexSchemaDoc, DataDoc anEdgeSchemaDoc)
		throws FCException
	{
		mAppCtx = anAppCtx;
		mEdgeSchemaDoc = anEdgeSchemaDoc;
		mVertexSchemaDoc = aVertexSchemaDoc;
		populateGridsFromDataGraph(aDataGraph);
	}

	/**
	 * Constructor accepts an application context and a data graph parameters
	 * and initializes the class accordingly. Use this when you intend to
	 * save the data graph as a CSV file where during that process the
	 * unified vertex and edge schemas will be defined.
	 *
	 * @param anAppCtx Application context
	 * @param aDataGraph Data graph instance
	 *
	 * @throws DSException Data source exception
	 * @throws FCException Data model mismatch
	 */
	public GraphDS(AppCtx anAppCtx, DataGraph aDataGraph)
		throws FCException
	{
		mAppCtx = anAppCtx;
		mName = aDataGraph.getName();
		populateGridsFromDataGraph(aDataGraph);
	}

	/**
	 * Returns a string summary representation of the data source.
	 *
	 * @return String summary representation of the data source
	 */
	@Override
	public String toString()
	{
		return String.format("Vertexes: %s - Edges: %s", mVertexGridDS.toString(), mEdgeGridDS.toString());
	}

	/**
	 * Assign a name to the grid data sourcse.
	 *
	 * @param aName Name of the data source
	 */
	public void setName(String aName)
	{
		mName = aName;
		mEdgeGridDS.setName(String.format("%s - Edges", aName));
		mVertexGridDS.setName(String.format("%s - Vertexes", aName));
	}

	/**
	 * Returns the name of the data source.
	 *
	 * @return Data source name
	 */
	public String getName()
	{
		return mName;
	}

	/**
	 * Returns the internally managed graph vertex grid data source instance.
	 *
	 * @return Grid data source instance
	 */
	public GridDS getVertexGridDS()
	{
		return mVertexGridDS;
	}

	/**
	 * Returns the internally managed graph vertex schema data doc instance.
	 *
	 * @return Vertex schema data document instance
	 */
	public DataDoc getVertexSchema()
	{
		return mVertexGridDS.getSchema();
	}

	/**
	 * Convenience method that identifies the vertex data grid row count.
	 *
	 * @return Row count
	 */
	public int vertexRowCount()
	{
		return mVertexGridDS.getDataGrid().rowCount();
	}

	/**
	 * Returns the internally managed graph edge grid data source instance.
	 *
	 * @return Grid data source instance
	 */
	public GridDS getEdgeGridDS()
	{
		return mEdgeGridDS;
	}

	/**
	 * Returns the internally managed graph edge schema data doc instance.
	 *
	 * @return Edge schema data document instance
	 */
	public DataDoc getEdgeSchema()
	{
		return mEdgeGridDS.getSchema();
	}

	/**
	 * Convenience method that identifies the edge data grid row count.
	 *
	 * @return Row count
	 */
	public int edgeRowCount()
	{
		return mEdgeGridDS.getDataGrid().rowCount();
	}

	/**
	 * Assigns a delimiter character for data items that are multi-value.
	 *
	 * @param aDelimiterChar Delimiter character.
	 */
	public void setMultiValueDelimiterChar(char aDelimiterChar)
	{
		mDelimiterChar = aDelimiterChar;
	}

	/**
	 * Returns the graph vertex file name (derived from the internal data source name).
	 *
	 * @return Data source schema definition file name
	 */
	public String createVertexSchemaFileName()
	{
		String fileName = StrUtl.removeAllChar(mName.toLowerCase(), StrUtl.CHAR_SPACE);
		return String.format("%s_vertex_schema.xml", fileName);
	}

	/**
	 * Returns the graph vertex path/file name derived from the path name, value format type
	 * (e.g. xml or csv) and the internal data source name.
	 *
	 * @param aPathName Path name where the file should be written.
	 *
	 * @return Data source values path/file name.
	 */
	public String createVertexSchemaPathFileName(String aPathName)
	{
		return String.format("%s%c%s", aPathName, File.separatorChar, createVertexSchemaFileName());
	}

	/**
	 * Returns the graph vertex file name (derived from the internal data source name).
	 *
	 * @return Data source schema definition file name
	 */
	public String createEdgeSchemaFileName()
	{
		String fileName = StrUtl.removeAllChar(mName.toLowerCase(), StrUtl.CHAR_SPACE);
		return String.format("%s_edge_schema.xml", fileName);
	}

	/**
	 * Returns the graph vertex path/file name derived from the path name, value format type
	 * (e.g. xml or csv) and the internal data source name.
	 *
	 * @param aPathName Path name where the file should be written.
	 *
	 * @return Data source values path/file name.
	 */
	public String createEdgeSchemaPathFileName(String aPathName)
	{
		return String.format("%s%c%s", aPathName, File.separatorChar, createEdgeSchemaFileName());
	}

	/**
	 * Returns the SmartClient path/file name (derived from the path name parameter
	 * and the internal data source name) for the graph vertexes.
	 *
	 * @param aPathName Path name where the file should be written
	 *
	 * @return Smart GWT data source definition path/file name
	 */
	public String createSmartClientVertexPathFileName(String aPathName)
	{
		String dsName = StrUtl.removeAllChar(mName.toLowerCase(), StrUtl.CHAR_SPACE);
		return String.format("%s%c%sVertex.ds.xml", aPathName, File.separatorChar, dsName);
	}

	/**
	 * Returns the SmartClient path/file name (derived from the path name parameter
	 * and the internal data source name) for the graph edges.
	 *
	 * @param aPathName Path name where the file should be written
	 *
	 * @return Smart GWT data source definition path/file name
	 */
	public String createSmartClientEdgePathFileName(String aPathName)
	{
		String dsName = StrUtl.removeAllChar(mName.toLowerCase(), StrUtl.CHAR_SPACE);
		return String.format("%s%c%sEdge.ds.xml", aPathName, File.separatorChar, dsName);
	}

	/**
	 * Stores the graph vertex schema definition of the underlying data source
	 * (formatted in XML) to the file system.
	 *
	 * @param aPathFileName Path/file schema should be written to
	 *
	 * @throws IOException I/O related exception
	 */
	public void saveVertexSchema(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveVertexSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mVertexGridDS.saveSchema(aPathFileName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the graph vertex schema definition file and assigns it
	 * to the internal grid data source instance.
	 *
	 * @param aPathFileName Path/file schema should be written to
	 *
	 * @throws IOException I/O related exception
	 * @throws ParserConfigurationException Parser exception
	 * @throws SAXException DOM exception
	 */
	public void loadVertexSchema(String aPathFileName)
		throws IOException, ParserConfigurationException, SAXException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadVertexSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mVertexGridDS.loadSchema(aPathFileName);
		mVertexSchemaDoc = new DataDoc(mVertexGridDS.getSchema());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the graph edge schema definition file and assigns it
	 * to the internal grid data source instance.
	 *
	 * @param aPathFileName Path/file schema should be written to
	 *
	 * @throws IOException I/O related exception
	 * @throws ParserConfigurationException Parser exception
	 * @throws SAXException DOM exception
	 */
	public void loadEdgeSchema(String aPathFileName)
		throws IOException, ParserConfigurationException, SAXException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadEdgeSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mEdgeGridDS.loadSchema(aPathFileName);
		mEdgeSchemaDoc = new DataDoc(mEdgeGridDS.getSchema());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Stores the graph edges schema definition of the underlying data source
	 * (formatted in XML) to the file system.
	 *
	 * @param aPathFileName Path/file schema should be written to
	 *
	 * @throws IOException I/O related exception
	 */
	public void saveEdgeSchema(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveEdgeSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mEdgeGridDS.saveSchema(aPathFileName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Generates an extended edge schema suitable for UI presentations.
	 *
	 * @param anIsEdgeDirectionHidden If true, then hide it in the definition
	 *
	 * @return Extended data document instance.
	 */
	public DataDoc createEdgeExtendedSchema(boolean anIsEdgeDirectionHidden)
	{
		Logger appLogger = mAppCtx.getLogger(this, "createEdgeExtendedSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc edgeSchemaDoc = mEdgeGridDS.getSchema();
		DataDoc edgeExtendedSchemaDoc = new DataDoc(edgeSchemaDoc.getName());
		for (DataItem dataItem : edgeSchemaDoc.getItems())
		{
			if (dataItem.getName().startsWith(Data.GRAPH_COMMON_PREFIX))
			{
				edgeExtendedSchemaDoc.add(new DataItem(dataItem));
				if (dataItem.getName().equals(Data.GRAPH_DST_VERTEX_ID_NAME))
				{
					edgeExtendedSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_EDGE_DIRECTION_NAME).title(Data.GRAPH_EDGE_DIRECTION_TITLE).isHidden(anIsEdgeDirectionHidden).build());
					edgeExtendedSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_COMMON_ID).title(Data.GRAPH_COMMON_ID_TITLE).isVisible(false).build());
				}
			}
		}
		edgeExtendedSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_VERTEX_NAME).title(Data.GRAPH_VERTEX_TITLE).build());
		for (DataItem dataItem : edgeSchemaDoc.getItems())
		{
			if (! dataItem.getName().startsWith(Data.GRAPH_COMMON_PREFIX))
				edgeExtendedSchemaDoc.add(new DataItem(dataItem));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return edgeExtendedSchemaDoc;
	}

	/**
	 * Stores the graph edges extended schema definition of the underlying data source
	 * (formatted in XML) to the file system.
	 *
	 * @param aPathFileName Path/file schema should be written to
	 * @param anIsEdgeDirectionHidden If true, then hide it in the definition
	 *
	 * @throws IOException I/O related exception
	 */
	public void saveEdgeExtendedSchema(String aPathFileName, boolean anIsEdgeDirectionHidden)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveEdgeExtendedSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc edgeExtendedSchemaDoc = createEdgeExtendedSchema(anIsEdgeDirectionHidden);
		DataDocXML dataDocXML = new DataDocXML(edgeExtendedSchemaDoc);
		dataDocXML.save(aPathFileName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void populateGridsFromDataGraph(DataGraph aDataGraph)
		throws FCException
	{
		DataGrid dataGrid;
		Logger appLogger = mAppCtx.getLogger(this, "populateGridsFromDataGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mEdgeGridDS = new GridDS(mAppCtx);
		mEdgeGridDS.getDataGrid().emptyRows();
		mVertexGridDS = new GridDS(mAppCtx);
		mVertexGridDS.getDataGrid().emptyRows();
		if (mEdgeSchemaDoc == null)
			dataGrid = aDataGraph.getEdgesDataGrid();
		else
			dataGrid = aDataGraph.getEdgesDataGrid(mEdgeSchemaDoc);
		mEdgeGridDS.setDatGrid(dataGrid);
		if (mVertexSchemaDoc == null)
			dataGrid = aDataGraph.getVertexDataGrid();
		else
			dataGrid = aDataGraph.getVertexDataGrid(mVertexSchemaDoc);
		DataDoc vertexSchemaDoc = dataGrid.getColumns();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isEmpty())
			throw new DSException("Graph vertex documents must have a primary item feature assigned.");
		mVertexGridDS.setDatGrid(dataGrid);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Creates a new data graph instance from the internally managed grid data sources.
	 * This is typically required when the grid was updated by a UI application.
	 *
	 * @param aName Name of the new data graph
	 *
	 * @throws FCException Redis Labs exception
	 */
	public DataGraph createDataGraph(String aName)
		throws FCException
	{
		boolean isOK;
		DataGraph dataGraph;
		String srcVertexId, dstVertexId;
		DataDoc vertexDoc, edgeDoc, srcVertexDoc, dstVertexDoc;
		DataItem diGraphWeight, diVertexPK, diVertexLabel, diEdgePK, diEdgeGraphType;
		Logger appLogger = mAppCtx.getLogger(this, "createDataGraph");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid vertexDataGrid = mVertexGridDS.getDataGrid();
		DataDoc vertexSchemaDoc = vertexDataGrid.getColumns();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diVertexPK = optDataItem.get();
		else
		{
			String errMsg = "Graph vertex documents must have a primary item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		optDataItem = mVertexGridDS.getSchema().getItemByFeatureEnabledOptional(Data.FEATURE_IS_GRAPH_LABEL);
		if (optDataItem.isEmpty())
		{
			String errMsg = "Graph vertex documents must have a label item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		diVertexLabel = optDataItem.get();

		optDataItem = mEdgeGridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diEdgePK = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate edge item by feature name '%s'.", Data.FEATURE_IS_PRIMARY));
		DataDoc edgeSchemaDoc = mEdgeGridDS.getSchema();
		optDataItem = edgeSchemaDoc.getItemByNameOptional(Data.GRAPH_EDGE_TYPE_NAME);
		if (optDataItem.isEmpty())
			throw new DSException(String.format("Unable to locate edge item by name '%s'.", Data.GRAPH_EDGE_TYPE_NAME));
		DataGrid edgeDataGrid = mEdgeGridDS.getDataGrid();
		optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_WEIGHT);
		if (optDataItem.isPresent())
			dataGraph = new DataGraph(aName, Data.GraphStructure.DirectedWeightedPseudograph, Data.GraphData.DocDoc);
		else
			dataGraph = new DataGraph(aName, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);

// First, we focus on creating the vertexes.

		int vertexRowCount = vertexDataGrid.rowCount();
		for (int row = 0; row < vertexRowCount; row++)
		{
			vertexDoc = vertexDataGrid.getRowAsDoc(row);
			vertexDoc.setName(vertexDoc.getValueByName(diVertexLabel.getName()));
			vertexDoc.remove(Data.GRAPH_VERTEX_LABEL_NAME);
			isOK = dataGraph.addVertex(vertexDoc);
			if (! isOK)
				appLogger.error(String.format("Vertex data graph add operation failed for id '%s'.", vertexDoc.getValueByName(diVertexPK.getName())));
		}

// Now we can create the edges that link the vertexes.

		int edgeRowCount = edgeDataGrid.rowCount();
		for (int row = 0; row < edgeRowCount; row++)
		{
			edgeDoc = edgeDataGrid.getRowAsDoc(row);
			edgeDoc.setName(edgeDoc.getValueByName(Data.GRAPH_EDGE_TYPE_NAME));
			srcVertexId = edgeDoc.getValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME);
			srcVertexDoc = dataGraph.getVertexDocByNameValue(diVertexPK.getName(), srcVertexId);
			if (srcVertexDoc != null)
			{
				dstVertexId = edgeDoc.getValueByName(Data.GRAPH_DST_VERTEX_ID_NAME);
				dstVertexDoc = dataGraph.getVertexDocByNameValue(diVertexPK.getName(), dstVertexId);
				if (dstVertexDoc != null)
				{
					optDataItem = edgeDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_WEIGHT);
					diGraphWeight = optDataItem.orElse(null);
					try
					{
						edgeDoc.remove(Data.GRAPH_SRC_VERTEX_ID_NAME);
						edgeDoc.remove(Data.GRAPH_DST_VERTEX_ID_NAME);
						if ((diGraphWeight != null) && (diGraphWeight.isValueAssigned()))
							isOK = dataGraph.addEdge(srcVertexDoc, dstVertexDoc, edgeDoc, diGraphWeight.getValueAsDouble());
						else
							isOK = dataGraph.addEdge(srcVertexDoc, dstVertexDoc, edgeDoc);
						if (! isOK)
							appLogger.error(String.format("Edge data graph add operation failed for source id '%s' and destination id '%s'.", srcVertexId, dstVertexId));
					}
					catch (IllegalArgumentException e)
					{
						String errMsg = String.format("[Row %d]: %s - source vertex id '%s' and destination vertex id '%s'", row+1, e.getMessage(), srcVertexId, dstVertexId);
						appLogger.error(errMsg);
						throw new DSException(errMsg);
					}
				}
				else
					appLogger.error(String.format("Unable to locate destination vertex by id '%s' - skipping record.", dstVertexId));
			}
			else
				appLogger.error(String.format("Unable to locate source vertex by id '%s' - skipping record.", srcVertexId));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGraph;
	}

	/**
	 * Creates a new data graph instance from the internally managed grid data sources.
	 * This is typically required when the grid was updated by a UI application.
	 *
	 * @throws FCException Redis Labs exception
	 */
	public DataGraph createGraph()
		throws FCException
	{
		return createDataGraph(mName);
	}

	/**
	 * Assigns graph vertex column name serving as the unique primary key.
	 *
	 * @param aName Name of the column
	 */
	public void setVertexPrimaryKey(String aName)
	{
		DataDoc schemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			dataItem.enableFeature(Data.FEATURE_IS_PRIMARY);
		}
	}

	private void populateVertexDetails(DataDoc anEdgeDoc, String anId, DataItem aPrimaryItem,
									   DataItem aGraphTitleItem, String aDirection)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "populateVertexDetails");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		anEdgeDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_VERTEX_NAME).title(Data.GRAPH_VERTEX_TITLE).build());
		anEdgeDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_EDGE_DIRECTION_NAME).title(Data.GRAPH_EDGE_DIRECTION_TITLE).value(aDirection).build());
		if (aGraphTitleItem != null)
		{
			DSCriteria dsCriteria = new DSCriteria(getName() + "'%s' Criteria");
			dsCriteria.add(aPrimaryItem.getName(), anId);
			DataGrid vertexDataGrid = mVertexGridDS.fetch(dsCriteria, 0, 1);
			if (vertexDataGrid.rowCount() > 0)
				anEdgeDoc.setValueByName(Data.GRAPH_VERTEX_NAME, vertexDataGrid.getRowAsDoc(0).getValueByName(aGraphTitleItem.getName()));
			else
				appLogger.error(String.format("Unable to locate vertex by id '%s' in data source.", anId));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private Optional<DataDoc> findVertexDocByNameValue(String aName, String aValue)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "findVertexDocByNameValue");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc dataDoc = null;
		DSCriteria dsCriteria = new DSCriteria(mVertexGridDS.getName() + " Criteria");
		dsCriteria.add(aName, Data.Operator.EQUAL, aValue);
		DataGrid dataGrid = mVertexGridDS.fetch(dsCriteria, 0, 1);
		if (dataGrid.rowCount() > 0)
			dataDoc = dataGrid.getRowAsDoc(0);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(dataDoc);
	}

	/**
	 * Convenience method that retrieves the vertex document instance
	 * at the row offset specified.  In addition, the method will assign
	 * the vertex label to the data document name.
	 *
	 * @param aRow Row offset (starting with zero)
	 *
	 * @return Data document instance
	 */
	public DataDoc vertexRowAsDoc(int aRow)
	{
		DataDoc vertexDoc = mVertexGridDS.getDataGrid().getRowAsDoc(aRow);
		String vertexLabelName = vertexDoc.getValueByName(Data.GRAPH_VERTEX_LABEL_NAME);
		if (StringUtils.isNotEmpty(vertexLabelName))
			vertexDoc.setName(vertexLabelName);

		return vertexDoc;
	}

	/**
	 * Convenience method that retrieves the edge document instance
	 * at the row offset specified.  In addition, the method will assign
	 * the edge type to the data document name.
	 *
	 * @param aRow Row offset (starting with zero)
	 *
	 * @return Data document instance
	 */
	public DataDoc edgeRowAsDoc(int aRow)
	{
		DataDoc edgeDoc = mEdgeGridDS.getDataGrid().getRowAsDoc(aRow);
		String edgeType = edgeDoc.getValueByName(Data.GRAPH_EDGE_TYPE_NAME);
		if (StringUtils.isNotEmpty(edgeType))
			edgeDoc.setName(edgeType);

		return edgeDoc;
	}

	/**
	 * Assigns graph edge column name serving as the unique primary key.
	 *
	 * @param aName Name of the column
	 */
	public void setEdgePrimaryKey(String aName)
	{
		DataDoc schemaDoc = mEdgeGridDS.getSchema();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			dataItem.enableFeature(Data.FEATURE_IS_PRIMARY);
		}
	}

	/**
	 * Identifies if the feature name is standard to the foundation
	 * data package.
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
				case Data.FEATURE_IS_HIDDEN:
				case Data.FEATURE_IS_SEARCH:
				case Data.FEATURE_IS_PRIMARY:
				case Data.FEATURE_IS_VISIBLE:
				case Data.FEATURE_IS_SUGGEST:
				case Data.FEATURE_IS_REQUIRED:
				case Data.FEATURE_IS_GRAPH_TYPE:
				case Data.FEATURE_IS_GRAPH_LABEL:
				case Data.FEATURE_IS_GRAPH_WEIGHT:
					return true;
			}
		}

		return false;
	}

	/**
	 * Creates a vertex data document with items suitable for a schema editor UI.
	 *
	 * @param aName Name of the schema
	 *
	 * @return Data document representing a schema
	 */
	public DataDoc createVertexSchema(String aName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "createVertexSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = new DataDoc(aName);
		schemaDoc.add(new DataItem.Builder().name("item_name").title("Item Name").build());
		schemaDoc.add(new DataItem.Builder().name("item_type").title("Item Type").build());
		schemaDoc.add(new DataItem.Builder().name("item_title").title("Item Title").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_PRIMARY).title("Is Primary").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_REQUIRED).title("Is Required").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_VISIBLE).title("Is Visible").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_SEARCH).title("Is Searchable").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_SUGGEST).title("Is Suggest").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_GRAPH_LABEL).title("Is Graph Label").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_GRAPH_TITLE).title("Is Graph Title").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_HIDDEN).title("Is Hidden").build());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return schemaDoc;
	}

	/**
	 * Convert a data document vertex schema definition into a data grid suitable for
	 * rendering in a schema editor UI.
	 *
	 * @param aSchemaDoc Data document instance (representing the schema)
	 * @param anIsExtended If <i>true</i>, then non standard features will be recognized
	 *
	 * @return Data grid representing the schema defintion
	 */
	public DataGrid vertexSchemaDocToDataGrid(DataDoc aSchemaDoc, boolean anIsExtended)
	{
		String schemaName;
		HashMap<String,String> mapFeatures;
		Logger appLogger = mAppCtx.getLogger(this, "vertexSchemaDocToDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Create our initial data grid schema based on standard item info plus features.

		if (StringUtils.endsWith(aSchemaDoc.getName(), "Schema"))
			schemaName = aSchemaDoc.getName();
		else
			schemaName = String.format("%s Schema", aSchemaDoc.getName());
		DataDoc schemaDoc = createVertexSchema(schemaName);
		DataGrid dataGrid = new DataGrid(schemaDoc);

// Extend the data grid schema for use defined features.

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
			if (dataItem.getName().equals(Data.GRAPH_VERTEX_LABEL_NAME))
				dataGrid.setValueByName(Data.FEATURE_IS_GRAPH_LABEL, true);
			dataGrid.addRow();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Updates the data items captured in the vertex <i>DataDoc</i>
	 * against the schema.  The data items must be derived
	 * from the schema definition.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aSchemaDoc Data document instance
	 *
	 * @return <i>true</i> on success and <i>false</i> otherwise
	 */
	public boolean updateVertexSchema(DataDoc aSchemaDoc)
	{
		String itemName, featureName, featureValue;
		Logger appLogger = mAppCtx.getLogger(this, "updateVertexSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = false;
		DataDoc schemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aSchemaDoc.getValueByName("item_name"));
		if (optDataItem.isPresent())
		{
			DataItem schemaItem = optDataItem.get();
			schemaItem.clearFeatures();
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
					featureName = dataItem.getName();
					featureValue = dataItem.getValue();
					if (Data.isValueTrue(featureValue))
					{
						if ((featureName.equals(Data.FEATURE_IS_SEARCH)) || (featureName.equals(Data.FEATURE_IS_SUGGEST)))
						{
							if (Data.isText(schemaItem.getType()))
							{
								schemaItem.addFeature(featureName, featureValue);
								if (! isOK) isOK = true;
							}
						}
						else
						{
							schemaItem.addFeature(featureName, featureValue);
							if (! isOK) isOK = true;
						}
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Creates an edge data document with items suitable for a schema editor UI.
	 *
	 * @param aName Name of the schema
	 *
	 * @return Data document representing a schema
	 */
	public DataDoc createEdgeSchema(String aName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "createEdgeSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = new DataDoc(aName);
		schemaDoc.add(new DataItem.Builder().name("item_name").title("Item Name").build());
		schemaDoc.add(new DataItem.Builder().name("item_type").title("Item Type").build());
		schemaDoc.add(new DataItem.Builder().name("item_title").title("Item Title").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_PRIMARY).title("Is Primary").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_REQUIRED).title("Is Required").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_VISIBLE).title("Is Visible").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_GRAPH_TYPE).title("Is Graph Type").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_GRAPH_TITLE).title("Is Graph Title").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_GRAPH_WEIGHT).title("Is Graph Weight").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_HIDDEN).title("Is Hidden").build());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return schemaDoc;
	}

	/**
	 * Convert a data document edge schema definition into a data grid suitable for
	 * rendering in a schema editor UI.
	 *
	 * @param aSchemaDoc Data document instance (representing the schema)
	 * @param anIsExtended If <i>true</i>, then non standard features will be recognized
	 *
	 * @return Data grid representing the schema defintion
	 */
	public DataGrid edgeSchemaDocToDataGrid(DataDoc aSchemaDoc, boolean anIsExtended)
	{
		String schemaName;
		HashMap<String,String> mapFeatures;
		Logger appLogger = mAppCtx.getLogger(this, "edgeSchemaDocToDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Create our initial data grid schema based on standard item info plus features.

		if (StringUtils.endsWith(aSchemaDoc.getName(), "Schema"))
			schemaName = aSchemaDoc.getName();
		else
			schemaName = String.format("%s Schema", aSchemaDoc.getName());
		DataDoc schemaDoc = createEdgeSchema(schemaName);
		DataGrid dataGrid = new DataGrid(schemaDoc);

// Extend the data grid schema for use defined features.

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

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Updates the data items captured in the edge <i>DataDoc</i>
	 * against the schema.  The data items must be derived
	 * from the schema definition.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aSchemaDoc Data document instance
	 *
	 * @return <i>true</i> on success and <i>false</i> otherwise
	 */
	public boolean updateEdgeSchema(DataDoc aSchemaDoc)
	{
		String itemName, featureName, featureValue;
		Logger appLogger = mAppCtx.getLogger(this, "updateEdgeSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = false;
		DataDoc schemaDoc = mEdgeGridDS.getSchema();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aSchemaDoc.getValueByName("item_name"));
		if (optDataItem.isPresent())
		{
			DataItem schemaItem = optDataItem.get();
			schemaItem.clearFeatures();
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
					featureName = dataItem.getName();
					featureValue = dataItem.getValue();
					if (Data.isValueTrue(featureValue))
					{
						if ((featureName.equals(Data.FEATURE_IS_SEARCH)) || (featureName.equals(Data.FEATURE_IS_SUGGEST)))
						{
							if (Data.isText(schemaItem.getType()))
							{
								schemaItem.addFeature(featureName, featureValue);
								if (! isOK) isOK = true;
							}
						}
						else
						{
							schemaItem.addFeature(featureName, featureValue);
							if (! isOK) isOK = true;
						}
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Convenience method that queries the data graph by the primary id
	 * parameter and returns the matching data document as optional.
	 *
	 * @param aDataGraph Data graph instance
	 * @param anId Vertex primary id value
	 *
	 * @return Vertex data document instance
	 *
	 * @throws FCException Redis Labs exception
	 */
	public Optional<DataDoc> queryVertexByPrimaryId(DataGraph aDataGraph, String anId)
		throws FCException
	{
		DataDoc dgDoc;
		DataItem diPrimaryKey;
		Logger appLogger = mAppCtx.getLogger(this, "queryVertexByPrimaryId");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc vertexSchemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate vertex item by feature name '%s'", Data.FEATURE_IS_PRIMARY));

		try
		{
			dgDoc = aDataGraph.getVertexDocByNameValue(diPrimaryKey.getName(), anId);
		}
		catch (NoSuchElementException e)
		{
			dgDoc = null;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(dgDoc);
	}

	/**
	 * Convenience method that queries the data graph by an item name
	 * and value parameters - returning the matching data document as
	 * optional.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aName Item name
	 * @param aValue Item value
	 *
	 * @return Vertex data document instance
	 *
	 * @throws FCException Redis Labs exception
	 */
	public Optional<DataDoc> queryVertexByNameValue(DataGraph aDataGraph, String aName, String aValue)
		throws FCException
	{
		DataDoc dgDoc;
		Logger appLogger = mAppCtx.getLogger(this, "queryVertexByNameValue");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isEmpty(aName)) || (StringUtils.isEmpty(aValue)))
			throw new FCException("Name/Value parameters are emtpy or null.");

		try
		{
			dgDoc = aDataGraph.getVertexDocByNameValue(aName, aValue);
		}
		catch (NoSuchElementException e)
		{
			dgDoc = null;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(dgDoc);
	}

	/**
	 * Convenience method that queries for all edges that match the primary
	 * key id (source and destination) and returns the matching data grid.
	 *
	 * @param anId Primary key value.
	 * @param aLimit Fetch limit
	 *
	 * @return Data grid instance
	 *
	 * @throws DSException Data source exception
	 */
	public DataGrid queryEdgesByPrimaryId(String anId, int aLimit)
		throws DSException
	{
		String vertexId;
		DataDoc dataDoc;
		DataItem diPrimaryKey, diGraphTitle;
		Logger appLogger = mAppCtx.getLogger(this, "queryEdgesByPrimaryId");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc vertexSchemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate vertex item by feature name '%s'", Data.FEATURE_IS_PRIMARY));

		optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TITLE);
		diGraphTitle = optDataItem.orElse(null);

		DSCriteria dsCriteria = new DSCriteria(getName() + "'%s' Criteria");
		dsCriteria.add(Data.GRAPH_SRC_VERTEX_ID_NAME, anId);
		DataGrid srcDataGrid = mEdgeGridDS.fetch(dsCriteria, 0, aLimit);
		dsCriteria.reset();
		dsCriteria.add(Data.GRAPH_DST_VERTEX_ID_NAME, anId);
		DataGrid dstDataGrid = mEdgeGridDS.fetch(dsCriteria, 0, aLimit);

		DataDoc edgeSchema = new DataDoc(mEdgeGridDS.getSchema());
		edgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_VERTEX_NAME).title(Data.GRAPH_VERTEX_TITLE).build());
		edgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_EDGE_DIRECTION_NAME).title(Data.GRAPH_EDGE_DIRECTION_TITLE).build());
		DataGrid dataGrid = new DataGrid(edgeSchema);

		int srcRowCount = srcDataGrid.rowCount();
		for (int row = 0; row < srcRowCount; row++)
		{
			dataDoc = srcDataGrid.getRowAsDoc(row);
			vertexId = dataDoc.getValueByName(Data.GRAPH_DST_VERTEX_ID_NAME);
			populateVertexDetails(dataDoc, vertexId, diPrimaryKey, diGraphTitle, Data.GRAPH_EDGE_DIRECTION_INBOUND);
			dataGrid.addRow(dataDoc);
		}
		int dstRowCount = dstDataGrid.rowCount();
		for (int row = 0; row < dstRowCount; row++)
		{
			dataDoc = dstDataGrid.getRowAsDoc(row);
			vertexId = dataDoc.getValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME);
			populateVertexDetails(dataDoc, vertexId, diPrimaryKey, diGraphTitle, Data.GRAPH_EDGE_DIRECTION_OUTBOUND);
			dataGrid.addRow(dataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Convenience method that queries for only outbound edges that match the
	 * primary key id (source and destination) and returns the matching data grid.
	 *
	 * @param anId Primary key value.
	 * @param aLimit Fetch limit
	 *
	 * @return Data grid instance
	 *
	 * @throws DSException Data source exception
	 */
	public DataGrid queryOutboundEdgesByPrimaryId(String anId, int aLimit)
		throws DSException
	{
		String vertexId;
		DataDoc dataDoc;
		DataItem diPrimaryKey, diGraphTitle;
		Logger appLogger = mAppCtx.getLogger(this, "queryOutboundEdgesByPrimaryId");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc vertexSchemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate vertex item by feature name '%s'", Data.FEATURE_IS_PRIMARY));

		optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TITLE);
		diGraphTitle = optDataItem.orElse(null);

		DSCriteria dsCriteria = new DSCriteria(getName() + "'%s' Criteria");
		dsCriteria.add(Data.GRAPH_SRC_VERTEX_ID_NAME, anId);
		DataGrid srcDataGrid = mEdgeGridDS.fetch(dsCriteria, 0, aLimit);

		DataDoc edgeSchema = new DataDoc(mEdgeGridDS.getSchema());
		edgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_VERTEX_NAME).title(Data.GRAPH_VERTEX_TITLE).build());
		edgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_EDGE_DIRECTION_NAME).title(Data.GRAPH_EDGE_DIRECTION_TITLE).build());
		DataGrid dataGrid = new DataGrid(edgeSchema);

		int srcRowCount = srcDataGrid.rowCount();
		for (int row = 0; row < srcRowCount; row++)
		{
			dataDoc = srcDataGrid.getRowAsDoc(row);
			vertexId = dataDoc.getValueByName(Data.GRAPH_DST_VERTEX_ID_NAME);
			populateVertexDetails(dataDoc, vertexId, diPrimaryKey, diGraphTitle, Data.GRAPH_EDGE_DIRECTION_INBOUND);
			dataGrid.addRow(dataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Adds the vertex data document to the internal grid data sources.
	 *
	 * @param aDataDoc Vertex data document instance
	 *
	 * @return <i>true</i> if the update succeeds or <i>false</i> otherwise
	 *
	 * @throws FCException Redis labs exception
	 */
	public boolean addVertex(DataDoc aDataDoc)
		throws FCException
	{
		boolean isOK;
		DataItem diPrimaryKey;
		Logger appLogger = mAppCtx.getLogger(this, "addVertex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
		{
			String errMsg = String.format("Unable to locate vertex item by feature name '%s'.", Data.FEATURE_IS_PRIMARY);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}

		Optional<DataDoc> optVertexDoc = mVertexGridDS.findDataDocByPrimaryId(diPrimaryKey.getValue());
		if (optVertexDoc.isPresent())
		{
			isOK = false;
			appLogger.info(String.format("Vertex data document with id '%s' already exists in data graph.", diPrimaryKey.getValue()));
		}
		else
		{
			optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_LABEL);
			if (optDataItem.isPresent())
			{
				DataItem diGraphLabel = optDataItem.get();
				DataDoc vertexDoc = new DataDoc(mVertexGridDS.getSchema());
				vertexDoc.setName(diGraphLabel.getValue());
				for (DataItem dataItem : aDataDoc.getItems())
					vertexDoc.setValuesByName(dataItem.getName(), dataItem.getValues());
				isOK = mVertexGridDS.add(vertexDoc);
			}
			else
			{
				String errMsg = String.format("Unable to locate vertex item by feature name '%s'.", Data.FEATURE_IS_GRAPH_LABEL);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Adds a data document instance as an edge between the source and
	 * destination data document vertexes to the internal grid data sources.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param aDataDoc Data document instance representing the edge
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataDoc aSrcVertex, DataDoc aDstVertex, DataDoc aDataDoc)
		throws FCException
	{
		boolean isOK;
		DataItem diePrimaryItem, divPrimaryItem, diGraphType;
		Logger appLogger = mAppCtx.getLogger(this, "addEdge");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Get references to our primary and graph related features.

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diePrimaryItem = optDataItem.get();
		else
		{
			String errMsg = String.format("Unable to locate edge item by feature name '%s'.", Data.FEATURE_IS_PRIMARY);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		optDataItem = aDataDoc.getItemByNameOptional(Data.GRAPH_EDGE_TYPE_NAME);
		if (optDataItem.isPresent())
			diGraphType = optDataItem.get();
		else
		{
			optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TYPE);
			if (optDataItem.isPresent())
				diGraphType = optDataItem.get();
			else
			{
				String errMsg = String.format("Unable to locate edge item by feature name '%s'.", Data.FEATURE_IS_GRAPH_TYPE);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
			}
		}
		optDataItem = aSrcVertex.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			divPrimaryItem = optDataItem.get();
		else
		{
			String errMsg = String.format("Unable to locate vertex item by feature name '%s'.", Data.FEATURE_IS_PRIMARY);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}

// Ensure the graph edge data document is schema compliant.

		DataDoc edgeDoc = new DataDoc(mEdgeGridDS.getSchema());
		edgeDoc.setName(diGraphType.getValue());
		for (DataItem dataItem : aDataDoc.getItems())
			edgeDoc.setValuesByName(dataItem.getName(), dataItem.getValues());

		edgeDoc.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, aSrcVertex.getValueByName(divPrimaryItem.getName()));
		edgeDoc.setValueByName(Data.GRAPH_DST_VERTEX_ID_NAME, aDstVertex.getValueByName(divPrimaryItem.getName()));
		edgeDoc.setValueByName(Data.GRAPH_VERTEX_NAME, aDstVertex.getValueByName(Data.GRAPH_COMMON_NAME));

// Execute the operation.

		isOK = mEdgeGridDS.add(edgeDoc);
		if (! isOK)
			appLogger.error(String.format("Edge GridDS add operation failed for id '%s'.", diePrimaryItem.getValue()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	private String getVertexIdByGraphTitle(DataItem aPrimaryItem, String aTitle)
		throws DSException
	{
		DataItem diGraphTitle;
		Logger appLogger = mAppCtx.getLogger(this, "getVertexIdByGraphTitle");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = mVertexGridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TITLE);
		if (optDataItem.isPresent())
			diGraphTitle = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate vertex item by feature name '%s'.", Data.FEATURE_IS_GRAPH_TITLE));

		DSCriteria dsCriteria = new DSCriteria(getName() + "'%s' Criteria");
		dsCriteria.add(diGraphTitle.getName(), aTitle);
		DataGrid vertexDataGrid = mVertexGridDS.fetch(dsCriteria, 0, 1);
		if (vertexDataGrid.rowCount() == 0)
			throw new DSException(String.format("Unable to locate vertex document by graph title '%s'.", aTitle));

		DataDoc vertexDoc = vertexDataGrid.getRowAsDoc(0);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return vertexDoc.getValueByName(aPrimaryItem.getName());
	}

	/**
	 * Adds the vertex data document to the grid data sources and its related edges (captured in the
	 * data grid parameter).
	 *
	 * @param aVertexDoc Vertex data document instance
	 * @param anEdgesDataGrid Data grid of edge documents
	 *
	 * @return <i>true</i> if the update succeeds or <i>false</i> otherwise
	 *
	 * @throws FCException Redis labs exception
	 */
	public boolean addVertexAndEdges(DataDoc aVertexDoc, DataGrid anEdgesDataGrid)
		throws FCException
	{
		DataDoc dataDoc;
		DataItem diPrimaryKey, diGraphType;
		String srcVertexId, dstVertexId, dstVertexName;
		Logger appLogger = mAppCtx.getLogger(this, "addVertexAndEdges");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Get references to the primary id and graph type features.

		Optional<DataItem> optDataItem = aVertexDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
		{
			String errMsg = String.format("Unable to locate vertex item by feature name '%s'.", Data.FEATURE_IS_PRIMARY);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}

		DataDoc edgeSchemaDoc = mEdgeGridDS.getSchema();
		optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TYPE);
		if (optDataItem.isPresent())
			diGraphType = optDataItem.get();
		else
		{
			String errMsg = String.format("Unable to locate edge item by feature name '%s'.", Data.FEATURE_IS_GRAPH_TYPE);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}

// Add the vertex (if it does already exist) to the graph and grid.

		addVertex(aVertexDoc);

// Process each edge document in the data grid.

		int rowCount = anEdgesDataGrid.rowCount();
		if (rowCount > 0)
		{
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = anEdgesDataGrid.getRowAsDoc(row);

// Next, we need to ensure that the source and destination ids have been assigned.

				srcVertexId = dataDoc.getValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME);
				if (StringUtils.isEmpty(srcVertexId))
					dataDoc.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, diPrimaryKey.getValue());
				dstVertexId = dataDoc.getValueByName(Data.GRAPH_DST_VERTEX_ID_NAME);
				if (StringUtils.isEmpty(dstVertexId))
				{
					dstVertexName = dataDoc.getValueByName(Data.GRAPH_VERTEX_NAME);
					dstVertexId = getVertexIdByGraphTitle(diPrimaryKey, dstVertexName);
				}
				DataDoc edgeDoc = new DataDoc(mEdgeGridDS.getSchema());
				for (DataItem dataItem : dataDoc.getItems())
					edgeDoc.setValuesByName(dataItem.getName(), dataItem.getValues());
				edgeDoc.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, srcVertexId);
				edgeDoc.setValueByName(Data.GRAPH_DST_VERTEX_ID_NAME, dstVertexId);
				edgeDoc.setName(dataDoc.getValueByName(diGraphType.getName()));
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return true;
	}

	/**
	 * Updates the vertex data document in the data graph and rebuilds the
	 * grid data sources.
	 *
	 * Note: This operation focuses solely on the vertex properties
	 * and not its relationships.
	 *
	 * @param aDataDoc Vertex data document instance
	 *
	 * @return <i>true</i> if the update succeeds or <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs exception
	 */
	public boolean updateVertex(DataDoc aDataDoc)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "updateVertex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = mVertexGridDS.update(aDataDoc);
		if (! isOK)
			appLogger.error(String.format("%s: Vertex delete operation failed.", aDataDoc.getName()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Convenience method that will pre-load a <i>DataDoc</i> from
	 * the data source, apply the changes identified in the method
	 * parameter, perform the update operation and return the full
	 * version of the data document instance.
	 *
	 * Note: This operation focuses solely on the vertex properties
	 * and not its relationships.
	 *
	 * @param aDataDoc Data document instance (just items that changed)
	 *
	 * @return Data document instance reflect complete updates
	 *
	 * @throws FCException Data source related exception
	 */
	public DataDoc loadVertexApplyUpdate(DataDoc aDataDoc)
		throws FCException
	{
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadVertexApplyUpdate");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataDoc> optDataDoc = mVertexGridDS.findDataDocByPrimaryId(aDataDoc);
		if (optDataDoc.isPresent())
		{
			dataDoc = optDataDoc.get();
			for (DataItem dataItem : aDataDoc.getItems())
				dataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
			updateVertex(dataDoc);
		}
		else
			throw new DSException("Unable to isolate data document by primary id.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataDoc;
	}

	/**
	 * Updates the edge of a source vertex - the update could consist of
	 * property changes and/or edge type and/or the assignment of a
	 * different destination node.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param anEdge Data document instance representing the edge
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean updateVertexEdge(DataDoc aSrcVertex, DataDoc aDstVertex, DataDoc anEdge)
		throws FCException
	{
		boolean isOK;
		DataItem diSrcVertexPK, diDstVertexPK, diEdgePK, diEdgeGraphType;
		Logger appLogger = mAppCtx.getLogger(this, "updateVertexEdge");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Perform the operations against the grid, then rebuild the graph.

		Optional<DataItem> optDataItem = aSrcVertex.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diSrcVertexPK = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate source vertex item by feature name '%s'.", Data.FEATURE_IS_PRIMARY));
		optDataItem = aDstVertex.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diDstVertexPK = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate destination vertex item by feature name '%s'.", Data.FEATURE_IS_PRIMARY));
		optDataItem = mEdgeGridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diEdgePK = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate edge item by feature name '%s'.", Data.FEATURE_IS_PRIMARY));
		optDataItem = mEdgeGridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TYPE);
		diEdgeGraphType = optDataItem.orElse(diEdgePK);

// Assign our src and dst id and the update the edge grid.

		anEdge.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, aSrcVertex.getValueByName(diSrcVertexPK.getName()));
		anEdge.setValueByName(Data.GRAPH_DST_VERTEX_ID_NAME, aDstVertex.getValueByName(diDstVertexPK.getName()));
		anEdge.setName(diEdgeGraphType.getValue());
		isOK = mEdgeGridDS.update(anEdge);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Updates the vertex data document and its related edges (captured in the
	 * data grid parameter) and rebuilds the data structures.  This operation
	 * can be expensive due to the rebuilding of the internal data graph.
	 *
	 * @param aVertexDoc Vertex data document instance
	 * @param anEdgeDataGrid Data grid of edge documents
	 *
	 * @return <i>true</i> if the update succeeds or <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs exception
	 */
	public boolean updateVertexEdges(DataDoc aVertexDoc, DataGrid anEdgeDataGrid)
		throws FCException
	{
		boolean isOK;
		int rowCount;
		DataDoc edgeDoc;
		DataItem diVertexPK, diEdgePK, diEdgeGraphType;
		Logger appLogger = mAppCtx.getLogger(this, "updateVertexEdges");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Perform the operations against the grid, then rebuild the graph.

		Optional<DataItem> optDataItem = aVertexDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diVertexPK = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate vertex item by feature name '%s'.", Data.FEATURE_IS_PRIMARY));
		optDataItem = mEdgeGridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diEdgePK = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate edge item by feature name '%s'.", Data.FEATURE_IS_PRIMARY));
		optDataItem = mEdgeGridDS.getSchema().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TYPE);
		diEdgeGraphType = optDataItem.orElse(diEdgePK);

		Optional<DataDoc> optDataDoc = findVertexDocByNameValue(diVertexPK.getName(), diVertexPK.getValue());
		if (optDataDoc.isPresent())  // exists - remove edges, update vertex in grid
		{
			DataGrid edgesDataGrid = queryEdgesByPrimaryId(diVertexPK.getValue(), QUERY_LIMIT_DEFAULT);
			rowCount = edgesDataGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				edgeDoc = edgesDataGrid.getRowAsDoc(row);
				isOK = mEdgeGridDS.delete(edgeDoc);
				if (! isOK)
				{
					optDataItem = aVertexDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
					if (optDataItem.isPresent())
					{
						diEdgePK = optDataItem.get();
						appLogger.error(String.format("Edge grid delete operation failed for primary key '%s' = '%s'.", diEdgePK.getName(), diEdgePK.getValue()));
					}
				}
			}
			isOK = mVertexGridDS.update(aVertexDoc);
		}
		else	// does not exist, add the vertex
			isOK = mVertexGridDS.add(aVertexDoc);

		if (isOK)
		{
			rowCount = anEdgeDataGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				edgeDoc = anEdgeDataGrid.getRowAsDoc(row);
				edgeDoc.setName(edgeDoc.getValueByName(diEdgeGraphType.getName()));
				isOK = mEdgeGridDS.add(edgeDoc);
				if (! isOK)
				{
					optDataItem = aVertexDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
					if (optDataItem.isPresent())
					{
						diEdgePK = optDataItem.get();
						appLogger.error(String.format("Edge grid add operation failed for primary key '%s' = '%s'.", diEdgePK.getName(), diEdgePK.getValue()));
					}
				}
			}
		}
		else
			appLogger.error(String.format("Vertex grid add/update operation failed for primary key '%s' = '%s'.", diVertexPK.getName(), diVertexPK.getValue()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Deletes the vertex and all of its related edges from their respective
	 * data sources.
	 *
	 * @param aDataDoc Data document representing the vertex
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws DSException Data source exception
	 */
	public boolean deleteVertex(DataDoc aDataDoc)
		throws FCException
	{
		boolean isOK;
		DataDoc edgeDoc;
		DataItem diPrimaryKey;
		Logger appLogger = mAppCtx.getLogger(this, "deleteVertex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
			throw new DSException(String.format("Unable to locate vertex item by feature name '%s'", Data.FEATURE_IS_PRIMARY));

// Locate all the edges referencing the vertex, then you can remove the edges and vertex from the grids.

		DataGrid edgesDataGrid = queryEdgesByPrimaryId(diPrimaryKey.getValue(), QUERY_LIMIT_DEFAULT);
		int edgesRowCount = edgesDataGrid.rowCount();
		for (int row = 0; row < edgesRowCount; row++)
		{
			edgeDoc = edgesDataGrid.getRowAsDoc(row);
			isOK = mEdgeGridDS.delete(edgeDoc);
			if (! isOK)
				throw new DSException(String.format("[%d]: Unable to delete related edge with primary id of '%s'.",row, diPrimaryKey.getValue()));
		}
		isOK = mVertexGridDS.delete(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Deletes the edge from the data graph and grid data source.
	 *
	 * @param aDataDoc Data document representing the edge
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws DSException Data source exception
	 */
	public boolean deleteEdge(DataDoc aDataDoc)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "deleteEdge");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = mEdgeGridDS.delete(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Saves the data graph to the path name specified.  This method will generate
	 * a unified DataGrid CSV output stream for the graph vertexes and edges.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aPW Print writer output stream
	 *
	 * @throws IOException I/O related exception
	 * @throws FCException Redis Labs general exception
	 */
	public void saveToCSV(DataGraph aDataGraph, PrintWriter aPW)
		throws FCException, IOException
	{
		Optional<DataItem> optDataItem;
		DataDoc dgDataDoc, srcVertexDoc, dstVertexDoc;
		Logger appLogger = mAppCtx.getLogger(this, "saveToCSV");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Create our graph schema that we will use to store a unified data grid.

		DataDoc graphSchemaDoc = new DataDoc(String.format("%s - Schema", mName));
		graphSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name("graph_row_type").title("Row Type").build());
		graphSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name("graph_item_name").title("Item Name").build());
		graphSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name("graph_src_vertex_name").title("Source Vertex Name").build());
		graphSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name("graph_src_vertex_id").title("Source Vertex Id").build());
		graphSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name("graph_dst_vertex_name").title("Destination Vertex Name").build());
		graphSchemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name("graph_dst_vertex_id").title("Destination Vertex Id").build());

		for (DataItem dataItem : mVertexGridDS.getSchema().getItems())
		{
			optDataItem = graphSchemaDoc.getItemByNameOptional(dataItem.getName());
			if (optDataItem.isEmpty())
				graphSchemaDoc.add(new DataItem(dataItem));
		}
		for (DataItem dataItem : mEdgeGridDS.getSchema().getItems())
		{
			optDataItem = graphSchemaDoc.getItemByNameOptional(dataItem.getName());
			if (optDataItem.isEmpty())
				graphSchemaDoc.add(new DataItem(dataItem));
		}

// Next, we need to create a unified data grid based on the graph schema.

		DataGrid csvDataGrid = new DataGrid(graphSchemaDoc);
		for (DataDoc dd : aDataGraph.getVertexDocSet())
		{
			dgDataDoc = new DataDoc(graphSchemaDoc);
			dgDataDoc.setValueByName("graph_row_type", ROW_TYPE_VERTEX);
			dgDataDoc.setValueByName("graph_item_name", dd.getName());
			for (DataItem dataItem : dd.getItems())
				dgDataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
			csvDataGrid.addRow(dgDataDoc);
		}
		for (DataGraphEdge dge : aDataGraph.getEdgeSet())
		{
			dgDataDoc = new DataDoc(graphSchemaDoc);
			dgDataDoc.setValueByName("graph_row_type", ROW_TYPE_EDGE);
			dgDataDoc.setValueByName("graph_item_name", dge.getName());
			srcVertexDoc = aDataGraph.getEdgeDocSource(dge);
			dstVertexDoc = aDataGraph.getEdgeDocDestination(dge);
			dgDataDoc.setValueByName("graph_src_vertex_name", srcVertexDoc.getName());
			dgDataDoc.setValueByName("graph_src_vertex_id", srcVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY));
			dgDataDoc.setValueByName("graph_dst_vertex_name", dstVertexDoc.getName());
			dgDataDoc.setValueByName("graph_dst_vertex_id", dstVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY));
			for (DataItem dataItem : dge.getDoc().getItems())
				dgDataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
			csvDataGrid.addRow(dgDataDoc);
		}

		DataGridCSV dataGridCSV = new DataGridCSV();
		dataGridCSV.setMultiValueDelimiterChar(mDelimiterChar);
		dataGridCSV.save(csvDataGrid, aPW, true, false);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Saves the data graph to the path name specified.  This method will generate
	 * a unified DataGrid CSV output stream for the graph vertexes and edges.
	 *
	 * @param aPW Print writer output stream
	 *
	 * @throws IOException I/O related exception
	 * @throws FCException Redis Labs general exception
	 */
	public void saveToCSV(PrintWriter aPW)
		throws FCException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveToCSV");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGraph dataGraph = createDataGraph(mName);
		saveToCSV(dataGraph, aPW);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Saves the data graph to the CSV path name specified.  This method will generate
	 * a unified DataGrid CSV file for the graph vertexes and edges.
	 *
	 * @param aPathFileName Absolute path/file name
	 *
	 * @throws IOException I/O related exception
	 */
	public void saveToCSV(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveToCSV");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			saveToCSV(printWriter);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the data groph from the CSV path file name specified.  The CSV should have
	 * been saved with the "saveToCSV()" method.  This method assumes that the vertex
	 * and edge schemas have been previously defined.
	 *
	 * @param aPathFileName Data graph CSV path file name
	 *
	 * @throws IOException I/O related exception
	 * @throws FCException Redis Labs general exception
	 */
	public void loadFromCSV(String aPathFileName)
		throws IOException, FCException
	{
		boolean isOK;
		DataGraph dataGraph;
		DataItem diGraphWeight;
		Logger appLogger = mAppCtx.getLogger(this, "loadFromCSV");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc edgeSchemaDoc = mEdgeGridDS.getSchema();
		DataDoc vertexSchemaDoc = mVertexGridDS.getSchema();
		int edgeItemCount = edgeSchemaDoc.count();
		int vertexItemCount = vertexSchemaDoc.count();
		if ((edgeItemCount == 0) || (vertexItemCount == 0))
		{
			String errMsg = String.format("Graph vertex (%d) and/or edge (%d) schema are empty.",
										  vertexItemCount, edgeItemCount);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isEmpty())
		{
			String errMsg = "Graph vertex documents must have a primary item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataItem primaryItem = optDataItem.get();
		optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_WEIGHT);
		if (optDataItem.isPresent())
			dataGraph = new DataGraph(mName, Data.GraphStructure.DirectedWeightedPseudograph, Data.GraphData.DocDoc);
		else
			dataGraph = new DataGraph(mName, Data.GraphStructure.DirectedPseudograph, Data.GraphData.DocDoc);

		DataGridCSV dataGridCSV = new DataGridCSV();
		dataGridCSV.setMultiValueDelimiterChar(mDelimiterChar);
		Optional<DataGrid> optDataGrid = dataGridCSV.load(aPathFileName, true);
		if (optDataGrid.isPresent())
		{
			String rowType;
			String srcVertexId, dstVertexId;
			DataDoc dataDoc, vertexDoc, edgeDoc, srcDataDoc, dstDataDoc;

// The following logic assumes that all vertexes are defined ahead of edge definitions.

			DataGrid dataGrid = optDataGrid.get();
			int rowCount = dataGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = dataGrid.getRowAsDoc(row);
				rowType = dataDoc.getValueByName("graph_row_type");
				if (StringUtils.equals(rowType, ROW_TYPE_VERTEX))
				{
					vertexDoc = new DataDoc(vertexSchemaDoc);
					vertexDoc.setName(dataDoc.getValueByName("graph_item_name"));
					// Items not part of the document schema will be ignored
					for (DataItem dataItem : dataDoc.getItems())
						vertexDoc.setValuesByName(dataItem.getName(), dataItem.getValues());
					isOK = dataGraph.addVertex(vertexDoc);
					if (! isOK)
						appLogger.error(String.format("Vertex data graph add operation failed for id '%s'.", dataDoc.getValueByName(primaryItem.getName())));
				}
				else if (StringUtils.equals(rowType, ROW_TYPE_EDGE))
				{
					edgeDoc = new DataDoc(edgeSchemaDoc);
					// Items not part of the document schema will be ignored
					for (DataItem dataItem : dataDoc.getItems())
						edgeDoc.setValuesByName(dataItem.getName(), dataItem.getValues());
					edgeDoc.setName(dataDoc.getValueByName("graph_item_name"));
					srcVertexId = dataDoc.getValueByName("graph_src_vertex_id");
					srcDataDoc = dataGraph.getVertexDocByNameValue(primaryItem.getName(), srcVertexId);
					if (srcDataDoc == null)
						appLogger.error(String.format("Unable to locate source vertex by id '%s' - skipping record.", srcVertexId));
					else
					{
						dstVertexId = dataDoc.getValueByName("graph_dst_vertex_id");
						dstDataDoc = dataGraph.getVertexDocByNameValue(primaryItem.getName(), dstVertexId);
						if (dstDataDoc == null)
							appLogger.error(String.format("Unable to locate destination vertex by id '%s' - skipping record.", dstVertexId));
						else
						{
							edgeDoc.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, srcVertexId);
							edgeDoc.setValueByName(Data.GRAPH_DST_VERTEX_ID_NAME, dstVertexId);
							optDataItem = edgeDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_WEIGHT);
							diGraphWeight = optDataItem.orElse(null);
							try
							{
								if ((diGraphWeight != null) && (diGraphWeight.isValueAssigned()))
									isOK = dataGraph.addEdge(srcDataDoc, dstDataDoc, edgeDoc, diGraphWeight.getValueAsDouble());
								else
									isOK = dataGraph.addEdge(srcDataDoc, dstDataDoc, edgeDoc);
								if (! isOK)
									appLogger.error(String.format("Edge data graph add operation failed for source id '%s' and destination id '%s'.", srcVertexId, dstVertexId));
							}
							catch (IllegalArgumentException e)
							{
								String errMsg = String.format("[Row %d]: %s - source vertex id '%s' and destination vertex id '%s'", row+1, e.getMessage(), srcVertexId, dstVertexId);
								appLogger.error(errMsg);
								throw new DSException(errMsg);
							}
						}
					}
				}
				else
					appLogger.error(String.format("Unknown row type '%s' - skipping record.", rowType));
			}

			populateGridsFromDataGraph(dataGraph);
		}
		else
			throw new DSException(String.format("%s: Unable to load graph from CSV file.", aPathFileName));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the graph vertex and edge schemas from the specified file names followed by
	 * the loading of the data graph definitions via the CSV path file name.
	 *
	 * @param aVertexPathFileName Graph vertex schema path file name
	 * @param anEdgePathFileName Graph edge schema path file name
	 * @param aCSVPathFileName Graph CSV path file name
	 *
	 * @throws IOException I/O related exception
	 * @throws ParserConfigurationException XML parser related exception.
	 * @throws SAXException XML parser related exception.
	 * @throws FCException Redis Labs general exception
	 */
	public void loadFromCSV(String aVertexPathFileName, String anEdgePathFileName, String aCSVPathFileName)
		throws IOException, ParserConfigurationException, SAXException, FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadFromCSV");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		loadEdgeSchema(anEdgePathFileName);
		loadVertexSchema(aVertexPathFileName);
		loadFromCSV(aCSVPathFileName);

		if (mVertexSchemaDoc != null)
			mVertexGridDS.getDataGrid().setColumns(mVertexSchemaDoc);
		if (mEdgeSchemaDoc != null)
			mEdgeGridDS.getDataGrid().setColumns(mEdgeSchemaDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Create data document capturing the default configuration options for
	 * visualization of a data graph.
	 *
	 * @return Data document instance
	 */
	public DataDoc createOptionsDefault()
	{
		Logger appLogger = mAppCtx.getLogger(this, "createOptionsDefault");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc optionsDoc = new DataDoc(String.format("%s - Visualization Options", mName));
		optionsDoc.add(new DataItem.Builder().name("graph_title").title("Graph Title").value(mName).build());
		optionsDoc.add(new DataItem.Builder().name("ui_js_url").title("UI JavaScript URL").value("https://unpkg.com/vis-network/standalone/umd/vis-network.min.js").build());
		optionsDoc.add(new DataItem.Builder().name("ui_width").title("UI Width").value("780px").build());
		optionsDoc.add(new DataItem.Builder().name("ui_height").title("UI Height").value("590px").build());
		optionsDoc.add(new DataItem.Builder().name("ui_border").title("UI Border").value("1px solid lightgray").build());
		optionsDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name("is_hierarchical").title("Is Hierarchical").value(false).build());
		optionsDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name("is_matched").title("Is Matched").value(false).build());
		DataRange shapeRange = new DataRange("ellipse", "circle", "database", "box", "text", "diamond", "star", "triangle", "triangleDown", "hexagon", "square");
		optionsDoc.add(new DataItem.Builder().name("node_shape").title("Node Shape").value("ellipse").range(shapeRange).build());
		optionsDoc.add(new DataItem.Builder().name("node_color").title("Node Color").value("#97C2FC").build());
		optionsDoc.add(new DataItem.Builder().name("edge_color").title("Edge Color").value("#97C2FC").build());
		DataRange arrowRange = new DataRange("to", "from", "to;from", "middle");
		optionsDoc.add(new DataItem.Builder().name("edge_arrow").title("Edge Arrow").range(arrowRange).value("to").build());
		optionsDoc.add(new DataItem.Builder().name("match_color").title("Match Color").value("#FAA0A0").build());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optionsDoc;
	}

	private void generateVertexes(DataGraph aDataGraph, PrintWriter aPW, DataDoc anOptionsDoc)
		throws FCException
	{
		DataItem titleItem;
		Logger appLogger = mAppCtx.getLogger(this, "generateVertexes");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc vertexSchemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isEmpty())
		{
			String errMsg = "Graph vertex documents must have a primary item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataItem primaryItem = optDataItem.get();
		optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TITLE);
		titleItem = optDataItem.orElse(primaryItem);

		int lineCount = 0;
		String nodeColor = anOptionsDoc.getValueByName("node_color");
		for (DataDoc dd : aDataGraph.getVertexDocSet())
		{
			if (lineCount > 0)
				aPW.printf(",%n");
			aPW.printf("     {id: \"%s\", title: \"%s\", label: \"%s\", shape: \"%s\"", dd.getValueByName(primaryItem.getName()), dd.getName(), dd.getValueByName(titleItem.getName()), anOptionsDoc.getValueByName("node_shape"));
			if (StringUtils.isNotEmpty(nodeColor))
				aPW.printf(", color: \"%s\"}", nodeColor);
			else
				aPW.printf("}");
			lineCount++;
		}
		if (lineCount > 0)
			aPW.printf("%n");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void generateEdges(DataGraph aDataGraph, PrintWriter aPW, DataDoc anOptionsDoc)
		throws FCException
	{
		DataItem titleItem;
		String srcVertexId, dstVertexId;
		DataDoc edgeDoc, srcVertexDoc, dstVertexDoc;
		Logger appLogger = mAppCtx.getLogger(this, "generateEdges");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc edgeSchemaDoc = mEdgeGridDS.getSchema();
		Optional<DataItem> optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isEmpty())
		{
			String errMsg = "Graph edge documents must have a primary item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataItem primaryItem = optDataItem.get();
		optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TITLE);
		titleItem = optDataItem.orElse(primaryItem);

		int lineCount = 0;
		String edgeColor = anOptionsDoc.getValueByName("edge_color");
		String edgeArrow = anOptionsDoc.getValueByName("edge_arrow");
		for (DataGraphEdge dge : aDataGraph.getEdgeSet())
		{
			edgeDoc = dge.getDoc();
			srcVertexDoc = aDataGraph.getEdgeDocSource(dge);
			dstVertexDoc = aDataGraph.getEdgeDocDestination(dge);
			srcVertexId = srcVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY);
			dstVertexId = dstVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY);
			if (lineCount > 0)
				aPW.printf(",%n");
			aPW.printf("     {from: \"%s\", to: \"%s\", label: \"%s\", title: \"%s\"", srcVertexId, dstVertexId, edgeDoc.getValueByName(Data.GRAPH_EDGE_TYPE_NAME), edgeDoc.getValueByName(titleItem.getName()));
			if (StringUtils.isNotEmpty(edgeColor))
				aPW.printf(", color: \"%s\"", edgeColor);
			if (StringUtils.isNotEmpty(edgeArrow))
				aPW.printf(", arrows: \"%s\"", edgeArrow);
			aPW.printf("}");
			lineCount++;
		}
		if (lineCount > 0)
			aPW.printf("%n");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Generates an HTML document (as a string) containing the graph visualization
	 * logic. This HTML document can be rendered in any modern browser.
	 *
	 * @param aDataGraph Data graph instance
	 * @param anOptionsDoc Graph configuration options data document
	 *
	 * @return HTML document string
	 *
	 * @throws FCException Redis Labs exception
	 */
	public String visualizeToString(DataGraph aDataGraph, DataDoc anOptionsDoc)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "visualizeToString");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Generate our HTML web page that will visualize the graph

		StringWriter stringWriter = new StringWriter();
		PrintWriter pw = new PrintWriter(stringWriter);

		pw.printf("<!DOCTYPE html>%n");
		pw.printf("<html lang=\"en\">%n");
		pw.printf(" <head>%n");
		pw.printf(String.format("  <title>%s</title>%n", anOptionsDoc.getValueByName("graph_title")));
		pw.printf(String.format("  <script type=\"text/javascript\" src=\"%s\"></script>%n", anOptionsDoc.getValueByName("ui_js_url")));
		pw.printf("  <style type=\"text/css\">%n");
		pw.printf("   #rwgraph {%n");
		pw.printf(String.format("    width: %s;%n", anOptionsDoc.getValueByName("ui_width")));
		pw.printf(String.format("    height: %s;%n", anOptionsDoc.getValueByName("ui_height")));
		pw.printf(String.format("    border: %s;%n", anOptionsDoc.getValueByName("ui_border")));
		pw.printf("   }%n");
		pw.printf("  </style>%n");
		pw.printf(" </head>%n");
		pw.printf(" <body>%n");
		pw.printf("  <div id=\"rwgraph\"></div>%n");
		pw.printf("  <script type=\"text/javascript\">%n");
		pw.printf("   const width  = window.innerWidth || document.documentElement.clientWidth || document.body.clientWidth;%n");
		pw.printf("   const height = window.innerHeight|| document.documentElement.clientHeight|| document.body.clientHeight;%n");
		pw.printf("   console.log(\"Window width, height (pixels): \" + width + \", \" + height);%n");
		pw.printf("   var rwNodes = new vis.DataSet([%n");
		generateVertexes(aDataGraph, pw, anOptionsDoc);
		pw.printf("     ]);%n");
		pw.printf("   var rwEdges = new vis.DataSet([%n");
		generateEdges(aDataGraph, pw, anOptionsDoc);
		pw.printf("     ]);%n");
		pw.printf("  var rwContainer = document.getElementById(\"rwgraph\");%n");
		pw.printf("  var rwData = { nodes: rwNodes, edges: rwEdges};%n");
		if (Data.isValueTrue(anOptionsDoc.getValueByName("is_hierarchical")))
			pw.printf("  var rwOptions = {layout:{ hierarchical: true }};%n");
		else
			pw.printf("  var rwOptions = {};%n");
		pw.printf("  var rwNetwork = new vis.Network(rwContainer, rwData, rwOptions);%n");
		pw.printf("  </script>%n");
		pw.printf(" </body>%n");
		pw.printf("</html>%n");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return stringWriter.toString();
	}

	/**
	 * Generates an HTML document (as a file) containing the graph visualization
	 * logic.  This HTML file can be rendered in any modern browser.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aPathFileName Path file name of the HTML document
	 * @param anOptionsDoc Graph configuration options data document
	 *
	 * @throws IOException IO exception
	 */
	public void visualizeToFileName(String aPathFileName, DataGraph aDataGraph, DataDoc anOptionsDoc)
			throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			String htmlGraph = visualizeToString(aDataGraph, anOptionsDoc);
			printWriter.print(htmlGraph);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}

	/**
	 * Generates an HTML document (as a file) containing the graph visualization
	 * logic.  This HTML file can be rendered in any modern browser.
	 *
	 * @param aPathFileName Path file name of the HTML document
	 * @param anOptionsDoc Graph configuration options data document
	 *
	 * @throws IOException IO exception
	 * @throws FCException Foundation classes exception
	 */
	public void visualizeToFileName(String aPathFileName, DataDoc anOptionsDoc)
		throws IOException, FCException
	{
		DataGraph dataGraph = createDataGraph(mName);
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			String htmlGraph = visualizeToString(dataGraph, anOptionsDoc);
			printWriter.print(htmlGraph);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}

	private boolean isEdgeMatched(GridDS anEdgeGridDS, String aSrcPrimaryIdValue, String aDstPrimaryIdValue)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "isVertexMatched");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(String.format("%s - Criteria", anEdgeGridDS.getName()));
		dsCriteria.add(Data.GRAPH_SRC_VERTEX_ID_NAME, Data.Operator.EQUAL, aSrcPrimaryIdValue);
		dsCriteria.add(Data.GRAPH_DST_VERTEX_ID_NAME, Data.Operator.EQUAL, aDstPrimaryIdValue);
		DataGrid dataGrid = anEdgeGridDS.fetch(dsCriteria, 0, 1);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid.rowCount() > 0;
	}

	private void generateEdges(PrintWriter aPW, DataGraph aCompleteDataGraph, DataGraph aMatchedDataGraph,
							   DataDoc anOptionsDoc)
		throws FCException
	{
		DataItem titleItem;
		DataDoc edgeDoc, srcVertexDoc, dstVertexDoc;
		String srcPrimaryIdValue, dstPrimaryIdValue;
		Logger appLogger = mAppCtx.getLogger(this, "generateEdges");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		GridDS edgeGridDS = new GridDS(mAppCtx);
		DataGrid dataGrid = aMatchedDataGraph.getEdgesDataGrid();
		edgeGridDS.setName(String.format("%s - Edges", aMatchedDataGraph.getName()));
		edgeGridDS.setDatGrid(dataGrid);

		DataDoc edgeSchemaDoc = mEdgeGridDS.getSchema();
		Optional<DataItem> optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isEmpty())
		{
			String errMsg = "Graph edge documents must have a primary item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataItem primaryItem = optDataItem.get();
		optDataItem = edgeSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_GRAPH_TITLE);
		titleItem = optDataItem.orElse(primaryItem);

		int lineCount = 0;
		String edgeColor = anOptionsDoc.getValueByName("edge_color");
		if (Data.isValueTrue(anOptionsDoc.getValueByName("is_matched")))
		{
			String matchColor = anOptionsDoc.getValueByName("match_color");
			for (DataGraphEdge dge : aCompleteDataGraph.getEdgeSet())
			{
				edgeDoc = dge.getDoc();
				srcVertexDoc = aCompleteDataGraph.getEdgeDocSource(dge);
				srcPrimaryIdValue = srcVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY);
				dstVertexDoc = aCompleteDataGraph.getEdgeDocDestination(dge);
				dstPrimaryIdValue = dstVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY);
				if (lineCount > 0)
					aPW.printf(",%n");
				aPW.printf("     {from: \"%s\", to: \"%s\", label: \"%s\", title: \"%s\"", srcPrimaryIdValue, dstPrimaryIdValue, edgeDoc.getValueByName(Data.GRAPH_EDGE_TYPE_NAME), edgeDoc.getValueByName(titleItem.getName()));
				if (isEdgeMatched(edgeGridDS, srcPrimaryIdValue, dstPrimaryIdValue))
				{
					if (StringUtils.isNotEmpty(matchColor))
						aPW.printf(", color: \"%s\"", matchColor);
				}
				else
				{
					if (StringUtils.isNotEmpty(edgeColor))
						aPW.printf(", color: \"%s\"", edgeColor);
				}
				aPW.printf("}");
				lineCount++;
			}
			if (lineCount > 0)
				aPW.printf("%n");
		}
		else
		{
			String edgeArrow = anOptionsDoc.getValueByName("edge_arrow");
			for (DataGraphEdge dge : aMatchedDataGraph.getEdgeSet())
			{
				edgeDoc = dge.getDoc();
				srcVertexDoc = aMatchedDataGraph.getEdgeDocSource(dge);
				srcPrimaryIdValue = srcVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY);
				dstVertexDoc = aMatchedDataGraph.getEdgeDocDestination(dge);
				dstPrimaryIdValue = dstVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY);
				if (lineCount > 0)
					aPW.printf(",%n");
				aPW.printf("     {from: \"%s\", to: \"%s\", label: \"%s\", title: \"%s\"", srcPrimaryIdValue, dstPrimaryIdValue, edgeDoc.getValueByName(Data.GRAPH_EDGE_TYPE_NAME), edgeDoc.getValueByName(titleItem.getName()));
				if (StringUtils.isNotEmpty(edgeColor))
					aPW.printf(", color: \"%s\"", edgeColor);
				if (StringUtils.isNotEmpty(edgeArrow))
					aPW.printf(", arrows: \"%s\"", edgeArrow);
				aPW.printf("}");
				lineCount++;
			}
			if (lineCount > 0)
				aPW.printf("%n");
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private boolean isVertexMatched(GridDS aVertexGridDS, String aName, String aValue)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "isVertexMatched");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(mVertexGridDS.getName() + " Criteria");
		dsCriteria.add(aName, Data.Operator.EQUAL, aValue);
		DataGrid dataGrid = aVertexGridDS.fetch(dsCriteria, 0, 1);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid.rowCount() > 0;
	}

	private void generateVertexes(PrintWriter aPW, DataGraph aCompleteDataGraph, DataGraph aMatchedDataGraph, DataDoc anOptionsDoc)
		throws FCException
	{
		DataItem titleItem;
		String primaryName, primaryValue;
		Logger appLogger = mAppCtx.getLogger(this, "generateVertexes");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		GridDS vertexGridDS = new GridDS(mAppCtx);
		vertexGridDS.setName(String.format("%s - Vertexes", aMatchedDataGraph.getName()));
		DataGrid dataGrid = aMatchedDataGraph.getVertexDataGrid();
		vertexGridDS.setDatGrid(dataGrid);

		DataDoc vertexSchemaDoc = mVertexGridDS.getSchema();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isEmpty())
		{
			String errMsg = "Graph vertex documents must have a primary item feature assigned.";
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		DataItem primaryItem = optDataItem.get();
		primaryName = primaryItem.getName();

		int lineCount = 0;
		String nodeColor = anOptionsDoc.getValueByName("node_color");
		if (Data.isValueTrue(anOptionsDoc.getValueByName("is_matched")))
		{
			String matchColor = anOptionsDoc.getValueByName("match_color");
			for (DataDoc dd : aCompleteDataGraph.getVertexDocSet())
			{
				primaryValue = dd.getValueByName(primaryName);
				optDataItem = dd.getFirstItemByFeatureNameWithValueOptional(Data.FEATURE_IS_GRAPH_TITLE);
				titleItem = optDataItem.orElse(primaryItem);
				if (lineCount > 0)
					aPW.printf(",%n");
				aPW.printf("     {id: \"%s\", title: \"%s\", label: \"%s\", shape: \"%s\"", primaryValue, dd.getName(), dd.getValueByName(titleItem.getName()), anOptionsDoc.getValueByName("node_shape"));
				if (isVertexMatched(vertexGridDS, primaryName, primaryValue))
				{
					if (StringUtils.isNotEmpty(matchColor))
						aPW.printf(", color: \"%s\"}", matchColor);
					else
						aPW.printf("}");
				}
				else
				{
					if (StringUtils.isNotEmpty(nodeColor))
						aPW.printf(", color: \"%s\"}", nodeColor);
					else
						aPW.printf("}");
				}
				lineCount++;
			}
			if (lineCount > 0)
				aPW.printf("%n");
		}
		else
		{
			for (DataDoc dd : aMatchedDataGraph.getVertexDocSet())
			{
				primaryValue = dd.getValueByName(primaryName);
				optDataItem = dd.getFirstItemByFeatureNameWithValueOptional(Data.FEATURE_IS_GRAPH_TITLE);
				titleItem = optDataItem.orElse(primaryItem);
				if (lineCount > 0)
					aPW.printf(",%n");
				aPW.printf("     {id: \"%s\", title: \"%s\", label: \"%s\", shape: \"%s\"", primaryValue, dd.getName(), dd.getValueByName(titleItem.getName()), anOptionsDoc.getValueByName("node_shape"));
				if (StringUtils.isNotEmpty(nodeColor))
					aPW.printf(", color: \"%s\"}", nodeColor);
				else
					aPW.printf("}");
				lineCount++;
			}
			if (lineCount > 0)
				aPW.printf("%n");
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Generates an HTML document (as a string) containing the graph visualization
	 * logic. This method will highlight any portion of the graph that matches
	 * the data graph instance provided as a parameter. This HTML document can be
	 * rendered in any modern browser.
	 *
	 * @param aCompleteDataGraph Data graph instance of complete graph
	 * @param aMatchedDataGraph Data graph instance that should be matched
	 * @param anOptionsDoc Graph configuration options data document
	 *
	 * @return HTML document string
	 *
	 * @throws FCException Redis Labs exception
	 */
	public String visualizeToString(DataGraph aCompleteDataGraph, DataGraph aMatchedDataGraph, DataDoc anOptionsDoc)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "visualizeToString");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Generate our HTML web page that will visualize the graph

		anOptionsDoc.setValueByName("is_matched", true);
		StringWriter stringWriter = new StringWriter();
		PrintWriter pw = new PrintWriter(stringWriter);

		pw.printf("<!DOCTYPE html>%n");
		pw.printf("<html lang=\"en\">%n");
		pw.printf(" <head>%n");
		pw.printf(String.format("  <title>%s</title>%n", anOptionsDoc.getValueByName("graph_title")));
		pw.printf(String.format("  <script type=\"text/javascript\" src=\"%s\"></script>%n", anOptionsDoc.getValueByName("ui_js_url")));
		pw.printf("  <style type=\"text/css\">%n");
		pw.printf("   #rwgraph {%n");
		pw.printf(String.format("    width: %s;%n", anOptionsDoc.getValueByName("ui_width")));
		pw.printf(String.format("    height: %s;%n", anOptionsDoc.getValueByName("ui_height")));
		pw.printf(String.format("    border: %s;%n", anOptionsDoc.getValueByName("ui_border")));
		pw.printf("   }%n");
		pw.printf("  </style>%n");
		pw.printf(" </head>%n");
		pw.printf(" <body>%n");
		pw.printf("  <div id=\"rwgraph\"></div>%n");
		pw.printf("  <script type=\"text/javascript\">%n");
		pw.printf("   const width  = window.innerWidth || document.documentElement.clientWidth || document.body.clientWidth;%n");
		pw.printf("   const height = window.innerHeight|| document.documentElement.clientHeight|| document.body.clientHeight;%n");
		pw.printf("   console.log(\"Window width, height (pixels): \" + width + \", \" + height);%n");
		pw.printf("   var rwNodes = new vis.DataSet([%n");
		generateVertexes(pw, aCompleteDataGraph, aMatchedDataGraph, anOptionsDoc);
		pw.printf("     ]);%n");
		pw.printf("   var rwEdges = new vis.DataSet([%n");
		generateEdges(pw, aCompleteDataGraph, aMatchedDataGraph, anOptionsDoc);
		pw.printf("     ]);%n");
		pw.printf("  var rwContainer = document.getElementById(\"rwgraph\");%n");
		pw.printf("  var rwData = { nodes: rwNodes, edges: rwEdges};%n");
		if (Data.isValueTrue(anOptionsDoc.getValueByName("is_hierarchical")))
			pw.printf("  var rwOptions = {layout:{ hierarchical: true }};%n");
		else
			pw.printf("  var rwOptions = {};%n");
		pw.printf("  var rwNetwork = new vis.Network(rwContainer, rwData, rwOptions);%n");
		pw.printf("  </script>%n");
		pw.printf(" </body>%n");
		pw.printf("</html>%n");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return stringWriter.toString();
	}

	/**
	 * Generates an HTML document (as a file) containing the graph visualization
	 * logic. This method will highlight any portion of the graph that matches
	 * the data graph instance provided as a parameter. This HTML document can be
	 * rendered in any modern browser.
	 *
	 * @param aCompleteDataGraph Data graph instance of complete graph
	 * @param aMatchedDataGraph Data graph instance that should be matched
	 * @param aPathFileName Path file name for the HTML document
	 * @param anOptionsDoc Graph configuration options data document
	 *
	 * @throws IOException IO exception
	 */
	public void visualizeToFileName(DataGraph aCompleteDataGraph, DataGraph aMatchedDataGraph,
									String aPathFileName, DataDoc anOptionsDoc)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "visualizeToFileName");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		anOptionsDoc.setValueByName("is_matched", true);
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StandardCharsets.UTF_8))
		{
			String htmlGraph = visualizeToString(aCompleteDataGraph, aMatchedDataGraph, anOptionsDoc);
			printWriter.print(htmlGraph);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Add an application defined property to the data grid.
	 * <p>
	 * <b>Notes:</b>
	 * </p>
	 * <ul>
	 *     <li>The goal of the DataDoc is to strike a balance between
	 *     providing enough properties to adequately model application
	 *     related data without overloading it.</li>
	 *     <li>This method offers a mechanism to capture additional
	 *     (application specific) properties that may be needed.</li>
	 *     <li>Properties added with this method are transient and
	 *     will not be stored when saved or cloned.</li>
	 * </ul>
	 *
	 * @param aName Property name (duplicates are not supported).
	 * @param anObject Instance of an object.
	 */
	public void addProperty(String aName, Object anObject)
	{
		if (mProperties == null)
			mProperties = new HashMap<String, Object>();
		mProperties.put(aName, anObject);
	}

	/**
	 * Updates the property by name with the object instance.
	 *
	 * @param aName Name of the property
	 * @param anObject Instance of an object
	 */
	public void updateProperty(String aName, Object anObject)
	{
		if (mProperties == null)
			mProperties = new HashMap<String, Object>();
		mProperties.put(aName, anObject);
	}

	/**
	 * Removes a property from the data grid.
	 *
	 * @param aName Name of the property
	 */
	public void deleteProperty(String aName)
	{
		if (mProperties != null)
			mProperties.remove(aName);
	}

	/**
	 * Returns an Optional for an object associated with the property name.
	 *
	 * @param aName Name of the property.
	 * @return Optional instance of an object.
	 */
	public Optional<Object> getProperty(String aName)
	{
		if (mProperties == null)
			return Optional.empty();
		else
			return Optional.ofNullable(mProperties.get(aName));
	}

	/**
	 * Removes all application defined properties assigned to this data grid.
	 */
	public void clearProperties()
	{
		if (mProperties != null)
			mProperties.clear();
	}

	/**
	 * Returns the property map instance managed by the data document or <i>null</i>
	 * if empty.
	 *
	 * @return Hash map instance.
	 */
	public HashMap<String, Object> getProperties()
	{
		return mProperties;
	}
}
