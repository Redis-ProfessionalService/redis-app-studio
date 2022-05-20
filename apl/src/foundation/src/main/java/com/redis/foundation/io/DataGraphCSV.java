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

package com.redis.foundation.io;

import com.redis.foundation.data.*;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import com.redis.foundation.data.*;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The DataGraphCSV provides a collection of methods that can generate/load
 * a CSV representation of a <i>DataGraph</i> object.
 * <p>
 * This class utilizes the
 * <a href="http://supercsv.sourceforge.net">SuperCSV</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 *
 */
public class DataGraphCSV
{
	private final String ROW_TYPE_EDGE = "E";
	private final String ROW_TYPE_VERTEX = "V";
	private final String ROW_TYPE_SCHEMA = "S";

	private DataGraph mDataGraph;
	private char mDelimiterChar = StrUtl.CHAR_PIPE;
	private HashMap<String, DataDoc> mSchemaMap = new HashMap<String, DataDoc>();

	/**
	 * Default constructor
	 */
	public DataGraphCSV()
	{
	}

	/**
	 * Constructor that accepts a data graph instance.
	 *
	 * @param aDataGraph Data graph instance
	 */
	public DataGraphCSV(DataGraph aDataGraph)
	{
		mDataGraph = aDataGraph;
	}

	/**
	 * Return an instance to the internally managed data graph.
	 *
	 * @return Data graph instance.
	 */
	public DataGraph getDataGraph()
	{
		return mDataGraph;
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
	 * Identifies if the path file name is formmatted properly for a data graph.
	 *
	 * @param aPathFileName Path file name
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isPathFileNameValid(String aPathFileName)
	{
		File graphFile = new File(aPathFileName);
		if (graphFile.exists())
		{
			String fileName = FilenameUtils.getName(aPathFileName);
			if ((fileName.startsWith("graph_ver")) || (fileName.startsWith("graph_edg")))
				return fileName.endsWith(".csv");
		}
		return false;
	}

	private String graphStructureToString(DataGraph aDataGraph)
	{
		String graphStructure;

		switch (aDataGraph.getStructure())
		{
			case SimpleWeightedGraph:
				graphStructure = "swgr";
				break;
			case SimpleDirectedGraph:
				graphStructure = "sdgr";
				break;
			case SimpleDirectedWeightedGraph:
				graphStructure = "sdwg";
				break;
			case MultiGraph:
				graphStructure = "mult";
				break;
			case DirectedPseudograph:
				graphStructure = "dirp";
				break;
			default:
				graphStructure = "sigr";
				break;
		}
		return graphStructure;
	}

	private Data.GraphStructure graphStructureFromFileName(String aPathFileName)
		throws FCException
	{
		if (! isPathFileNameValid(aPathFileName))
			throw new FCException(String.format("Invalid graph file name: %s", aPathFileName));

		File graphFile = new File(aPathFileName);
		String fileName = graphFile.getName();
		// "graph_ver_sigr_di_product_catalog.csv"
		//  012345678901234567890
		String graphStructure = fileName.substring(10, 14);
		switch (graphStructure)
		{
			case "sigr":
				return Data.GraphStructure.SimpleGraph;
			case "swgr":
				return Data.GraphStructure.SimpleWeightedGraph;
			case "sdgr":
				return Data.GraphStructure.SimpleDirectedGraph;
			case "sdwg":
				return Data.GraphStructure.SimpleDirectedWeightedGraph;
			case "mult":
				return Data.GraphStructure.MultiGraph;
			case "dirp":
				return Data.GraphStructure.DirectedPseudograph;
			default:
				throw new FCException(String.format("Unknown graph structure in file name: %s", aPathFileName));
		}
	}

	private String graphDataToString(DataGraph aDataGraph)
	{
		String graphData;

		switch (aDataGraph.getDataModel())
		{
			case DocDoc:
				graphData = "dd";
				break;
			case DocItem:
				graphData = "di";
				break;
			default:
				graphData = "ii";
				break;
		}
		return graphData;
	}

	private Data.GraphData graphDataFromFileName(String aPathFileName)
		throws FCException
	{
		if (! isPathFileNameValid(aPathFileName))
			throw new FCException(String.format("Invalid graph file name: %s", aPathFileName));

		String fileName = FilenameUtils.getName(aPathFileName);
		String graphData = fileName.substring(15, 17);
		switch (graphData)
		{
			case "ii":
				return Data.GraphData.ItemItem;
			case "dd":
				return Data.GraphData.DocDoc;
			case "di":
				return Data.GraphData.DocItem;
			default:
				throw new FCException(String.format("Unknown graph structure in file name: %s", aPathFileName));
		}
	}

	private String graphNameFromFileName(String aPathFileName)
		throws FCException
	{
		if (! isPathFileNameValid(aPathFileName))
			throw new FCException(String.format("Invalid graph file name: %s", aPathFileName));

		String fileName = FilenameUtils.getName(aPathFileName);
		return fileName.substring(18, fileName.lastIndexOf('.'));
	}

	/**
	 * Utility method that creates an export/import graph vertex file name
	 * based on the structure and data layout of the graph.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aPathName Path name where file should be created
	 *
	 * @return File name (e.g. "graph_ver_sigr_di_product_catalog.csv")
	 */
	public String createVertexFileName(DataGraph aDataGraph, String aPathName)
	{
		String graphData = graphDataToString(aDataGraph);
		String graphStructure = graphStructureToString(aDataGraph);

		return String.format("%s%cgraph_ver_%s_%s_%s.csv", aPathName, File.separatorChar,
							 graphStructure, graphData, Data.titleToName(aDataGraph.getName()));
	}

	/**
	 * Utility method that creates an export/import graph edge file name
	 * based on the structure and data layout of the graph.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aPathName Path name where file should be created
	 *
	 * @return File name (e.g. "graph_edg_sigr_di_product_catalog.csv")
	 */
	public String createEdgeFileName(DataGraph aDataGraph, String aPathName)
	{
		String graphData = graphDataToString(aDataGraph);
		String graphStructure = graphStructureToString(aDataGraph);

		return String.format("%s%cgraph_edg_%s_%s_%s.csv", aPathName, File.separatorChar,
							 graphStructure, graphData, Data.titleToName(aDataGraph.getName()));
	}

	private int createGraphSchemaDocs(DataGrid aDataGrid)
	{
		DataDoc dataDoc;
		Optional<DataItem> optDataItem;

		int rowOffset = 0;	// skip the header row
		DataDoc dgSchemaDoc = aDataGrid.getColumns();
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			dataDoc = aDataGrid.getRowAsDoc(row);
			if (dataDoc.getValueByName("row_type").equals(ROW_TYPE_SCHEMA))
			{
				rowOffset = row;
				DataDoc schemaDoc = new DataDoc(dataDoc.getValueByName("label_name"));
				for (String itemName : dataDoc.getValuesByName("schema_items"))
				{
					optDataItem = dgSchemaDoc.getItemByNameOptional(itemName);
					optDataItem.ifPresent(schemaDoc::add);
				}
				mSchemaMap.put(schemaDoc.getName(), schemaDoc);
			}
			else
				break;
		}

		return rowOffset;
	}

	private DataDoc createGraphDoc(DataGrid aDataGrid, DataDoc aRowDoc, int aColOffset)
	{
		DataDoc dataDoc = null;

		String labelName = aRowDoc.getValueByName("label_name");
		if (mSchemaMap != null)
		{
			DataDoc schemaDoc = mSchemaMap.get(labelName);
			if (schemaDoc != null)
				dataDoc = new DataDoc(schemaDoc);
		}

		if (dataDoc == null)
		{
			dataDoc = new DataDoc(aDataGrid.getColumns());
			dataDoc.setName(labelName);
		}

		int colCount = aRowDoc.count();
		if (aColOffset < colCount)
		{
			for (int col = aColOffset; col < colCount; col++)
				dataDoc.add(new DataItem(aRowDoc.getItemByOffset(col)));
		}

		return dataDoc;
	}

	/**
	 * Loads an optional data graph instance from the specified file name using
	 * the header row as a schema definition if set to <i>true</i>.
	 *
	 * @param aVertexesPathFileName Path file name identifying a graph vertexes CSV file
	 * @param anEdgesPathFileName Path file name identifying a graph edges CSV file
	 *
	 * @return Optional data graph instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Redis Labs general exception
	 */
	public Optional<DataGraph> load(String aVertexesPathFileName, String anEdgesPathFileName)
		throws FCException, IOException
	{
		int rowCount;
		DataGrid dgVertexes, dgEdges;
		DataItem srcDataItem, dstDataItem;
		String srcVertexName, dstVertexName;
		DataDoc dataDoc, srcDataDoc, dstDataDoc;

		if (! isPathFileNameValid(aVertexesPathFileName))
			throw new FCException(String.format("Invalid graph vertexes file name: %s", aVertexesPathFileName));
		if (! isPathFileNameValid(anEdgesPathFileName))
			throw new FCException(String.format("Invalid graph edges file name: %s", anEdgesPathFileName));

		DataGridCSV dataGridCSV = new DataGridCSV();
		dataGridCSV.setMultiValueDelimiterChar(mDelimiterChar);
		Optional<DataGrid> optDataGrid = dataGridCSV.load(aVertexesPathFileName, true);
		if (optDataGrid.isPresent())
		{
			dgVertexes = optDataGrid.get();
			dgVertexes.setName("Data Graph Vertexes");
		}
		else
			throw new FCException(String.format("Unable to load graph vertexes CSV file name: %s", aVertexesPathFileName));

		dataGridCSV = new DataGridCSV();
		dataGridCSV.setMultiValueDelimiterChar(mDelimiterChar);
		optDataGrid = dataGridCSV.load(anEdgesPathFileName, true);
		if (optDataGrid.isPresent())
		{
			dgEdges = optDataGrid.get();
			dgEdges.setName("Data Graph Edges");
		}
		else
			throw new FCException(String.format("Unable to load graph edges CSV file name: %s", anEdgesPathFileName));

		String graphName = graphNameFromFileName(aVertexesPathFileName);
		Data.GraphData graphData = graphDataFromFileName(aVertexesPathFileName);
		Data.GraphStructure graphStructure = graphStructureFromFileName(aVertexesPathFileName);
		DataGraph dataGraph = new DataGraph(graphName, graphStructure, graphData);

// First, we will add the vertexes to the DataGraph.

		if ((graphData == Data.GraphData.DocDoc) || (graphData == Data.GraphData.DocItem))
		{
			mSchemaMap.clear();
			int rowOffset = createGraphSchemaDocs(dgVertexes);
			rowCount = dgVertexes.rowCount();
			for (int row = rowOffset+1; row < rowCount; row++)
			{
				dataDoc = dgVertexes.getRowAsDoc(row);
				dataGraph.addVertex(createGraphDoc(dgVertexes, dataDoc, 3));
			}
		}
		else
		{
			String labelName;
			Optional<DataItem> optDataItem;

			rowCount = dgVertexes.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = dgVertexes.getRowAsDoc(row);
				labelName = dataDoc.getValueByName("label_name");
				optDataItem = dataDoc.getItemByNameOptional(labelName);
				if (optDataItem.isPresent())
					dataGraph.addVertex(optDataItem.get());
			}
		}

// Next, we will add the edges to the DataGraph.

		rowCount = dgEdges.rowCount();
		if (graphData == Data.GraphData.DocDoc)
		{
			mSchemaMap.clear();
			int rowOffset = createGraphSchemaDocs(dgEdges);
			for (int row = rowOffset+1; row < rowCount; row++)
			{
				dataDoc = dgEdges.getRowAsDoc(row);
				srcVertexName = dataDoc.getValueByName("src_vertex_name");
				srcDataDoc = dataGraph.getVertexDocByName(srcVertexName);
				dstVertexName = dataDoc.getValueByName("dst_vertex_name");
				dstDataDoc = dataGraph.getVertexDocByName(dstVertexName);
				dataGraph.addEdge(srcDataDoc, dstDataDoc, createGraphDoc(dgEdges, dataDoc, 6));
			}
		}
		else if (graphData == Data.GraphData.DocItem)
		{
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = dgEdges.getRowAsDoc(row);
				srcVertexName = dataDoc.getValueByName("src_vertex_name");
				srcDataDoc = dataGraph.getVertexDocByName(srcVertexName);
				dstVertexName = dataDoc.getValueByName("dst_vertex_name");
				dstDataDoc = dataGraph.getVertexDocByName(dstVertexName);
				if (dataDoc.count() > 3)
					dataGraph.addEdge(srcDataDoc, dstDataDoc, dataDoc.getItemByOffset(3));
			}
		}
		else // Data.GraphData.ItemItem
		{
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = dgEdges.getRowAsDoc(row);
				srcVertexName = dataDoc.getValueByName("src_vertex_name");
				srcDataItem = dataGraph.getVertexItemByName(srcVertexName);
				dstVertexName = dataDoc.getValueByName("dst_vertex_name");
				dstDataItem = dataGraph.getVertexItemByName(dstVertexName);
				if (dataDoc.count() > 3)
					dataGraph.addEdge(srcDataItem, dstDataItem, dataDoc.getItemByOffset(3));
			}
		}

		return Optional.of(dataGraph);
	}

	private DataDoc createGraphVertexSchema(DataGraph aDataGraph)
		throws FCException
	{
		DataItem schemaDataItem;
		DataDoc graphVertexSchema;
		Optional<DataItem> optDataItem;

		graphVertexSchema = new DataDoc(String.format("%s - Vertexes", aDataGraph.getName()));
		graphVertexSchema.add(new DataItem.Builder().type(Data.Type.Text).name("row_type").title("Row Type").build());
		graphVertexSchema.add(new DataItem.Builder().type(Data.Type.Text).name("label_name").title("Label Name").build());
		graphVertexSchema.add(new DataItem.Builder().type(Data.Type.Text).name("schema_items").title("Schema Items").build());

		if (aDataGraph.isVertexDataDoc())
		{
			for (DataDoc dataDoc : aDataGraph.getVertexDocSet())
			{
				for (DataItem dataItem : dataDoc.getItems())
				{
					optDataItem = graphVertexSchema.getItemByNameOptional(dataItem.getName());
					if (! optDataItem.isPresent())
					{
						schemaDataItem = new DataItem(dataItem);
						schemaDataItem.clearValues();
						graphVertexSchema.add(schemaDataItem);
					}
				}
			}
		}
		else
		{
			for (DataItem dataItem : aDataGraph.getVertexItemSet())
			{
				optDataItem = graphVertexSchema.getItemByNameOptional(dataItem.getName());
				if (! optDataItem.isPresent())
				{
					schemaDataItem = new DataItem(dataItem);
					schemaDataItem.clearValues();
					graphVertexSchema.add(schemaDataItem);
				}
			}
		}

		return graphVertexSchema;
	}

	private DataDoc createGraphEdgeSchema(DataGraph aDataGraph)
		throws FCException
	{
		DataItem schemaDataItem;
		DataDoc graphEdgeSchema;
		Optional<DataItem> optDataItem;

		graphEdgeSchema = new DataDoc(String.format("%s - Edges", aDataGraph.getName()));
		graphEdgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name("row_type").title("Row Type").build());
		graphEdgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name("label_name").title("Label Name").build());
		graphEdgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name("schema_items").title("Schema Items").build());
		graphEdgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name("edge_name").title("Edge Name").build());
		graphEdgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name("src_vertex_name").title("Source Vertex Name").build());
		graphEdgeSchema.add(new DataItem.Builder().type(Data.Type.Text).name("dst_vertex_name").title("Destination Vertex Name").build());

		if (aDataGraph.isEdgeDataDoc())
		{
			for (DataGraphEdge dge : aDataGraph.getEdgeSet())
			{
				for (DataItem dataItem : dge.getDoc().getItems())
				{
					optDataItem = graphEdgeSchema.getItemByNameOptional(dataItem.getName());
					if (! optDataItem.isPresent())
					{
						schemaDataItem = new DataItem(dataItem);
						schemaDataItem.clearValues();
						graphEdgeSchema.add(schemaDataItem);
					}
				}
			}
		}
		else
		{
			for (DataGraphEdge dge : aDataGraph.getEdgeSet())
			{
				schemaDataItem = new DataItem(dge.getItem());
				schemaDataItem.clearValues();
				graphEdgeSchema.add(schemaDataItem);
			}
		}

		return graphEdgeSchema;
	}

	private DataGrid graphVertexesToDataGrid(DataGraph aDataGraph)
		throws FCException
	{
		DataDoc dataDoc, dgDataDoc;

		DataDoc graphVertexSchema = createGraphVertexSchema(aDataGraph);
		DataGrid dataGrid = new DataGrid(graphVertexSchema);

		if (aDataGraph.isVertexDataDoc())
		{
			DataItem diNames = new DataItem.Builder().name("item_names").title("Item Names").build();
			HashMap<String, DataDoc> dgVUniqueMap = aDataGraph.getVertexDocSetUnique();
			for (Map.Entry<String, DataDoc> me : dgVUniqueMap.entrySet())
			{
				dataDoc = me.getValue();
				diNames.clearValues();
				for (DataItem dataItem : dataDoc.getItems())
					diNames.addValue(dataItem.getName());
				dgDataDoc = new DataDoc(graphVertexSchema);
				dgDataDoc.setValueByName("row_type", ROW_TYPE_SCHEMA);
				dgDataDoc.setValueByName("label_name", dataDoc.getName());
				dgDataDoc.setValuesByName("schema_items", diNames.getValues());
				dataGrid.addRow(dgDataDoc);
			}

			for (DataDoc dd : aDataGraph.getVertexDocSet())
			{
				dgDataDoc = new DataDoc(graphVertexSchema);
				dgDataDoc.setValueByName("row_type", ROW_TYPE_VERTEX);
				dgDataDoc.setValueByName("label_name", dd.getName());
				for (DataItem dataItem : dd.getItems())
					dgDataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
				dataGrid.addRow(dgDataDoc);
			}
		}
		else
		{
			for (DataItem dataItem : aDataGraph.getVertexItemSet())
			{
				dgDataDoc = new DataDoc(graphVertexSchema);
				dgDataDoc.setValueByName("row_type", ROW_TYPE_VERTEX);
				dgDataDoc.setValueByName("label_name", dataItem.getName());
				dgDataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
				dataGrid.addRow(dgDataDoc);
			}
		}

		return dataGrid;
	}

	private DataGrid graphEdgesToDataGrid(DataGraph aDataGraph)
		throws FCException
	{
		DataDoc dataDoc, dgDataDoc;
		DataDoc graphEdgeSchema = createGraphEdgeSchema(aDataGraph);
		DataGrid dataGrid = new DataGrid(graphEdgeSchema);

// Generate an aggregate schema data document.

		if (aDataGraph.isEdgeDataDoc())
		{
			DataDoc srcVertex, dstVertex;

			DataItem diNames = new DataItem.Builder().name("item_names").title("Item Names").build();
			HashMap<String, DataDoc> dgVUniqueMap = aDataGraph.getEdgeDocSetUnique();
			for (Map.Entry<String, DataDoc> me : dgVUniqueMap.entrySet())
			{
				dataDoc = me.getValue();
				diNames.clearValues();
				for (DataItem dataItem : dataDoc.getItems())
					diNames.addValue(dataItem.getName());
				dgDataDoc = new DataDoc(graphEdgeSchema);
				dgDataDoc.setValueByName("row_type", ROW_TYPE_SCHEMA);
				dgDataDoc.setValueByName("label_name", dataDoc.getName());
				dgDataDoc.setValuesByName("schema_items", diNames.getValues());
				dataGrid.addRow(dgDataDoc);
			}

			for (DataGraphEdge dge : aDataGraph.getEdgeSet())
			{
				dgDataDoc = new DataDoc(graphEdgeSchema);
				dgDataDoc.setValueByName("row_type", ROW_TYPE_EDGE);
				dgDataDoc.setValueByName("label_name", dge.getName());
				srcVertex = aDataGraph.getEdgeDocSource(dge);
				dstVertex = aDataGraph.getEdgeDocDestination(dge);
				dgDataDoc.setValueByName("edge_name", dge.getName());
				dgDataDoc.setValueByName("src_vertex_name", srcVertex.getName());
				dgDataDoc.setValueByName("dst_vertex_name", dstVertex.getName());
				for (DataItem dataItem : dge.getDoc().getItems())
					dgDataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
				dataGrid.addRow(dgDataDoc);
			}
		}
		else
		{
			DataItem srcVertex, dstVertex;

			for (DataGraphEdge dge : aDataGraph.getEdgeSet())
			{
				dgDataDoc = new DataDoc(graphEdgeSchema);
				dgDataDoc.setValueByName("row_type", ROW_TYPE_EDGE);
				dgDataDoc.setValueByName("label_name", dge.getName());
				srcVertex = aDataGraph.getEdgeItemSource(dge);
				dstVertex = aDataGraph.getEdgeItemDestination(dge);
				dgDataDoc.setValueByName("edge_name", dge.getName());
				dgDataDoc.setValueByName("src_vertex_name", srcVertex.getName());
				dgDataDoc.setValueByName("dst_vertex_name", dstVertex.getName());
				dgDataDoc.setValueByName(dge.getItem().getName(), dge.getItem().getValue());
				dataGrid.addRow(dgDataDoc);
			}
		}

		return dataGrid;
	}

	/**
	 * Saves the previous assigned data grid (e.g. via constructor or set method)
	 * to the <i>PrintWriter</i> output stream.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aPW Print writer output stream
	 *
	 * @throws IOException I/O related exception
	 */
	public void save(DataGrid aDataGrid, PrintWriter aPW)
		throws IOException
	{
		DataGridCSV dataGridCSV = new DataGridCSV();
		dataGridCSV.setMultiValueDelimiterChar(mDelimiterChar);
		dataGridCSV.save(aDataGrid, aPW, true, false);
	}

	/**
	 * Saves the data graph to the path name specified.  This method will generate
	 * a separate DataGrid CSV file for the graph vertexes and edges.
	 *
	 * @param aDataGraph Data graph instance
	 * @param aPathName Absolute path name
	 *
	 * @throws IOException I/O related exception
	 * @throws FCException Redis Labs general exception
	 */
	public void save(DataGraph aDataGraph, String aPathName)
		throws FCException, IOException
	{
		DataGrid dgEdges = graphEdgesToDataGrid(aDataGraph);
		DataGrid dgVertexes = graphVertexesToDataGrid(aDataGraph);
		String edgePathFileName = createEdgeFileName(aDataGraph, aPathName);
		String vertexPathFileName = createVertexFileName(aDataGraph, aPathName);

		try (PrintWriter printWriter = new PrintWriter(edgePathFileName, StandardCharsets.UTF_8))
		{
			save(dgEdges, printWriter);
		}
		catch (Exception e)
		{
			throw new IOException(edgePathFileName + ": " + e.getMessage());
		}

		try (PrintWriter printWriter = new PrintWriter(vertexPathFileName, StandardCharsets.UTF_8))
		{
			save(dgVertexes, printWriter);
		}
		catch (Exception e)
		{
			throw new IOException(vertexPathFileName + ": " + e.getMessage());
		}
	}
}
