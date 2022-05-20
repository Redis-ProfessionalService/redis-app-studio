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

package com.redis.foundation.data;

import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.jgrapht.graph.*;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.DepthFirstIterator;

import java.util.*;
import java.util.stream.Stream;

/**
 * A DataGraph manages a collection of graph data structures and algorithms.  It offers
 * a thin abstraction over the JGraphT graph library which focuses on supporting
 * foundational Data objects such as DataItem, DataDoc and DataGrid with its vertex and
 * edge definitions.
 *
 * JGraphT allows any Data object to be used for vertex and edge types, with full type
 * safety via generics edges can be directed or undirected, weighted or unweighted
 * simple graphs, multigraphs, and pseudographs unmodifiable graphs allow modules
 * to provide “read-only” access to internal graphs listenable graphs allow external
 * listeners to track modification events live subgraph views on other graphs
 * compositions and converter views for combining and adapting graphs
 * customizable incidence and adjacency representations
 *
 * JGraphT offers powerful specialized iterators for graph traversal (DFS, BFS, etc)
 * algorithms for path finding, clique detection, isomorphism detection, coloring,
 * common ancestors, tours, connectivity, matching, cycle detection, partitions, cuts,
 * flows, centrality, spanning, and the list goes on exporters and importers for
 * popular external representations such as GraphViz live adapters to other graph
 * libraries such as JGraphX visualization and Guava Graphs
 * generators and transforms
 *
 * JGraphT is efficient and designed for performance, with near-native speed in many
 * cases adapters for memory-optimized fastutil representation sparse representations
 * for immutable graphs
 *
 * @see <a href="https://jgrapht.org/">JGraphT</a>
 * @see <a href="https://jgrapht.org/guide/UserOverview">JGraphT User Guide</a>
 * @see <a href="https://github.com/jgrapht/jgrapht">JGraphT GitHub</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataGraph
{
	protected String mName;
	protected Data.GraphData mDataModel;
	protected Data.GraphStructure mStructure;
	protected AbstractBaseGraph<DataDoc, DataGraphEdge> mGraphDocDoc;
	protected AbstractBaseGraph<DataDoc, DataGraphEdge> mGraphDocItem;
	protected AbstractBaseGraph<DataItem, DataGraphEdge> mGraphItemItem;
	protected HashMap<String, String> mFeatures = new HashMap<String, String>();

	/**
	 * Constructor that assigns a name to the graph.
	 *
	 * @param aName Name of the graph
	 */
	public DataGraph(String aName)
	{
		setName(aName);
		mDataModel = Data.GraphData.ItemItem;
		initializeGraphItemItem(Data.GraphStructure.SimpleGraph);
	}

	/**
	 * Constructor that assigns a name and structure of the graph.
	 *
	 * @param aName Name of the graph
	 * @param aGraphStructure Structure of the graph
	 */
	public DataGraph(String aName, Data.GraphStructure aGraphStructure)
	{
		setName(aName);
		mDataModel = Data.GraphData.DocDoc;
		initializeGraphDocDoc(aGraphStructure);
	}

	/**
	 * Constructor that assigns a name, structure and data model of the graph.
	 *
	 * @param aName Name of the graph
	 * @param aGraphStructure Structure of the graph
	 * @param aDataModel Graph data model
	 */
	public DataGraph(String aName, Data.GraphStructure aGraphStructure, Data.GraphData aDataModel)
	{
		setName(aName);
		mDataModel = aDataModel;
		switch (mDataModel)
		{
			case DocDoc:
				initializeGraphDocDoc(aGraphStructure);
				break;
			case DocItem:
				initializeGraphDocItem(aGraphStructure);
				break;
			case ItemItem:
				initializeGraphItemItem(aGraphStructure);
				break;
		}
	}

	/**
	 * Constructor that assigns the rows in the data grid as vertexes in the graph.
	 *
	 * @param aDataGrid Data grid instance
	 */
	public DataGraph(DataGrid aDataGrid)
	{
		setName(aDataGrid.getName());
		mDataModel = Data.GraphData.DocDoc;
		initializeGraphDocDoc(Data.GraphStructure.DirectedPseudograph);
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			try
			{
				addVertex(aDataGrid.getRowAsDoc(row));
			}
			catch (FCException ignored)
			{
			}
		}
	}

	private void initializeGraphDocDoc(Data.GraphStructure aGraphStructure)
	{
		mStructure = aGraphStructure;
		switch (mStructure)
		{
			case SimpleGraph:
				mGraphDocDoc = new SimpleGraph<>(DataGraphEdge.class);
				break;
			case SimpleWeightedGraph:
				mGraphDocDoc = new SimpleWeightedGraph<>(DataGraphEdge.class);
				break;
			case SimpleDirectedGraph:
				mGraphDocDoc = new SimpleDirectedGraph<>(DataGraphEdge.class);
				break;
			case SimpleDirectedWeightedGraph:
				mGraphDocDoc = new SimpleDirectedWeightedGraph<>(DataGraphEdge.class);
				break;
			case MultiGraph:
				mGraphDocDoc = new Multigraph<>(DataGraphEdge.class);
				break;
			case DirectedPseudograph:
				mGraphDocDoc = new DirectedPseudograph<>(DataGraphEdge.class);
				break;
			case DirectedWeightedPseudograph:
				mGraphDocDoc = new DirectedWeightedPseudograph<>(DataGraphEdge.class);
				break;
		}
	}

	private void initializeGraphDocItem(Data.GraphStructure aGraphStructure)
	{
		mStructure = aGraphStructure;
		switch (mStructure)
		{
			case SimpleGraph:
				mGraphDocItem = new SimpleGraph<>(DataGraphEdge.class);
				break;
			case SimpleWeightedGraph:
				mGraphDocItem = new SimpleWeightedGraph<>(DataGraphEdge.class);
				break;
			case SimpleDirectedGraph:
				mGraphDocItem = new SimpleDirectedGraph<>(DataGraphEdge.class);
				break;
			case SimpleDirectedWeightedGraph:
				mGraphDocItem = new SimpleDirectedWeightedGraph<>(DataGraphEdge.class);
				break;
			case MultiGraph:
				mGraphDocItem = new Multigraph<>(DataGraphEdge.class);
				break;
			case DirectedPseudograph:
				mGraphDocItem = new DirectedPseudograph<>(DataGraphEdge.class);
				break;
			case DirectedWeightedPseudograph:
				mGraphDocItem = new DirectedWeightedPseudograph<>(DataGraphEdge.class);
				break;
		}
	}

	private void initializeGraphItemItem(Data.GraphStructure aGraphStructure)
	{
		mStructure = aGraphStructure;
		switch (mStructure)
		{
			case SimpleGraph:
				mGraphItemItem = new SimpleGraph<>(DataGraphEdge.class);
				break;
			case SimpleWeightedGraph:
				mGraphItemItem = new SimpleWeightedGraph<>(DataGraphEdge.class);
				break;
			case SimpleDirectedGraph:
				mGraphItemItem = new SimpleDirectedGraph<>(DataGraphEdge.class);
				break;
			case SimpleDirectedWeightedGraph:
				mGraphItemItem = new SimpleDirectedWeightedGraph<>(DataGraphEdge.class);
				break;
			case MultiGraph:
				mGraphItemItem = new Multigraph<>(DataGraphEdge.class);
				break;
			case DirectedPseudograph:
				mGraphItemItem = new DirectedPseudograph<>(DataGraphEdge.class);
				break;
			case DirectedWeightedPseudograph:
				mGraphItemItem = new DirectedPseudograph<>(DataGraphEdge.class);
				break;
		}
	}

	public String toString()
	{
		String idName;

		if (StringUtils.isEmpty(mName))
			idName = "Data Graph";
		else
			idName = mName;
		if (mGraphDocDoc != null)
			return String.format("%s - %s", idName, mGraphDocDoc.toString());
		else if (mGraphDocItem != null)
			return String.format("%s - %s", idName, mGraphDocItem.toString());
		else if (mGraphItemItem != null)
			return String.format("%s - %s", idName, mGraphItemItem.toString());
		else
			return idName;
	}

	/**
	 * Assigns a name to the graph.
	 *
	 * @param aName Name of the graph
	 */
	public void setName(String aName)
	{
		mName = aName;
	}

	/**
	 * Retrieves the name of the graph.
	 *
	 * @return Graph name
	 */
	public String getName()
	{
		return mName;
	}

	/**
	 * Retrieves the data model of the graph.
	 *
	 * @return Graph data model
	 */
	public Data.GraphData getDataModel()
	{
		return mDataModel;
	}

	/**
	 * Retrieves the underlying structure of the graph.
	 *
	 * @return Graph structure
	 */
	public Data.GraphStructure getStructure()
	{
		return mStructure;
	}

	/**
	 * Adds the data document as a vertex to the graph.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addVertex(DataDoc aDataDoc)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.addVertex(aDataDoc);
			case DocItem:
				return mGraphDocItem.addVertex(aDataDoc);
			default:
				throw new FCException("Graph vertex data is not document.");
		}
	}

	/**
	 * Adds the data item as a vertex to the graph.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addVertex(DataItem aDataItem)
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.addVertex(aDataItem);
		else
			throw new FCException("Graph vertex data is not an item.");
	}

	/**
	 * Adds the label name as a vertex to the graph.
	 *
	 * @param aName Vertex label name
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addVertex(String aName)
		throws FCException
	{
		return addVertex(new DataItem.Builder().name(aName).build());
	}

	/**
	 * Deletes the vertex matching the data document instance from the graph.
	 *
	 * @param aVertex Data document instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean deleteVertex(DataDoc aVertex)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.removeVertex(aVertex);
			case DocItem:
				return mGraphDocItem.removeVertex(aVertex);
			default:
				throw new FCException("Graph data vertex is not a document.");
		}
	}

	/**
	 * Deletes the vertex matching the data item instance from the graph.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean deleteVertex(DataItem aDataItem)
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.removeVertex(aDataItem);
		else
			throw new FCException("Graph vertex data is not an item.");
	}

	/**
	 * Adds a data document instance as an edge with a corresponding weight value
	 * between the source and destination data document vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param aDataDoc Data document instance representing the edge
	 * @param aWeight Edge weight value
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataDoc aSrcVertex, DataDoc aDstVertex, DataDoc aDataDoc, double aWeight)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataDoc == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocDoc)
		{
			boolean isOK = mGraphDocDoc.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataDoc));
			mGraphDocDoc.setEdgeWeight(aSrcVertex, aDstVertex, aWeight);
			return isOK;
		}
		else
			throw new FCException("Graph edge data is not a document.");
	}

	/**
	 * Adds a data document instance as an edge between the source and
	 * destination data document vertexes to the graph.
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
		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataDoc == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocDoc)
			return mGraphDocDoc.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataDoc));
		else
			throw new FCException("Graph edge data is not a document.");
	}

	/**
	 * Uniquely adds a data document instance as an edge between the source and
	 * destination data document vertexes to the graph.  This method is helpful
	 * when you have a multi-edge graph and want to avoid duplication.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param aDataDoc Data document instance representing the edge
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdgeUnique(DataDoc aSrcVertex, DataDoc aDstVertex, DataDoc aDataDoc)
		throws FCException
	{
		DataDoc dgeSrcVertex, dgeDstVertex, dgeDataDoc;
		String dgeSrcVertexHash, dgeDstVertexHash, dgeDataDocHash;

		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataDoc == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocDoc)
		{
			String edgeHash = aDataDoc.generateUniqueHash(false);
			String srcVertexHash = aSrcVertex.generateUniqueHash(false);
			String dstVertexHash = aDstVertex.generateUniqueHash(false);
			Set<DataGraphEdge> edgeSet = getEdgeSet();
			for (DataGraphEdge dge : edgeSet)
			{
				dgeSrcVertex = getEdgeDocSource(dge);
				dgeSrcVertexHash = dgeSrcVertex.generateUniqueHash(false);
				dgeDstVertex = getEdgeDocDestination(dge);
				dgeDstVertexHash = dgeDstVertex.generateUniqueHash(false);
				if ((srcVertexHash.equals(dgeSrcVertexHash)) || (srcVertexHash.equals(dgeDstVertexHash)) &&
					(dstVertexHash.equals(dgeSrcVertexHash)) || (dstVertexHash.equals(dgeDstVertexHash)))
				{
					dgeDataDoc = dge.getDoc();
					dgeDataDocHash = dgeDataDoc.generateUniqueHash(false);
					if (edgeHash.equals(dgeDataDocHash))
						return false;
				}
			}
			return mGraphDocDoc.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataDoc));
		}
		else
			throw new FCException("Graph edge data is not a document.");
	}

	/**
	 * Adds an edge between the source and destination data document vertexes
	 * to the graph.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataGraphEdge addEdge(DataDoc aSrcVertex, DataDoc aDstVertex)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocDoc)
			return mGraphDocDoc.addEdge(aSrcVertex, aDstVertex);
		else
			throw new FCException("Graph edge data is not a document.");
	}

	/**
	 * Adds a data item instance as an edge with a corresponding weight value
	 * between the source and destination data document vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param aDataItem Data item instance representing the edge
	 * @param aWeight Edge weight value
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataDoc aSrcVertex, DataDoc aDstVertex, DataItem aDataItem, double aWeight)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataItem == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocItem)
		{
			boolean isOk = mGraphDocItem.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataItem));
			mGraphDocItem.setEdgeWeight(aSrcVertex, aDstVertex, aWeight);
			return isOk;
		}
		else
			throw new FCException("Graph edge data is not an item.");
	}

	/**
	 * Adds a data item instance as an edge with a corresponding weight value
	 * between the source and destination data document vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param aWeight Edge weight value
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataGraphEdge addEdge(DataDoc aSrcVertex, DataDoc aDstVertex, double aWeight)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocDoc)
		{
			DataGraphEdge dataGraphEdge = mGraphDocDoc.addEdge(aSrcVertex, aDstVertex);
			mGraphDocDoc.setEdgeWeight(dataGraphEdge, aWeight);
			return dataGraphEdge;
		}
		else if (mDataModel == Data.GraphData.DocItem)
		{
			DataGraphEdge dataGraphEdge = mGraphDocItem.addEdge(aSrcVertex, aDstVertex);
			mGraphDocDoc.setEdgeWeight(dataGraphEdge, aWeight);
			return dataGraphEdge;
		}
		else
			throw new FCException("Graph edge data is not an item.");
	}

	/**
	 * Adds a data item instance as an edge between the source and destination
	 * data document vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data document instance
	 * @param aDstVertex Destination vertex data document instance
	 * @param aDataItem Edge data item instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataDoc aSrcVertex, DataDoc aDstVertex, DataItem aDataItem)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataItem == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.DocItem)
			return mGraphDocItem.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataItem));
		else
			throw new FCException("Graph edge data is not an item.");
	}

	/**
	 * Adds a data item instance as an edge with a corresponding weight value
	 * between the source and destination data item vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data item instance
	 * @param aDstVertex Destination vertex data item instance
	 * @param aDataItem Edge data item instance
	 * @param aWeight Edge weight value
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataItem aSrcVertex, DataItem aDstVertex, DataItem aDataItem, double aWeight)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataItem == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.ItemItem)
		{
			boolean isOk = mGraphItemItem.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataItem));
			mGraphItemItem.setEdgeWeight(aSrcVertex, aDstVertex, aWeight);
			return isOk;
		}
		else
			throw new FCException("Graph vertex and edge data is not an item.");
	}

	/**
	 * Adds a data item instance as an edge with a corresponding weight value
	 * between the source and destination data item vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data item instance
	 * @param aDstVertex Destination vertex data item instance
	 * @param aWeight Edge weight value
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataItem aSrcVertex, DataItem aDstVertex, double aWeight)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.ItemItem)
		{
			String edgeName = String.format("%s-%s", aSrcVertex.getName(), aDstVertex.getName());
			DataGraphEdge dataGraphEdge = new DataGraphEdge(edgeName);
			mGraphItemItem.setEdgeWeight(dataGraphEdge, aWeight);
			return mGraphItemItem.addEdge(aSrcVertex, aDstVertex, dataGraphEdge);
		}
		else
			throw new FCException("Graph vertex and edge data is not an item.");
	}

	/**
	 * Adds a data item instance as an edge between the source and destination
	 * data item vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data item instance
	 * @param aDstVertex Destination vertex data item instance
	 * @param aDataItem Edge data item instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataItem aSrcVertex, DataItem aDstVertex, DataItem aDataItem)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null) || (aDataItem == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(aDataItem));
		else
			throw new FCException("Graph vertex and edge data is not an item.");
	}

	/**
	 * Adds an edge with a corresponding weight value between the source
	 * and destination data item vertexes to the graph.
	 *
	 * @param aSrcVertex Source vertex data item instance
	 * @param aDstVertex Destination vertex data item instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(DataItem aSrcVertex, DataItem aDstVertex)
		throws FCException
	{
		if ((aSrcVertex == null) || (aDstVertex == null))
			throw new FCException("Graph edge has null parameters.");

		if (mDataModel == Data.GraphData.ItemItem)
		{
			String edgeName = String.format("%s-%s", aSrcVertex.getName(), aDstVertex.getName());
			return mGraphItemItem.addEdge(aSrcVertex, aDstVertex, new DataGraphEdge(edgeName));
		}
		else
			throw new FCException("Graph vertex and edge data is not an item.");
	}

	/**
	 * Adds an edge between the source and destination vertexes identified by
	 * their labels to the graph.
	 *
	 * @param aSrcVertex Source vertex name
	 * @param aDstVertex Destination vertex name
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean addEdge(String aSrcVertex, String aDstVertex)
		throws FCException
	{
		if ((StringUtils.isEmpty(aSrcVertex)) || (StringUtils.isEmpty(aDstVertex)))
			throw new FCException("Graph edge has null parameters.");

		return addEdge(new DataItem.Builder().name(aSrcVertex).build(), new DataItem.Builder().name(aDstVertex).build());
	}

	/**
	 * Deletes the edge from the graph.
	 *
	 * @param aDataGraphEdge Data graph edge instance
	 *
	 * @return <i>true</i> if the operation succeeds and <i>false</i> otherwise
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public boolean deleteEdge(DataGraphEdge aDataGraphEdge)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.removeEdge(aDataGraphEdge);
			case DocItem:
				return mGraphDocItem.removeEdge(aDataGraphEdge);
			case ItemItem:
				return mGraphItemItem.removeEdge(aDataGraphEdge);
			default:
				throw new FCException("Graph edge is not a data document or item - cannot delete.");
		}
	}

	/**
	 * Identifies if the graph data model uses data documents for vertexes.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isVertexDataDoc()
	{
		switch (mDataModel)
		{
			case DocDoc:
			case DocItem:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Identifies if the graph data model uses data documents for edges.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isEdgeDataDoc()
	{
		return (mDataModel == Data.GraphData.DocDoc);
	}

	/**
	 * Identifies if the graph data model uses data items for vertexes.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isVertexDataItem()
	{
		return (mDataModel == Data.GraphData.ItemItem);
	}

	/**
	 * Identifies if the graph data model uses data items for edges.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isEdgeDataItem()
	{
		return (mDataModel == Data.GraphData.ItemItem);
	}

	/**
	 * Retrieves the set of all vertexes from the graph as data document instances.
	 *
	 * @return Set of vertexes as data documents
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Set<DataDoc> getVertexDocSet()
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.vertexSet();
			case DocItem:
				return mGraphDocItem.vertexSet();
			default:
				throw new FCException("Graph vertex data is not a document.");
		}
	}

	/**
	 * Retrieves the unique set of all vertexes from the graph as data document instances.
	 *
	 * @return Hash map of vertexes (label, data documents)
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public HashMap<String, DataDoc> getVertexDocSetUnique()
		throws FCException
	{
		String labelName;
		Set<DataDoc> ddVertexSet;

		switch (mDataModel)
		{
			case DocDoc:
				ddVertexSet = mGraphDocDoc.vertexSet();
				break;
			case DocItem:
				ddVertexSet = mGraphDocItem.vertexSet();
				break;
			default:
				throw new FCException("Graph vertex data is not a document.");
		}

		HashMap<String, DataDoc> ddVertexMapUnique = new HashMap<>();
		for (DataDoc ddv : ddVertexSet)
		{
			labelName = ddv.getName();
			ddVertexMapUnique.putIfAbsent(labelName, ddv);
		}

		return ddVertexMapUnique;
	}

	/**
	 * Generates a data grid from the vertex set of data documents
	 * and adds them to the data grid.  A union of all items will
	 * be captured in the schema definition.
	 *
	 * @return Data grid instance
	 *
	 * @throws FCException Data model mismatch
	 */
	public DataGrid getVertexDataGrid()
		throws FCException
	{
		DataDoc vertexDoc;
		String vertexlabel;
		Optional<DataItem> optDataItem;
		DataItem schemaItem, vertexItem;

		DataGrid dataGrid = new DataGrid(mName);

// First, we will create our union schema document.

		Set<DataDoc> dataDocSet = getVertexDocSet();
		DataDoc schemaDoc = new DataDoc(String.format("%s Schema", mName));
		DataItem diVertexLabel = new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_VERTEX_LABEL_NAME).title(Data.GRAPH_VERTEX_LABEL_TITLE).build();
		diVertexLabel.enableFeature(Data.FEATURE_IS_GRAPH_LABEL);
		schemaDoc.add(diVertexLabel);
		for (DataDoc nodeDoc : dataDocSet)
		{
			for (DataItem dataItem : nodeDoc.getItems())
			{
				optDataItem = schemaDoc.getItemByNameOptional(dataItem.getName());
				if (optDataItem.isEmpty())
				{
					schemaItem = new DataItem(dataItem);
					schemaItem.clearValues();
					schemaDoc.add(schemaItem);
				}
			}
		}
		if (schemaDoc.count() > 0)
			dataGrid.setColumns(schemaDoc);

// Now we can add our vertex rows and assign our vertex label range values.

		Set<String> vertexLabelSet = new HashSet<>();
		for (DataDoc nodeDoc : dataDocSet)
		{
			vertexlabel = nodeDoc.getName();
			vertexLabelSet.add(vertexlabel);
			vertexDoc = new DataDoc(nodeDoc);
			vertexItem = new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_VERTEX_LABEL_NAME).title(Data.GRAPH_VERTEX_LABEL_TITLE).value(vertexlabel).build();
			vertexItem.enableFeature(Data.FEATURE_IS_GRAPH_LABEL);
			vertexDoc.add(vertexItem);
			dataGrid.addRow(vertexDoc);
		}
		String[] rangeValues = vertexLabelSet.toArray(String[]::new);
		if (rangeValues.length > 0)
			diVertexLabel.setRange(rangeValues);

		return dataGrid;
	}

	/**
	 * Generates a data grid from the vertex set of data documents
	 * and adds them to the data grid applying the features of
	 * the schema document instance.
	 *
	 * @param aVertexSchemaDoc Vertex schema data document instance
	 *
	 * @return Data grid instance
	 *
	 * @throws FCException Data model mismatch
	 */
	public DataGrid getVertexDataGrid(DataDoc aVertexSchemaDoc)
		throws FCException
	{
		DataItem schemaDataItem;

		if ((aVertexSchemaDoc == null) || (aVertexSchemaDoc.count() == 0))
			throw new FCException("Vertex schema document is null or empty.");
		if (mDataModel != Data.GraphData.DocDoc)
			throw new FCException("Graph vertex data is not a document.");

		DataGrid dataGrid = getVertexDataGrid();
		DataDoc vertexSchemaDoc = dataGrid.getColumns();
		for (DataItem graphDataItem: vertexSchemaDoc.getItems())
		{
			schemaDataItem = aVertexSchemaDoc.getItemByName(graphDataItem.getName());
			if (schemaDataItem != null)
				graphDataItem.copyFeatures(schemaDataItem);
		}

		return dataGrid;
	}

	/**
	 * Retrieves the graph vertexes as a stream of data document instances.
	 *
	 * @return Stream of data document instances
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Stream<DataDoc> getVertexDocStream()
		throws FCException
	{
		return getVertexDocSet().stream();
	}

	/**
	 * Retrieves the vertex from the graph identified by name as a data
	 * document instance.
	 *
	 * @param aName Label name of the vertex
	 *
	 * @return Vertex data document instance
	 *
	 * @throws FCException Redis Labs internal exception
	 * @throws NoSuchElementException If the vertex cannot be matched by the name
	 */
	public DataDoc getVertexDocByName(String aName)
		throws FCException, NoSuchElementException
	{
		if (StringUtils.isEmpty(aName))
			throw new FCException("Unable to match graph vertex document by name - invalid parameters.");
		return getVertexDocStream().filter(vd -> vd.getName().equals(aName)).findAny().get();
	}

	/**
	 * Retrieves the vertex from the graph identified by its item name and
	 * value and returns a data document instance.  This is a helpful method
	 * to match vertex documents by their primary id value.
	 *
	 * @param aName Name of the vertex item
	 * @param aValue Value of vertex item to compare
	 *
	 * @return Vertex data document instance or null if it is not found
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataDoc getVertexDocByNameValue(String aName, String aValue)
		throws FCException
	{
		if ((StringUtils.isEmpty(aName)) || (StringUtils.isEmpty(aValue)))
			throw new FCException("Unable to match graph vertex document by name & value - invalid parameters.");

		Optional<DataDoc> optDataDoc = getVertexDocStream().filter(vd -> vd.getValueByName(aName).equals(aValue)).findAny();
		return optDataDoc.orElse(null);
	}

	/**
	 * Retrieves the vertex from the graph identified by matching feature name
	 * and value.
	 *
	 * @param aFeatureName Feature name
	 * @param aFeatureValue Feature value
	 *
	 * @return Matching vertex data document instance or <i>null</i>
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataDoc getVertexDocByFeatureValue(String aFeatureName, String aFeatureValue)
		throws FCException
	{
		Set<DataDoc> vertexDocSet = getVertexDocSet();
		for (DataDoc vd : vertexDocSet)
		{
			if (StringUtils.equals(vd.getFeature(aFeatureName), aFeatureValue))
				return vd;
		}

		return null;
	}

	/**
	 * Retrieves the set of all vertexes from the graph as data item instances.
	 *
	 * @return Set of vertexes as data items
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Set<DataItem> getVertexItemSet()
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.vertexSet();
		else
			throw new FCException("Graph vertex data is not an item.");
	}

	/**
	 * Retrieves the graph vertexes as a stream of data item instances.
	 *
	 * @return Stream of data item instances
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Stream<DataItem> getVertexItemStream()
		throws FCException
	{
		return getVertexItemSet().stream();
	}

	/**
	 * Retrieves the vertex from the graph identified by name as a data
	 * item instance.
	 *
	 * @param aName Label name of the vertex
	 *
	 * @return Vertex data item instance
	 *
	 * @throws FCException Foundation class exception
	 * @throws NoSuchElementException If the vertex cannot be matched by the name
	 */
	public DataItem getVertexItemByName(String aName)
		throws FCException
	{
		try
		{
			return getVertexItemStream().filter(vi -> vi.getName().equals(aName)).findAny().get();
		}
		catch (NoSuchElementException e)
		{
			throw new FCException(String.format("Vertex not found: %s", aName));
		}
	}

	/**
	 * Creates an iterator that traverses the graph in a depth-first manner.
	 *
	 * @param aVertex Starting vertex to base the iterator on.  If <i>null</i> then it will select a root vertex.
	 *
	 * @return Graph iterator for data document instances
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Iterator<DataDoc> depthFirstIterator(DataDoc aVertex)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				if (aVertex == null)
					return new DepthFirstIterator<>(mGraphDocDoc);
				else
					return new DepthFirstIterator<>(mGraphDocDoc, aVertex);
			case DocItem:
				if (aVertex == null)
					return new DepthFirstIterator<>(mGraphDocItem);
				else
					return new DepthFirstIterator<>(mGraphDocItem, aVertex);
			default:
				throw new FCException("Graph vertex is not a document.");
		}
	}

	/**
	 * Creates an iterator that traverses the graph in a depth-first manner.
	 *
	 * @param aVertex Starting vertex to base the iterator on.  If <i>null</i> then it will select a root vertex.
	 *
	 * @return Graph iterator for data item instances
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Iterator<DataItem> depthFirstIterator(DataItem aVertex)
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
		{
			if (aVertex == null)
				return new DepthFirstIterator<>(mGraphItemItem);
			else
				return new DepthFirstIterator<>(mGraphItemItem, aVertex);
		}
		else
			throw new FCException("Graph vertex data is not an item.");
	}

	/**
	 * Creates an iterator that traverses the graph in a breadth-first manner.
	 *
	 * @param aVertex Starting vertex to base the iterator on.  If <i>null</i> then it will select a root vertex.
	 *
	 * @return Graph iterator for data document instances
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Iterator<DataDoc> breadthFirstIterator(DataDoc aVertex)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				if (aVertex == null)
					return new BreadthFirstIterator<>(mGraphDocDoc, aVertex);
				else
					return new BreadthFirstIterator<>(mGraphDocDoc, aVertex);
			case DocItem:
				if (aVertex == null)
					return new BreadthFirstIterator<>(mGraphDocItem, aVertex);
				else
					return new BreadthFirstIterator<>(mGraphDocItem, aVertex);
			default:
				throw new FCException("Graph vertex is not a document.");
		}
	}

	/**
	 * Given an edge, retrieve the source vertex data document instance
	 * from the graph.
	 *
	 * @param aEdge Graph edge
	 *
	 * @return Vertex data document instance
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataDoc getEdgeDocSource(DataGraphEdge aEdge)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.getEdgeSource(aEdge);
			case DocItem:
				return mGraphDocItem.getEdgeSource(aEdge);
			default:
				throw new FCException("Graph vertex is not a document.");
		}
	}

	/**
	 * Given an edge, retrieve the destination vertex data document instance
	 * from the graph.
	 *
	 * @param aEdge Graph edge
	 *
	 * @return Vertex data document instance
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataDoc getEdgeDocDestination(DataGraphEdge aEdge)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.getEdgeTarget(aEdge);
			case DocItem:
				return mGraphDocItem.getEdgeTarget(aEdge);
			default:
				throw new FCException("Graph vertex is not a document.");
		}
	}

	/**
	 * Given an edge, retrieve the source vertex data item instance
	 * from the graph.
	 *
	 * @param aEdge Graph edge
	 *
	 * @return Vertex data item instance
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataItem getEdgeItemSource(DataGraphEdge aEdge)
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.getEdgeSource(aEdge);
		else
			throw new FCException("Graph vertex is not an item.");
	}

	/**
	 * Given an edge, retrieve the destination vertex data item instance
	 * from the graph.
	 *
	 * @param aEdge Graph edge
	 *
	 * @return Vertex data item instance
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataItem getEdgeItemDestination(DataGraphEdge aEdge)
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.getEdgeTarget(aEdge);
		else
			throw new FCException("Graph vertex is not an item.");
	}

	/**
	 * Retrieves the set of all edges from the graph.
	 *
	 * @return Set of graph edges
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Set<DataGraphEdge> getEdgeSet()
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.edgeSet();
			case DocItem:
				return mGraphDocItem.edgeSet();
			case ItemItem:
				return mGraphItemItem.edgeSet();
			default:
				throw new FCException("Graph has undefined data model.");
		}
	}

	/**
	 * Retrieves the unique set of all edges (based on type) from the graph
	 * as data document instances.
	 *
	 * @return Hash map of edges (label, data documents)
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public HashMap<String, DataDoc> getEdgeDocSetUnique()
		throws FCException
	{
		String typeName;
		DataDoc dataDoc;

		if (mDataModel != Data.GraphData.DocDoc)
			throw new FCException("Graph edge data is not a document.");

		Set<DataGraphEdge> ddEdgeSet = mGraphDocDoc.edgeSet();
		HashMap<String, DataDoc> ddVertexMapUnique = new HashMap<>();
		for (DataGraphEdge dge : ddEdgeSet)
		{
			dataDoc = dge.getDoc();
			typeName = dataDoc.getName();
			ddVertexMapUnique.putIfAbsent(typeName, dataDoc);
		}

		return ddVertexMapUnique;
	}

	/**
	 * Retrieves the graph edges as a stream.
	 *
	 * @return Stream of graph edges
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Stream<DataGraphEdge> getEdgeStream()
		throws FCException
	{
		return getEdgeSet().stream();
	}

	/**
	 * Retrieves the data edge from the graph identified by its name.
	 *
	 * @param aName Name of the edge
	 *
	 * @return Data graph edge instance
	 *
	 * @throws FCException Redis Labs internal exception
	 * @throws NoSuchElementException If the edge cannot be matched by the name
	 */
	public DataGraphEdge getDataEdgeByName(String aName)
		throws FCException, NoSuchElementException
	{
		if (StringUtils.isEmpty(aName))
			throw new FCException("Unable to match graph edge document by name - invalid parameters.");
		return getEdgeStream().filter(dge -> dge.getName().equals(aName)).findAny().get();
	}

	/**
	 * Retrieves the data edge from the graph identified by its name and
	 * value and returns a data edge instance.  This is a helpful method
	 * to match edge documents by their value.
	 *
	 * @param aName Name of the edge item
	 * @param aValue Value of edge item to compare
	 *
	 * @return Data graph edge instance
	 *
	 * @throws FCException Redis Labs internal exception
	 * @throws NoSuchElementException If the edge cannot be matched by the name
	 */
	public DataGraphEdge getEdgeDocByNameValue(String aName, String aValue)
		throws FCException, NoSuchElementException
	{
		if ((StringUtils.isEmpty(aName)) || (StringUtils.isEmpty(aValue)))
			throw new FCException("Unable to match graph vertex document by name & value - invalid parameters.");

		return getEdgeStream().filter(dge -> dge.getDoc().getValueByName(aName).equals(aValue)).findAny().get();
	}

	/**
	 * Retrieves the data edge from the graph identified by matching feature name
	 * and value.
	 *
	 * @param aFeatureName Feature name
	 * @param aFeatureValue Feature value
	 *
	 * @return Matching data graph instance or <i>null</i>
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public DataGraphEdge getEdgeDocByFeatureValue(String aFeatureName, String aFeatureValue)
		throws FCException
	{
		Set<DataGraphEdge> dataEdgeSet = getEdgeSet();
		for (DataGraphEdge dge : dataEdgeSet)
		{
			if (StringUtils.equals(dge.getDoc().getFeature(aFeatureName), aFeatureValue))
				return dge;
		}

		return null;
	}

	/**
	 * Given a vertex data document instance, retrieve all edges connected
	 * to it.
	 *
	 * @param aVertex Vertex data document instance
	 *
	 * @return Set of graph edges
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Set<DataGraphEdge> getEdgesOf(DataDoc aVertex)
		throws FCException
	{
		switch (mDataModel)
		{
			case DocDoc:
				return mGraphDocDoc.edgesOf(aVertex);
			case DocItem:
				return mGraphDocItem.edgesOf(aVertex);
			default:
				throw new FCException("Graph data vertex is not a document.");
		}
	}

	/**
	 * Given a vertex data item instance, retrieve all edges connected
	 * to it.
	 *
	 * @param aVertex Vertex data item instance
	 *
	 * @return Set of graph edges
	 *
	 * @throws FCException Redis Labs internal exception
	 */
	public Set<DataGraphEdge> getEdgesOf(DataItem aVertex)
		throws FCException
	{
		if (mDataModel == Data.GraphData.ItemItem)
			return mGraphItemItem.edgesOf(aVertex);
		else
			throw new FCException("Graph vertex is not a data item.");
	}

	/**
	 * Generates a data grid from the edge set of data documents
	 * and adds them to the data grid.  A union of all items will
	 * be captured in the schema definition.
	 *
	 * @return Data grid instance
	 *
	 * @throws FCException Data model mismatch
	 */
	public DataGrid getEdgesDataGrid()
		throws FCException
	{
		String edgeType;
		DataItem schemaItem;
		Optional<DataItem> optDataItem;
		DataDoc edgeDoc, edgeUnionDoc, srcVertexDoc, dstVertexDoc;

		if (mDataModel != Data.GraphData.DocDoc)
			throw new FCException("Graph edge data is not a document.");

		DataDoc schemaDoc = new DataDoc(String.format("%s Schema", mName));
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_SRC_VERTEX_ID_NAME).title(Data.GRAPH_SRC_VERTEX_ID_TITLE).isHidden(true).build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_DST_VERTEX_ID_NAME).title(Data.GRAPH_DST_VERTEX_ID_TITLE).isHidden(true).build());
		DataItem diEdgeType = new DataItem.Builder().type(Data.Type.Text).name(Data.GRAPH_EDGE_TYPE_NAME).title(Data.GRAPH_EDGE_TYPE_TITLE).build();
		schemaDoc.add(diEdgeType);

// First, we will create our union schema document.

		Set<DataGraphEdge> ddEdgeSet = mGraphDocDoc.edgeSet();
		for (DataGraphEdge dge : ddEdgeSet)
		{
			edgeDoc = dge.getDoc();
			for (DataItem dataItem : edgeDoc.getItems())
			{
				optDataItem = schemaDoc.getItemByNameOptional(dataItem.getName());
				if (optDataItem.isEmpty())
				{
					schemaItem = new DataItem(dataItem);
					schemaItem.clearValues();
					schemaDoc.add(schemaItem);
				}
			}
		}
		DataGrid dataGrid = new DataGrid(mName, schemaDoc);

// Now we can add our edge rows and assign our edge type range values.

		Set<String> edgeTypeSet = new HashSet<>();
		for (DataGraphEdge dge : ddEdgeSet)
		{
			edgeDoc = dge.getDoc();
			edgeType = dge.getType();
			edgeUnionDoc = new DataDoc(schemaDoc);
			srcVertexDoc = getEdgeDocSource(dge);
			edgeUnionDoc.setValueByName(Data.GRAPH_SRC_VERTEX_ID_NAME, srcVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY));
			dstVertexDoc = getEdgeDocDestination(dge);
			edgeUnionDoc.setValueByName(Data.GRAPH_DST_VERTEX_ID_NAME, dstVertexDoc.featureFirstItemValue(Data.FEATURE_IS_PRIMARY));
			edgeUnionDoc.setValueByName(Data.GRAPH_EDGE_TYPE_NAME, edgeType);
			edgeTypeSet.add(edgeType);
			for (DataItem dataItem : edgeDoc.getItems())
				edgeUnionDoc.setValuesByName(dataItem.getName(), dataItem.getValues());
			dataGrid.addRow(edgeUnionDoc);
		}
		String[] rangeValues = edgeTypeSet.toArray(String[]::new);
		if (rangeValues.length > 0)
			diEdgeType.setRange(rangeValues);

		return dataGrid;
	}

	/**
	 * Generates a data grid from the edge set of data documents
	 * and adds them to the data grid applying the features of
	 * the schema document instance.
	 *
	 * @param anEdgeSchemaDoc Edge schema data document instance
	 *
	 * @return Data grid instance
	 *
	 * @throws FCException Data model mismatch
	 */
	public DataGrid getEdgesDataGrid(DataDoc anEdgeSchemaDoc)
		throws FCException
	{
		DataItem schemaDataItem;

		if ((anEdgeSchemaDoc == null) || (anEdgeSchemaDoc.count() == 0))
			throw new FCException("Edge schema document is null or empty.");
		if (mDataModel != Data.GraphData.DocDoc)
			throw new FCException("Graph edge data is not a document.");

		DataGrid dataGrid = getEdgesDataGrid();
		DataDoc edgeSchemaDoc = dataGrid.getColumns();
		for (DataItem graphDataItem: edgeSchemaDoc.getItems())
		{
			schemaDataItem = anEdgeSchemaDoc.getItemByName(graphDataItem.getName());
			if (schemaDataItem != null)
				graphDataItem.copyFeatures(schemaDataItem);
		}

		return dataGrid;
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, String aValue)
	{
		mFeatures.put(aName, aValue);
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, int aValue)
	{
		addFeature(aName, Integer.toString(aValue));
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, long aValue)
	{
		addFeature(aName, Long.toString(aValue));
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, float aValue)
	{
		addFeature(aName, Float.toString(aValue));
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, double aValue)
	{
		addFeature(aName, Double.toString(aValue));
	}

	/**
	 * Enabling the feature will add the name and assign it a
	 * value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of the feature.
	 */
	public void enableFeature(String aName)
	{
		mFeatures.put(aName, StrUtl.STRING_TRUE);
	}

	/**
	 * Disabling a feature will remove its name and value
	 * from the internal list.
	 *
	 * @param aName Name of feature.
	 */
	public void disableFeature(String aName)
	{
		mFeatures.remove(aName);
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and assigned a value.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureAssigned(String aName)
	{
		return (getFeature(aName) != null);
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and assigned a value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureTrue(String aName)
	{
		return StrUtl.stringToBoolean(mFeatures.get(aName));
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and not assigned a value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureFalse(String aName)
	{
		return !StrUtl.stringToBoolean(mFeatures.get(aName));
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and its value matches the one provided as a
	 * parameter.
	 *
	 * @param aName Feature name.
	 * @param aValue Feature value to match.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureEqual(String aName, String aValue)
	{
		String featureValue = getFeature(aName);
		return StringUtils.equalsIgnoreCase(featureValue, aValue);
	}

	/**
	 * Count of unique features assigned to this document.
	 *
	 * @return Feature count.
	 */
	public int featureCount()
	{
		return mFeatures.size();
	}

	/**
	 * Returns the String associated with the feature name or
	 * <i>null</i> if the name could not be found.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public String getFeature(String aName)
	{
		return mFeatures.get(aName);
	}

	/**
	 * Returns the int associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public int getFeatureAsInt(String aName)
	{
		return Data.createInt(getFeature(aName));
	}

	/**
	 * Returns the long associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public long getFeatureAsLong(String aName)
	{
		return Data.createLong(getFeature(aName));
	}

	/**
	 * Returns the float associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public float getFeatureAsFloat(String aName)
	{
		return Data.createFloat(getFeature(aName));
	}

	/**
	 * Returns the double associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public double getFeatureAsDouble(String aName)
	{
		return Data.createDouble(getFeature(aName));
	}

	/**
	 * Removes all features assigned to this object instance.
	 */
	public void clearFeatures()
	{
		mFeatures.clear();
	}

	/**
	 * Assigns the hash map of features to the list.
	 *
	 * @param aFeatures Feature list.
	 */
	public void setFeatures(HashMap<String, String> aFeatures)
	{
		if (aFeatures != null)
			mFeatures = new HashMap<String, String>(aFeatures);
	}

	/**
	 * Returns a read-only copy of the internal map containing
	 * feature list.
	 *
	 * @return Internal feature map instance.
	 */
	public final HashMap<String, String> getFeatures()
	{
		return mFeatures;
	}
}

