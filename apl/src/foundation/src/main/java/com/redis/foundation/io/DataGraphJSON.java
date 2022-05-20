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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.redis.foundation.data.*;
import com.redis.foundation.data.*;
import com.redis.foundation.std.FCException;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * This I/O utility class provides methods to save/load data graph instances as
 * JSON documents.  These JSON encoding is handled by Google's GSON package.
 *
 * @see <a href="https://github.com/google/gson/blob/master/UserGuide.md">Gson User Guide</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataGraphJSON
{
	/**
	 * Default constructor.
	 */
	public DataGraphJSON()
	{
	}

	/**
	 * Writes a data graph edge as an object to a JsonWriter stream.
	 *
	 * @param aWriter Json write stream
	 * @param aDataGraph Data graph instance
	 * @param aDataGraphEdge Data graph edge instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Graph access exception
	 */
	public void save(JsonWriter aWriter, DataGraph aDataGraph, DataGraphEdge aDataGraphEdge)
		throws IOException, FCException
	{
		DataDocJSON dataDocJSON = new DataDocJSON();

		aWriter.beginObject();
		JSONUtil.writeNameValue(aWriter, "edge_name", aDataGraphEdge.getName());
		if (aDataGraph.isEdgeDataDoc())
		{
			DataDoc srcVertex = aDataGraph.getEdgeDocSource(aDataGraphEdge);
			DataDoc dstVertex = aDataGraph.getEdgeDocDestination(aDataGraphEdge);
			JSONUtil.writeNameValue(aWriter, "src_vertex_name", srcVertex.getName());
			JSONUtil.writeNameValue(aWriter, "dst_vertex_name", dstVertex.getName());
			aWriter.name("graph_document");
			dataDocJSON.save(aWriter, aDataGraphEdge.getDoc());
		}
		else
		{
			DataItem srcVertex = aDataGraph.getEdgeItemSource(aDataGraphEdge);
			DataItem dstVertex = aDataGraph.getEdgeItemDestination(aDataGraphEdge);
			JSONUtil.writeNameValue(aWriter, "src_vertex_name", srcVertex.getName());
			JSONUtil.writeNameValue(aWriter, "dst_vertex_name", dstVertex.getName());
			dataDocJSON.save(aWriter, aDataGraphEdge.getItem());
		}
		aWriter.endObject();
	}

	/**
	 * Writes a data graph as an object to a JsonWriter stream.
	 *
	 * @param aWriter Json write stream
	 * @param aDataGraph Data graph instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Graph access exception
	 */
	public void save(JsonWriter aWriter, DataGraph aDataGraph)
		throws IOException, FCException
	{
		DataDocJSON dataDocJSON = new DataDocJSON();

		aWriter.beginObject();

		JSONUtil.writeNameValue(aWriter, "graph_name", aDataGraph.getName());
		JSONUtil.writeNameValue(aWriter, "graph_structure", aDataGraph.getStructure().name());
		JSONUtil.writeNameValue(aWriter, "graph_data", aDataGraph.getDataModel().name());

		aWriter.name("graph_vertexes");
		aWriter.beginArray();
		if (aDataGraph.isVertexDataDoc())
		{
			for (DataDoc dataDoc : aDataGraph.getVertexDocSet())
				dataDocJSON.save(aWriter, dataDoc);
		}
		else
		{
			aWriter.beginObject();
			for (DataItem dataItem : aDataGraph.getVertexItemSet())
				dataDocJSON.save(aWriter, dataItem);
			aWriter.endObject();
		}
		aWriter.endArray();

		aWriter.name("graph_edges");
		aWriter.beginArray();
		for (DataGraphEdge dge : aDataGraph.getEdgeSet())
			save(aWriter, aDataGraph, dge);
		aWriter.endArray();

		aWriter.endObject();
	}

	/**
	 * Writes a data graph as an object to a JsonWriter stream.
	 *
	 * @param anOS Output stream
	 * @param aDataGraph Data graph instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Graph access exception
	 */
	public void save(OutputStream anOS, DataGraph aDataGraph)
		throws IOException, FCException
	{
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS, StandardCharsets.UTF_8);
		try (JsonWriter jsonWriter = new JsonWriter(outputStreamWriter))
		{
			jsonWriter.setIndent(" ");
			save(jsonWriter, aDataGraph);
		}
	}

	/**
	 * Writes a data graph as an object to file identified by the parameter path file name.
	 *
	 * @param aPathFileName Path file name
	 * @param aDataGraph Data graph instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Graph access exception
	 */
	public void save(String aPathFileName, DataGraph aDataGraph)
		throws IOException, FCException
	{
		try (FileOutputStream fileOutputStream = new FileOutputStream(aPathFileName))
		{
			save(fileOutputStream, aDataGraph);
		}
	}

	private DataGraph createGraph(String aName, Data.GraphStructure aStructure, Data.GraphData aData)
	{
		if ((StringUtils.isNotEmpty(aName)) && (aStructure != Data.GraphStructure.Undefined) &&
			(aData != Data.GraphData.Undefined))
			return new DataGraph(aName, aStructure, aData);
		else
			return null;
	}

	private void loadVertexes(JsonReader aReader, DataGraph aDataGraph)
		throws IOException, FCException
	{
		if (aDataGraph == null)
			throw new FCException("JSON parser: cannot load vertexes - data graph is null");

		DataDocJSON dataDocJSON = new DataDocJSON();
		aReader.beginArray();
		if (aDataGraph.isVertexDataDoc())
		{
			DataDoc dataDoc;

			int vertexId = 1;
			while (dataDocJSON.isNextTokenAnObject(aReader))
			{
				dataDoc = new DataDoc(String.format("Vertex %d", vertexId++));
				dataDocJSON.load(aReader, dataDoc);
				aDataGraph.addVertex(dataDoc);
			}
		}
		else
		{
			DataItem dataItem;
			String jsonName, jsonTitle;

			aReader.beginObject();
			JsonToken jsonToken = aReader.peek();
			while (jsonToken == JsonToken.NAME)
			{
				jsonName = aReader.nextName();
				jsonTitle = Data.nameToTitle(jsonName);
				jsonToken = aReader.peek();
				switch (jsonToken)
				{
					case BOOLEAN:
						dataItem = dataDocJSON.createByTokenType(aReader, jsonToken, jsonName, jsonTitle);
						aDataGraph.addVertex(dataItem);
						break;
					case NUMBER:
						dataItem = dataDocJSON.createByTokenType(aReader, jsonToken, jsonName, jsonTitle);
						aDataGraph.addVertex(dataItem);
						break;
					case STRING:
						dataItem = dataDocJSON.createByTokenType(aReader, jsonToken, jsonName, jsonTitle);
						aDataGraph.addVertex(dataItem);
						break;
					case NULL:
						aReader.nextNull();
						break;
					case BEGIN_ARRAY:
						aReader.beginArray();
						dataItem = new DataItem(jsonName, jsonTitle);
						jsonToken = aReader.peek();
						while (jsonToken != JsonToken.END_ARRAY)
						{
							dataDocJSON.assignValueByTokenType(aReader, jsonToken, dataItem);
							jsonToken = aReader.peek();
						}
						aReader.endArray();
						aDataGraph.addVertex(dataItem);
						break;
					default:
						aReader.skipValue();
						break;
				}
				jsonToken = aReader.peek();
			}
			aReader.endObject();
		}
		aReader.endArray();
	}

	private void loadEdges(JsonReader aReader, DataGraph aDataGraph)
		throws IOException, FCException
	{
		DataDoc dataDoc;

		if (aDataGraph == null)
			throw new FCException("JSON parser: cannot load edges - data graph is null");

		DataDocJSON dataDocJSON = new DataDocJSON();
		aReader.beginArray();

		int edgeId = 1;
		while (dataDocJSON.isNextTokenAnObject(aReader))
		{
			dataDoc = new DataDoc(String.format("Edge %d", edgeId++));
			dataDocJSON.load(aReader, dataDoc);
			if ((aDataGraph.isEdgeDataDoc()) && (dataDoc.childrenCount() ==1))
			{
				DataDoc srcDataDoc, dstDataDoc, edgeDataDoc;

				srcDataDoc = aDataGraph.getVertexDocByName(dataDoc.getValueByName("src_vertex_name"));
				dstDataDoc = aDataGraph.getVertexDocByName(dataDoc.getValueByName("dst_vertex_name"));
				edgeDataDoc = dataDoc.getChildDocsAsList().get(0);
				aDataGraph.addEdge(srcDataDoc, dstDataDoc, edgeDataDoc);
			}
			else if ((aDataGraph.isEdgeDataItem()) && (dataDoc.count() == 4))
			{
				DataItem edgeDataItem;
				DataItem srcDataItem, dstDataItem;

				srcDataItem = aDataGraph.getVertexItemByName(dataDoc.getValueByName("src_vertex_name"));
				dstDataItem = aDataGraph.getVertexItemByName(dataDoc.getValueByName("dst_vertex_name"));
				edgeDataItem = dataDoc.getItemByOffset(3);
				aDataGraph.addEdge(srcDataItem, dstDataItem, edgeDataItem);
			}
			else
				throw new FCException("JSON parser: improperly formatted edge object");
		}

		aReader.endArray();
	}

	/**
	 * Loads a JSON object from the JsonReader stream into an optional data graph instance.
	 *
	 * @param anIS Input stream
	 *
	 * @return Optional data graph instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Graph access exception
	 */
	public Optional<DataGraph> load(InputStream anIS)
		throws IOException, FCException
	{
		String jsonName;
		String graphName = StringUtils.EMPTY;
		Data.GraphData graphData = Data.GraphData.Undefined;
		Data.GraphStructure graphStructure = Data.GraphStructure.Undefined;
		InputStreamReader inputStreamReader = new InputStreamReader(anIS);
		JsonReader jsonReader = new JsonReader(inputStreamReader);

		DataGraph dataGraph = null;
		jsonReader.beginObject();
		JsonToken jsonToken = jsonReader.peek();
		while (jsonToken == JsonToken.NAME)
		{
			jsonName = jsonReader.nextName();
			switch (jsonName)
			{
				case "graph_name":
					graphName = jsonReader.nextString();
					dataGraph = createGraph(graphName, graphStructure, graphData);
					break;
				case "graph_structure":
					graphStructure = Data.GraphStructure.valueOf(jsonReader.nextString());
					dataGraph = createGraph(graphName, graphStructure, graphData);
					break;
				case "graph_data":
					graphData = Data.GraphData.valueOf(jsonReader.nextString());
					dataGraph = createGraph(graphName, graphStructure, graphData);
					break;
				case "graph_vertexes":
					loadVertexes(jsonReader, dataGraph);
					break;
				case "graph_edges":
					loadEdges(jsonReader, dataGraph);
					break;
				default:
					jsonReader.skipValue();
					throw new FCException(String.format("Unknown JSON field name: %s", jsonName));
			}
			jsonToken = jsonReader.peek();
		}
		jsonReader.endObject();

		return Optional.ofNullable(dataGraph);
	}

	/**
	 * Loads a JSON object from the path/file name into an optional data graph instance.
	 *
	 * @param aPathFileName Path file name
	 *
	 * @return Optional data graph instance
	 *
	 * @throws IOException I/O exception
	 * @throws FCException Graph access exception
	 */
	public Optional<DataGraph> load(String aPathFileName)
		throws IOException, FCException
	{
		Optional<DataGraph> optDataGraph;

		File jsonFile = new File(aPathFileName);
		if (! jsonFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
		{
			optDataGraph = load(fileInputStream);
		}

		return optDataGraph;
	}
}
