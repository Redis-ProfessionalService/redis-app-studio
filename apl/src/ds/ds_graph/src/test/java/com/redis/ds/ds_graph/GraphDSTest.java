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
import com.redis.foundation.io.DataGridConsole;
import com.redis.foundation.std.FCException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Optional;

/**
 * The ClassLibraryTest class will exercise classes and methods for
 * the library package.
 */
public class GraphDSTest
{
    private final String GRAPH_DB_NAME = "IMDb Graph";
    private final String GDS_PROPERTY_PREFIX = "gds";

    private AppCtx mAppCtx;

    @Before
    public void setup()
    {
        HashMap<String,Object> hmProperties = new HashMap<>();
        hmProperties.put(GDS_PROPERTY_PREFIX + ".host_name", "localhost");
        hmProperties.put(GDS_PROPERTY_PREFIX + ".port_number", 4455);
        hmProperties.put(GDS_PROPERTY_PREFIX + ".application_name", "GraphDS");
        hmProperties.put(GDS_PROPERTY_PREFIX + ".database_id", 0);
        hmProperties.put(GDS_PROPERTY_PREFIX + ".operation_timeout", 60);
        hmProperties.put(GDS_PROPERTY_PREFIX + ".encrypt_password", "1c518a1e-be3b-4ff0-8478-f319b887dca0");
        mAppCtx = new AppCtx(hmProperties);
    }

    private void exerciseGraphDS()
        throws FCException, IOException
    {
        DataDoc rowDoc;
        String graphSchemaHTMLPathFileName = "data/movie_graph_schema.html";

        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(false);
        PrintWriter printWriter = new PrintWriter(System.out, true);

        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph);

        GraphDS graphDS = new GraphDS(mAppCtx, dataGraph);
        DataGrid vertexDataGrid = graphDS.getVertexGridDS().getDataGrid();
        dataGridConsole.write(vertexDataGrid, printWriter, "Memory Vertexes Data Grid", 40, 1);

        DataGrid edgeDataGrid = graphDS.getEdgeGridDS().getDataGrid();
        dataGridConsole.write(edgeDataGrid, printWriter, "Memory Edges Data Grid", 40, 1);

        DataGrid dataGrid = graphDS.queryEdgesByPrimaryId("tt0068646", 10);
        Assert.assertEquals(7, dataGrid.rowCount());
        dataGridConsole.write(dataGrid, printWriter, "Edges By 'tt0068646' Data Grid", 40, 1);

        dataGrid = graphDS.queryEdgesByPrimaryId("nm0000380", 10);
        Assert.assertEquals(5, dataGrid.rowCount());
        dataGridConsole.write(dataGrid, printWriter, "Edges By 'nm0000380' Data Grid", 40, 1);

        DataGrid schemaGrid = graphDS.vertexSchemaDocToDataGrid(graphDS.getVertexGridDS().getSchema(), false);
        dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName() + " Before Update");
        int rowCount = schemaGrid.rowCount();
        for (int row = 0; row < rowCount; row++)
        {
            rowDoc = schemaGrid.getRowAsDoc(row);
            if (rowDoc.getValueByName("item_name").equals("common_name"))
            {
                rowDoc.setValueByName(Data.FEATURE_IS_GRAPH_TITLE, false);
                Assert.assertTrue(graphDS.updateVertexSchema(rowDoc));
            }
            else if (rowDoc.getValueByName("item_name").equals("movie_genres"))
            {
                rowDoc.setValueByName(Data.FEATURE_IS_GRAPH_TITLE, true);
                Assert.assertTrue(graphDS.updateVertexSchema(rowDoc));
            }
        }
        schemaGrid = graphDS.vertexSchemaDocToDataGrid(graphDS.getVertexGridDS().getSchema(), false);
        dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName() + " After Update");

        schemaGrid = graphDS.edgeSchemaDocToDataGrid(graphDS.getEdgeGridDS().getSchema(), false);
        dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName() + " Before Update");
        rowCount = schemaGrid.rowCount();
        for (int row = 0; row < rowCount; row++)
        {
            rowDoc = schemaGrid.getRowAsDoc(row);
            if (rowDoc.getValueByName("item_name").equals("contribution"))
            {
                rowDoc.setValueByName(Data.FEATURE_IS_GRAPH_TITLE, false);
                Assert.assertTrue(graphDS.updateEdgeSchema(rowDoc));
            }
            else if (rowDoc.getValueByName("item_name").equals("character_name"))
            {
                rowDoc.setValueByName(Data.FEATURE_IS_GRAPH_TITLE, true);
                Assert.assertTrue(graphDS.updateEdgeSchema(rowDoc));
            }
        }
        schemaGrid = graphDS.edgeSchemaDocToDataGrid(graphDS.getEdgeGridDS().getSchema(), false);
        dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName() + " After Update");

        DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
        graphDS.visualizeToFileName(graphSchemaHTMLPathFileName, graphOptionsDoc);
        graphOptionsDoc.count();
    }

    private void exerciseAddUpdateDelete()
        throws FCException, IOException
    {
        String graphAddedHTMLPathFileName = "data/movie_graph_added.html";
        String graphUpdatedHTMLPathFileName = "data/movie_graph_updated.html";
        String graphDeletedHTMLPathFileName = "data/movie_graph_deleted.html";

        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph);

        GraphDS graphDS = new GraphDS(mAppCtx, dataGraph);
        DataDoc ddMovieVertex = imdbGraph.createMovieVertex("mv10000", "Star Trek", "Science fiction movie franchise.",
                                                            "1975", "Science Fiction", 250000000.0);
        Assert.assertTrue(graphDS.addVertex(ddMovieVertex));

        DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
        graphDS.visualizeToFileName(graphAddedHTMLPathFileName, graphOptionsDoc);

        ddMovieVertex.setValueByName("common_name", "Star Trek The Movie");

        Assert.assertTrue(graphDS.updateVertex(ddMovieVertex));
        graphDS.visualizeToFileName(graphUpdatedHTMLPathFileName, graphOptionsDoc);

        DataDoc ddRoleEdge = imdbGraph.createRoleEdge("pr000009", "Actor", "2002", "Co-Lead", "Boss Spearman");
        Assert.assertTrue(graphDS.deleteEdge(ddRoleEdge));
        graphDS.visualizeToFileName(graphDeletedHTMLPathFileName, graphOptionsDoc);
    }

    private void saveDataGraphToCSV()
        throws FCException, IOException
    {
        String graphPathFileName = "data/graph_movies_data.csv";
        String edgeSchemaPathFileName = "data/graph_movies_edge_schema.xml";
        String vertexSchemaPathFileName = "data/graph_movies_vertex_schema.xml";

        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph);

        GraphDS graphDS = new GraphDS(mAppCtx, dataGraph);
        graphDS.saveVertexSchema(vertexSchemaPathFileName);
        graphDS.saveEdgeExtendedSchema(edgeSchemaPathFileName, false);
        graphDS.saveToCSV(graphPathFileName);
    }

    private void loadDataGraphFromCSV()
        throws FCException, IOException, ParserConfigurationException, SAXException
    {
        String graphPathFileName = "data/graph_movies_data.csv";
        String edgeSchemaPathFileName = "data/graph_movies_edge_schema.xml";
        String vertexSchemaPathFileName = "data/graph_movies_vertex_schema.xml";

        GraphDS graphDS = new GraphDS(mAppCtx, GRAPH_DB_NAME);
        graphDS.loadFromCSV(vertexSchemaPathFileName, edgeSchemaPathFileName, graphPathFileName);

        DataGraph dataGraph = graphDS.createDataGraph(GRAPH_DB_NAME);
        Optional<DataDoc> optDataDoc = graphDS.queryVertexByPrimaryId(dataGraph, "nm0000125");
        Assert.assertTrue(optDataDoc.isPresent());

        GridDS gridDS = new GridDS(mAppCtx, graphDS.createEdgeExtendedSchema(false));
        gridDS.saveSmartClient("data");
        graphDS.getVertexGridDS().saveSmartClient("data");

        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(false);
        PrintWriter printWriter = new PrintWriter(System.out, true);

        DataGrid vertexDataGrid = graphDS.getVertexGridDS().getDataGrid();
        dataGridConsole.write(vertexDataGrid, printWriter, "CSV Vertexes Data Grid", 40, 1);

        DataGrid edgeDataGrid = graphDS.getEdgeGridDS().getDataGrid();
        dataGridConsole.write(edgeDataGrid, printWriter, "CSV Edges Data Grid", 40, 1);
    }

    private void exerciseFileIO()
        throws FCException, IOException, ParserConfigurationException, SAXException
    {
        saveDataGraphToCSV();
        loadDataGraphFromCSV();
    }

    public void exerciseSimpleVisualization()
        throws FCException, IOException, ParserConfigurationException, SAXException
    {
        String graphHTMLPathFileName = "data/movie_graph.html";
        String graphPathFileName = "data/graph_movies_data.csv";
        String edgeSchemaPathFileName = "data/graph_movies_edge_schema.xml";
        String vertexSchemaPathFileName = "data/graph_movies_vertex_schema.xml";

        GraphDS graphDS = new GraphDS(mAppCtx, GRAPH_DB_NAME);
        graphDS.loadFromCSV(vertexSchemaPathFileName, edgeSchemaPathFileName, graphPathFileName);
        DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
        graphDS.visualizeToFileName(graphHTMLPathFileName, graphOptionsDoc);
    }

    public void exerciseMatchedUpdatedDeletedVisualization()
        throws FCException, IOException, ParserConfigurationException, SAXException
    {
        String graphPathFileName = "data/graph_movies_data.csv";
        String edgeSchemaPathFileName = "data/graph_movies_edge_schema.xml";
        String vertexSchemaPathFileName = "data/graph_movies_vertex_schema.xml";
        String graphMatchedHTMLPathFileName = "data/movie_graph_matched.html";
        String graphUpdatedHTMLPathFileName = "data/movie_graph_updated.html";
        String graphDeletedHTMLPathFileName = "data/movie_graph_deleted.html";
        String graphHierarchicalHTMLPathFileName = "data/movie_graph_hierarchical.html";

        GraphDS graphDS = new GraphDS(mAppCtx, "Movie Graph");
        graphDS.loadFromCSV(vertexSchemaPathFileName, edgeSchemaPathFileName, graphPathFileName);
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph matchedMovieGraph = imdbGraph.createMatchedMovieGraph(true);
        Assert.assertNotNull(matchedMovieGraph);

        DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
        graphOptionsDoc.setValueByName("is_hierarchical", true);
        graphDS.visualizeToFileName(graphHierarchicalHTMLPathFileName, graphOptionsDoc);

        graphOptionsDoc = graphDS.createOptionsDefault();
        graphOptionsDoc.setValueByName("is_matched", true);
        DataGraph completeDataGraph = graphDS.createDataGraph(GRAPH_DB_NAME);
        graphDS.visualizeToFileName(completeDataGraph, matchedMovieGraph, graphMatchedHTMLPathFileName, graphOptionsDoc);

        graphOptionsDoc = graphDS.createOptionsDefault();
        DataDoc vertexDoc = graphDS.vertexRowAsDoc(6);
        vertexDoc.setValueByName(imdbGraph.fieldName("name"), "Mr. Robert Duvall");
        DataGrid edgesDataGrid = graphDS.queryEdgesByPrimaryId(vertexDoc.getValueByName(imdbGraph.fieldName("id")), 10);
        int rowCount = edgesDataGrid.rowCount();
        for (int row = 0; row < rowCount; row++)
            edgesDataGrid.setValueByRowName(row, "employment_year", 2021);
        Assert.assertTrue(graphDS.updateVertexEdges(vertexDoc, edgesDataGrid));
        graphDS.visualizeToFileName(graphUpdatedHTMLPathFileName, graphOptionsDoc);

        vertexDoc = graphDS.vertexRowAsDoc(0);
        Assert.assertTrue(graphDS.deleteVertex(vertexDoc));
        graphDS.visualizeToFileName(graphDeletedHTMLPathFileName, graphOptionsDoc);
    }

    public void exerciseAnalysis()
        throws FCException, IOException, ParserConfigurationException, SAXException
    {
        String graphPathFileName = "data/graph_movies_data.csv";
        String edgeSchemaPathFileName = "data/graph_movies_edge_schema.xml";
        String vertexSchemaPathFileName = "data/graph_movies_vertex_schema.xml";

        GraphDS graphDS = new GraphDS(mAppCtx, "Movie Graph");
        graphDS.loadFromCSV(vertexSchemaPathFileName, edgeSchemaPathFileName, graphPathFileName);
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph matchedMovieGraph = imdbGraph.createMatchedMovieGraph(true);
        Assert.assertNotNull(matchedMovieGraph);

        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(false);
        PrintWriter printWriter = new PrintWriter(System.out, true);

        DataGrid vertexDataGrid = graphDS.getVertexGridDS().analyze(10);
        dataGridConsole.write(vertexDataGrid, printWriter, "Analyze Vertexes Data Grid", 40, 1);

        DataGrid edgeDataGrid = graphDS.getEdgeGridDS().analyze(10);
        dataGridConsole.write(edgeDataGrid, printWriter, "Analyze Edges Data Grid", 40, 1);
    }

    @Test
    public void exercise()
        throws FCException, IOException, ParserConfigurationException, SAXException
    {
        exerciseAddUpdateDelete();
        exerciseGraphDS();
        exerciseFileIO();
        exerciseSimpleVisualization();
        exerciseMatchedUpdatedDeletedVisualization();
        exerciseAnalysis();
    }

    @After
    public void cleanup()
    {
    }
}
