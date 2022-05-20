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

package com.redis.ds.ds_redis;

import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_redis.graph.RedisGraphs;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.*;
import com.redis.foundation.io.DataGraphCSV;
import com.redis.foundation.io.DataGridConsole;
import com.redis.foundation.std.StrUtl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Optional;

/**
 * The RedisGraphTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisGraphTest
{
    private final String GRAPH_DB_NAME = "IMDb Graph";
    private final String APPLICATION_PREFIX_DEFAULT = "ASRG";

    private AppCtx mAppCtx;
    private RedisDS mRedisDS;

    @Before
    public void setup()
    {
        HashMap<String,Object> hmProperties = new HashMap<>();
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".host_name", Redis.HOST_NAME_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".port_number", Redis.PORT_NUMBER_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".ssl_enabled", StrUtl.STRING_FALSE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".application_prefix", APPLICATION_PREFIX_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".database_id", Redis.DBID_DEFAULT);
//		hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".database_account", "redis-service-account");
//		hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".database_password", "secret");
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_max_connections", Redis.POOL_MAX_TOTAL_CONNECTIONS);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_min_idle_connections", Redis.POOL_MIN_IDLE_CONNECTIONS);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_max_idle_connections", Redis.POOL_MAX_IDLE_CONNECTIONS);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_test_on_idle", StrUtl.STRING_TRUE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_test_on_borrow", StrUtl.STRING_TRUE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_test_on_return", StrUtl.STRING_TRUE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".pool_block_on_limit", StrUtl.STRING_TRUE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".operation_timeout", Redis.TIMEOUT_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".cache_expiration_time", 0);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".stream_command_limit", Redis.STREAM_LIMIT_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".encrypt_all_values", StrUtl.STRING_FALSE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".encrypt_password", "1c518a1e-be3b-4ff0-8478-f319b887dca0");
        mAppCtx = new AppCtx(hmProperties);
        mRedisDS = new RedisDS(mAppCtx);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public String createLogPathFile()
    {
        String logPathFileName = String.format("log%credis_graph_commands.txt", File.separatorChar);

        File logFile = new File (logPathFileName);
        if (logFile.exists())
            logFile.delete();

        return logPathFileName;
    }

    public void exercise_csv_save()
    {
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            DataGraphCSV dataGraphCSV = new DataGraphCSV(dataGraph1);
            dataGraphCSV.save(dataGraph1, "data");
            dataGraphCSV = new DataGraphCSV();
            String graphEdgesFileName = "data/graph_edg_dirp_dd_imdb_graph.csv";
            String graphVertexesFileName = "data/graph_ver_dirp_dd_imdb_graph.csv";
            Optional<DataGraph> optDataGraph = dataGraphCSV.load(graphVertexesFileName, graphEdgesFileName);
            Assert.assertTrue(optDataGraph.isPresent());
            DataGraph dataGraph2 = optDataGraph.get();
            Assert.assertEquals(dataGraph1.getVertexDocSet().size(), dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(dataGraph1.getEdgeSet().size(), dataGraph2.getEdgeSet().size());
            File graphEdgesFile = new File(graphEdgesFileName);
            Assert.assertTrue(graphEdgesFile.delete());
            File graphVertexesFile = new File(graphVertexesFileName);
            Assert.assertTrue(graphVertexesFile.delete());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_add_delete()
    {
        RedisGraphs redisGraph = mRedisDS.createGraph();    // cannot use this instance to visualize graphs - missing schemas
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph);

        try
        {
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);

// Add the entire IMDb Graph.

            redisGraph.add(dataGraph);

// Delete the entire IMDb Graph.

            redisGraph.delete(dataGraph);

// Selectively add two nodes and one relationship between them.

            DataDoc ddPerson = imdbGraph.createPersonVertex("nm0000380", "Robert Duvall", "Veteran actor and director Robert Selden Duvall was born on January 5, 1931, in San Diego, CA, to Mildred Virginia (Hart), an amateur actress, and William Howard Duvall, a career military officer who later became an admiral. Duvall majored in drama at Principia College (Elsah, IL), then served a two-year hitch in the army after graduating in 1953. He began attending The Neighborhood Playhouse School of the Theatre In New York City on the G.I. Bill in 1955, studying under Sanford Meisner along with Dustin Hoffman, with whom Duvall shared an apartment. Both were close to another struggling young actor named Gene Hackman. Meisner cast Duvall in the play \"The Midnight Caller\" by Horton Foote, a link that would prove critical to his career, as it was Foote who recommended Duvall to play the mentally disabled \"Boo Radley\" in To Kill a Mockingbird (1962). This was his first \"major\" role since his 1956 motion picture debut as an MP in Somebody Up There Likes Me (1956), starring Paul Newman.",
                                                            "1931", "actor,producer,soundtrack", true, 69, "Black");
            boolean isOK = redisGraph.addNode(dataGraph, ddPerson);
            Assert.assertTrue(isOK);
            DataDoc ddMovie = imdbGraph.createMovieVertex("tt0316356", "Open Range", "A former gunslinger is forced to take up arms again when he and his cattle crew are threatened by a corrupt lawman.",
                                                          "2003", "Action,Drama,Romance", 68300000.0);
            isOK = redisGraph.addNode(dataGraph, ddMovie);
            Assert.assertTrue(isOK);
            DataDoc ddRole = imdbGraph.createRoleEdge("pr000009", "Actor", "2002", "Co-Lead", "Boss Spearman");
            isOK = redisGraph.addRelationship(dataGraph, ddPerson, ddMovie, ddRole);
            Assert.assertTrue(isOK);
            redisGraph.delete(dataGraph);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_create_load_compare()
    {
        RedisGraphs redisGraph = mRedisDS.createGraph();    // cannot use this instance to visualize graphs - missing schemas
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            redisGraph.add(dataGraph1);

            DataGraph dataGraph2 = redisGraph.queryAll(GRAPH_DB_NAME);
            int vertexCount1 = dataGraph1.getVertexDocSet().size();
            int vertexCount2 = dataGraph2.getVertexDocSet().size();
            Assert.assertEquals(vertexCount1, vertexCount2);
            int edgeCount1 = dataGraph1.getEdgeSet().size();
            int edgeCount2 = dataGraph2.getEdgeSet().size();
            Assert.assertEquals(edgeCount1, edgeCount2);
            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_update_delete()
    {
        RedisGraphs redisGraph = mRedisDS.createGraph();    // cannot use this instance to visualize graphs - missing schemas
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph);

        try
        {
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            redisGraph.add(dataGraph);

// Update multiple properties on a node.

            DataDoc ddPerson = imdbGraph.createPersonVertex("nm0000380", "Robert Duvall", "Veteran actor and director Robert Selden Duvall was born on January 5, 1931, in San Diego, CA, to Mildred Virginia (Hart), an amateur actress, and William Howard Duvall, a career military officer who later became an admiral. Duvall majored in drama at Principia College (Elsah, IL), then served a two-year hitch in the army after graduating in 1953. He began attending The Neighborhood Playhouse School of the Theatre In New York City on the G.I. Bill in 1955, studying under Sanford Meisner along with Dustin Hoffman, with whom Duvall shared an apartment. Both were close to another struggling young actor named Gene Hackman. Meisner cast Duvall in the play \"The Midnight Caller\" by Horton Foote, a link that would prove critical to his career, as it was Foote who recommended Duvall to play the mentally disabled \"Boo Radley\" in To Kill a Mockingbird (1962). This was his first \"major\" role since his 1956 motion picture debut as an MP in Somebody Up There Likes Me (1956), starring Paul Newman.",
                                                            "1931", "actor,producer,soundtrack", true, 69, "Black");
            DGCriteria dgCriteria = new DGCriteria(dataGraph.getName());
            dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, ddPerson));
            Optional<DataDoc> optDataDoc = redisGraph.queryNode(dgCriteria);
            Assert.assertTrue(optDataDoc.isPresent());
            DataDoc rgDoc = optDataDoc.get();
            rgDoc.clearFeatures();
            rgDoc.getItemByName("common_id").enableFeature(Data.FEATURE_IS_PRIMARY);
            rgDoc.setValueByName("common_name", "Mr. Robert Duvall");
            rgDoc.setValueByName("principal_birth_year", 1940);
            rgDoc.remove("principal_professions");
            int propertiesSet = redisGraph.updateNode(dataGraph, rgDoc);
            Assert.assertEquals(3, propertiesSet);

// Delete a node and a relationship.

            ddPerson = imdbGraph.createPersonVertex("nm0000380", "Robert Duvall", "Veteran actor and director Robert Selden Duvall was born on January 5, 1931, in San Diego, CA, to Mildred Virginia (Hart), an amateur actress, and William Howard Duvall, a career military officer who later became an admiral. Duvall majored in drama at Principia College (Elsah, IL), then served a two-year hitch in the army after graduating in 1953. He began attending The Neighborhood Playhouse School of the Theatre In New York City on the G.I. Bill in 1955, studying under Sanford Meisner along with Dustin Hoffman, with whom Duvall shared an apartment. Both were close to another struggling young actor named Gene Hackman. Meisner cast Duvall in the play \"The Midnight Caller\" by Horton Foote, a link that would prove critical to his career, as it was Foote who recommended Duvall to play the mentally disabled \"Boo Radley\" in To Kill a Mockingbird (1962). This was his first \"major\" role since his 1956 motion picture debut as an MP in Somebody Up There Likes Me (1956), starring Paul Newman.",
                                                    "1931", "actor,producer,soundtrack", true, 69, "Black");
            int nodesDeleted = redisGraph.deleteNode(dataGraph, ddPerson);
            Assert.assertEquals(1, nodesDeleted);

            DataDoc ddRole = imdbGraph.createRoleEdge("pr000011", "Director", "2002", "Supporting", "Bob Smith");
            propertiesSet = redisGraph.updateRelationship(dataGraph, ddRole);
            Assert.assertEquals(2, propertiesSet);

            int relationshipsDeleted = redisGraph.deleteRelationship(dataGraph, ddRole);
            Assert.assertEquals(1, relationshipsDeleted);

            redisGraph.delete(dataGraph);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_queries_1()
    {
        RedisGraphs redisGraph = mRedisDS.createGraph();    // cannot use this instance to visualize graphs - missing schemas
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            redisGraph.add(dataGraph1);

            DGCriteria dgCriteria = new DGCriteria(dataGraph1.getName());
            DataDoc ddMovie = imdbGraph.createMovieVertex("tt0103855", "The Bodyguard", "A former Secret Service agent takes on the job of bodyguard to an R&B singer, whose lifestyle is most unlike a President's.",
                                                          "1992", "Action,Drama,Music", 411000000.0);
            dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, ddMovie));
            DataGraph dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(1, dataGraph2.getVertexDocSet().size());

            dgCriteria.reset();
            ddMovie = imdbGraph.createMovieVertex("tt0103855", "The Bodyguard", "A former Secret Service agent takes on the job of bodyguard to an R&B singer, whose lifestyle is most unlike a President's.",
                                                  "1992", "Action,Drama,Music", 411000000.0);
            DataDoc ddPerson = imdbGraph.createPersonVertex("nm0000126", "Kevin Costner", "Kevin Michael Costner was born on January 18, 1955 in Lynwood, California, the third child of Bill Costner, a ditch digger and ultimately an electric line servicer for Southern California Edison, and Sharon Costner (née Tedrick), a welfare worker. His older brother, Dan, was born in 1950. A middle brother died at birth in 1953. His father's job required him to move regularly, which caused Kevin to feel like an Army kid, always the new kid at school, which led to him being a daydreamer. As a teen, he sang in the Baptist church choir, wrote poetry, and took writing classes. At 18, he built his own canoe and paddled his way down the rivers that Lewis & Clark followed to the Pacific. Despite his present height, he was only 5'2\" when he graduated high school. Nonetheless, he still managed to be a basketball, football and baseball star. In 1973, he enrolled at California State University at Fullerton, where he majored in business. During that period, Kevin decided to take acting lessons five nights a week. He graduated with a business degree in 1978 and married his college sweetheart, Cindy Costner. He initially took a marketing job in Orange County. Everything changed when he accidentally met Richard Burton on a flight from Mexico. Burton advised him to go completely after acting if that is what he wanted. He quit his job and moved to Hollywood soon after. He drove a truck, worked on a deep sea fishing boat, and gave bus tours to stars' homes before finally making his own way into the films.",
                                                            "1955", "actor,producer,soundtrack", true, 73, "Blue");
            DataDoc ddRole = imdbGraph.createRoleEdge("pr000014", "Actor", "1991",  "Co-Lead", "Frank Farmer");
            dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, ddPerson));
            dgCriteria.add(new DGCriterion(Data.GraphObject.Edge, ddRole));
            dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, ddMovie));
            dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(2, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(1, dataGraph2.getEdgeSet().size());

            dgCriteria.reset();
            ddMovie = imdbGraph.createMovieVertex("tt0068646", "The Godfather", "An organized crime dynasty's aging patriarch transfers control of his clandestine empire to his reluctant son.",
                                                  "1972", "Crime,Drama", 4309000000.0);
            ddPerson = imdbGraph.createPersonVertex("nm0000199", "Al Pacino", "Alfredo James \"Al\" 'Pacino established himself as a film actor during one of cinema's most vibrant decades, the 1970s, and has become an enduring and iconic figure in the world of American movies. He was born April 25, 1940 in Manhattan, New York City, to Italian-American parents, Rose (nee Gerardi) and Sal Pacino.",
                                                    "1940", "actor,producer,soundtrack", true, 68, "Blue");
            ddRole = imdbGraph.createRoleEdge("pr000002", "Actor", "1971", "Supporting", "Michael Corleone");
            dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, ddPerson));
            dgCriteria.add(new DGCriterion(Data.GraphObject.Edge, ddRole));
            dgCriteria.add(new DGCriterion(Data.GraphObject.Vertex, ddMovie));
            dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(2, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(1, dataGraph2.getEdgeSet().size());

            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_queries_2()
    {
        RedisGraphs redisGraph = mRedisDS.createGraph();    // cannot use this instance to visualize graphs - missing schemas
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            redisGraph.add(dataGraph1);

// GRAPH.QUERY movie_graph "MATCH (p:Principal)-[a:Actor]->(m:Movie) WHERE p.name STARTS WITH 'Al' AND a.year < 1975 AND m.revenue > 4300000000 RETURN p,a,m"
            DGCriteria dgCriteria = new DGCriteria(dataGraph1.getName());
            DSCriteria dsCriteria = new DSCriteria("Principal Criteria");
            dsCriteria.add(new DSCriterion("common_name", Data.Operator.STARTS_WITH, "Al"));
            DGCriterion dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Principal", dsCriteria);
            dgCriteria.add(dgCriterion);
            dsCriteria = new DSCriteria("Actor Criteria");
            dsCriteria.add(new DSCriterion("employment_year", Data.Operator.LESS_THAN, 1975));
            dgCriterion = new DGCriterion(Data.GraphObject.Edge, "Actor", false, true, dsCriteria, 0);
            dgCriteria.add(dgCriterion);
            dsCriteria = new DSCriteria("Movie Criteria");
            dsCriteria.add(new DSCriterion("movie_revenue", Data.Operator.GREATER_THAN, 4300000000L));
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Movie", dsCriteria);
            dgCriteria.add(dgCriterion);
            DataGraph dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(2, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(1, dataGraph2.getEdgeSet().size());

// GRAPH.QUERY movie_graph "MATCH gp = (p:Principal)-[a:Actor*..1]-() WHERE p.name = 'Robert Duvall' RETURN gp"
            dgCriteria.reset();
            dsCriteria = new DSCriteria("Principal Criteria");
            dsCriteria.add(new DSCriterion("common_name", Data.Operator.EQUAL, "Robert Duvall"));
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Principal", dsCriteria);
            dgCriteria.add(dgCriterion);
            dgCriterion = new DGCriterion(Data.GraphObject.Edge, "Actor", false, false, 1);
            dgCriteria.add(dgCriterion);
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex);
            dgCriteria.add(dgCriterion);
            dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(6, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(5, dataGraph2.getEdgeSet().size());

// GRAPH.QUERY movie_graph "MATCH gp = (p:Principal)-[a:Actor*..2]-() WHERE p.name = 'Robert Duvall' RETURN gp"
            dgCriteria.reset();
            dsCriteria = new DSCriteria("Principal Criteria");
            dsCriteria.add(new DSCriterion("common_name", Data.Operator.EQUAL, "Robert Duvall"));
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Principal", dsCriteria);
            dgCriteria.add(dgCriterion);
            dgCriterion = new DGCriterion(Data.GraphObject.Edge, "Actor", false, false, 2);
            dgCriteria.add(dgCriterion);
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex);
            dgCriteria.add(dgCriterion);
            dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(12, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(11, dataGraph2.getEdgeSet().size());

// While we are saving the commands to a stream above, we use the RedisDS to save them to the file system.
// You validated via 'redis-cli' that these were created correctly.

            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_queries_3()
    {
        RedisGraphs redisGraph = mRedisDS.createGraph();    // cannot use this instance to visualize graphs - missing schemas
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            redisGraph.add(dataGraph1);

            String graphDBName = GRAPH_DB_NAME;
            String cypherStatement = "MATCH gp = (p:Principal)-[a:Actor*..2]-() WHERE p.common_name = 'Robert Duvall' RETURN nodes(gp),relationships(gp)";
            DataGraph dataGraph2 = redisGraph.queryCypher(graphDBName, cypherStatement);
            Assert.assertEquals(12, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(11, dataGraph2.getEdgeSet().size());

            cypherStatement = "MATCH (p:Principal)-[a:Actor]->(m:Movie) WHERE p.common_name STARTS WITH 'Al' AND a.employment_year < 1975 AND m.movie_revenue > 4300000000 RETURN p,a,m";
            dataGraph2 = redisGraph.queryCypher(graphDBName, cypherStatement);
            Assert.assertEquals(2, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(1, dataGraph2.getEdgeSet().size());

            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_queries_4()
    {
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            GraphDS graphDS = new GraphDS(mAppCtx, GRAPH_DB_NAME);
            graphDS.loadFromCSV("data/graph_movies_vertex_schema.xml", "data/graph_movies_edge_schema.xml", "data/graph_movies_data.csv");
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            RedisGraphs redisGraph = mRedisDS.createGraph(graphDS.getVertexSchema(), graphDS.getEdgeSchema());
            redisGraph.add(dataGraph1);

            DGCriteria dgCriteria = new DGCriteria(dataGraph1.getName());
            DSCriteria dsCriteria = new DSCriteria("Principal Criteria");
            dsCriteria.add(new DSCriterion("common_name", Data.Operator.STARTS_WITH, "Al"));
            DGCriterion dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Principal", dsCriteria);
            dgCriteria.add(dgCriterion);
            dsCriteria = new DSCriteria("Actor Criteria");
            dsCriteria.add(new DSCriterion("employment_year", Data.Operator.LESS_THAN, 1975));
            dgCriterion = new DGCriterion(Data.GraphObject.Edge, "Actor", false, true, dsCriteria, 0);
            dgCriteria.add(dgCriterion);
            dsCriteria = new DSCriteria("Movie Criteria");
            dsCriteria.add(new DSCriterion("movie_revenue", Data.Operator.GREATER_THAN, 4300000000L));
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Movie", dsCriteria);
            dgCriteria.add(dgCriterion);
            DataGraph dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(2, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(1, dataGraph2.getEdgeSet().size());

            String queryString = "broadway";
            dataGraph2 = redisGraph.queryNodeText(dataGraph1.getName(), "Principal", "broadway");
            Assert.assertEquals(3, dataGraph2.getVertexDocSet().size());

            queryString = "born";
            DataGrid dataGrid = redisGraph.queryNodeText(dataGraph1.getName(), graphDS.getVertexSchema(), queryString,
                                                           0, 100);
            Assert.assertEquals(27, dataGrid.rowCount());

            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(true);
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(dataGrid, printWriter, String.format("Query for '%s'", queryString), 40, 1);

            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    // Use this method to diagnose issues with advanced graph queries
    public void exercise_queries_5()
    {
        IMDbGraph imdbGraph = new IMDbGraph(mAppCtx);
        DataGraph dataGraph1 = imdbGraph.create(true);
        Assert.assertNotNull(dataGraph1);

        try
        {
            GraphDS graphDS = new GraphDS(mAppCtx, GRAPH_DB_NAME);
            graphDS.loadFromCSV("data/graph_movies_vertex_schema.xml", "data/graph_movies_edge_schema.xml", "data/graph_movies_data.csv");
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            RedisGraphs redisGraph = mRedisDS.createGraph(graphDS.getVertexSchema(), graphDS.getEdgeSchema());
            redisGraph.add(dataGraph1);

            DataGraph dataGraph2 = redisGraph.queryAll(dataGraph1.getName());
            int vertexCount = dataGraph2.getVertexDocSet().size();
            int edgeCount = dataGraph2.getEdgeSet().size();
            System.out.printf("%n[Query All] (%d v, %d e): %s%n", vertexCount, edgeCount, dataGraph2.getFeature(Redis.FEATURE_CYPHER_QUERY));
            System.out.printf("[Query All] (%d v, %d e): %s%n%n", vertexCount, edgeCount, dataGraph2.getFeature(Redis.FEATURE_REDISGRAPH_QUERY));

            DGCriteria dgCriteria = new DGCriteria(dataGraph1.getName());
            DSCriteria dsCriteria = new DSCriteria("Movie Criteria");
            dsCriteria.add(new DSCriterion("common_name", Data.Operator.STARTS_WITH, "Al"));
            DGCriterion dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Movie", dsCriteria);
            dgCriteria.add(dgCriterion);
            DataGraph dataGraph3 = redisGraph.queryPattern(dgCriteria);
            vertexCount = dataGraph3.getVertexDocSet().size();
            edgeCount = dataGraph3.getEdgeSet().size();
            System.out.printf("%n[%s] (%d v, %d e): %s%n", dsCriteria.getName(), vertexCount, edgeCount, dataGraph3.getFeature(Redis.FEATURE_CYPHER_QUERY));
            System.out.printf("[%s] (%d v, %d e): %s%n%n", dsCriteria.getName(), vertexCount, edgeCount, dataGraph3.getFeature(Redis.FEATURE_REDISGRAPH_QUERY));

            dgCriteria = new DGCriteria(dataGraph1.getName());
            dsCriteria = new DSCriteria("Movie Criteria");
            dsCriteria.add(new DSCriterion("movie_revenue", Data.Operator.GREATER_THAN, 21000000));
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Movie", dsCriteria);
            dgCriteria.add(dgCriterion);
            dataGraph3 = redisGraph.queryPattern(dgCriteria);
            vertexCount = dataGraph3.getVertexDocSet().size();
            edgeCount = dataGraph3.getEdgeSet().size();
            System.out.printf("%n[%s] (%d v, %d e): %s%n", dsCriteria.getName(), vertexCount, edgeCount, dataGraph3.getFeature(Redis.FEATURE_CYPHER_QUERY));
            System.out.printf("[%s] (%d v, %d e): %s%n%n", dsCriteria.getName(), vertexCount, edgeCount, dataGraph3.getFeature(Redis.FEATURE_REDISGRAPH_QUERY));
            String graphHTMLPathFileName = "data/imdb_graph.html";
            DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
            graphDS.visualizeToFileName(dataGraph2, dataGraph3, graphHTMLPathFileName, graphOptionsDoc);

            dsCriteria = new DSCriteria("Movie Criteria");
            String cypherStatement = "MATCH (n1:Principal)-[r1:Actor]-(n2:Movie) WHERE n1.common_name STARTS WITH 'B' AND r1.employment_year > 1970 AND n2.movie_revenue > 21000000 RETURN n1,r1,n2";
            DataGraph dataGraph4 = redisGraph.queryCypher(dataGraph1.getName(), cypherStatement);
            System.out.printf("%n[%s] (%d v, %d e): %s%n", dsCriteria.getName(), vertexCount, edgeCount, dataGraph4.getFeature(Redis.FEATURE_CYPHER_QUERY));
            System.out.printf("[%s] (%d v, %d e): %s%n%n", dsCriteria.getName(), vertexCount, edgeCount, dataGraph4.getFeature(Redis.FEATURE_REDISGRAPH_QUERY));
            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(false);
            PrintWriter printWriter = new PrintWriter(System.out, true);
            DataGrid edgeDataGrid = dataGraph4.getEdgesDataGrid();
            dataGridConsole.write(edgeDataGrid, printWriter, "Matched Edges Data Grid", 40, 1);
            graphHTMLPathFileName = "data/imdb_graph.html";
            graphOptionsDoc = graphDS.createOptionsDefault();
            graphDS.visualizeToFileName(dataGraph2, dataGraph4, graphHTMLPathFileName, graphOptionsDoc);

            /*
            dsCriteria = new DSCriteria("Actor Criteria");
            dsCriteria.add(new DSCriterion("employment_year", Data.Operator.LESS_THAN, 1975));
            dgCriterion = new DGCriterion(Data.GraphObject.Edge, "Actor", false, true, dsCriteria, 0);
            dgCriteria.add(dgCriterion);
            dsCriteria = new DSCriteria("Movie Criteria");
            dsCriteria.add(new DSCriterion("movie_revenue", Data.Operator.GREATER_THAN, 4300000000L));
            dgCriterion = new DGCriterion(Data.GraphObject.Vertex, "Movie", dsCriteria);
            dgCriteria.add(dgCriterion);
            DataGraph dataGraph2 = redisGraph.queryPattern(dgCriteria);
            Assert.assertEquals(2, dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(1, dataGraph2.getEdgeSet().size());
            */

            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_app_studio()
    {
        GraphDS graphDS = new GraphDS(mAppCtx, GRAPH_DB_NAME);

        try
        {
            graphDS.loadFromCSV("data/graph_movies_vertex_schema.xml", "data/graph_movies_edge_schema.xml", "data/graph_movies_data.csv");
            DataGraph dataGraph1 = graphDS.createDataGraph(GRAPH_DB_NAME);
            String logPathFileName = createLogPathFile();
            mRedisDS.openCaptureWithFile(logPathFileName);
            RedisGraphs redisGraph = mRedisDS.createGraph(graphDS.getVertexSchema(), graphDS.getEdgeSchema());
            redisGraph.add(dataGraph1);

            String graphHTMLPathFileName = "data/imdb_graph.html";
            DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
            graphDS.visualizeToFileName(graphHTMLPathFileName, graphOptionsDoc);

            DataGridConsole dataGridConsole = new DataGridConsole();
            dataGridConsole.setFormattedFlag(false);
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(graphDS.getEdgeGridDS().getDataGrid(), printWriter, "Edges Data Grid", 40, 1);

            DataGraph dataGraph2 = redisGraph.queryAll(dataGraph1.getName());
            Assert.assertEquals(dataGraph1.getVertexDocSet().size(), dataGraph2.getVertexDocSet().size());
            Assert.assertEquals(dataGraph1.getEdgeSet().size(), dataGraph2.getEdgeSet().size());

            Optional<DataDoc> optDataDoc = redisGraph.loadSchema(true);
            Assert.assertTrue(optDataDoc.isPresent());
            DataDoc nodeSchemaDoc = optDataDoc.get();
            DataGrid nodeSchemaGrid = redisGraph.schemaDocToDataGrid(nodeSchemaDoc, false);
            dataGridConsole.write(nodeSchemaGrid, printWriter, nodeSchemaGrid.getName());

            optDataDoc = redisGraph.loadSchema(false);
            Assert.assertTrue(optDataDoc.isPresent());
            DataDoc relSchemaDoc = optDataDoc.get();
            DataGrid relSchemaGrid = redisGraph.schemaDocToDataGrid(relSchemaDoc, false);
            dataGridConsole.write(relSchemaGrid, printWriter, relSchemaGrid.getName());

            redisGraph.delete(dataGraph1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    @Test
    public void exercise()
        throws RedisDSException
    {
        mRedisDS.open(APPLICATION_PREFIX_DEFAULT);
        mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
        mRedisDS.createCore().flushDatabase();
//        exercise_csv_save();
        exercise_add_delete();
        exercise_create_load_compare();
        exercise_update_delete();
        exercise_queries_1();
        exercise_queries_2();
        exercise_queries_3();
        exercise_queries_4();
        exercise_app_studio();
    }

    @After
    public void cleanup()
    {
        Jedis jedisConnection = mRedisDS.getCmdConnection();
        String streamKeyName = mRedisDS.streamKeyName();
        if (jedisConnection.exists(streamKeyName))
            jedisConnection.del(streamKeyName);
        mRedisDS.shutdown();
    }
}
