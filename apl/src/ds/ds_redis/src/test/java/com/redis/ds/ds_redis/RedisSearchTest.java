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

import com.redis.ds.ds_json.JsonDS;
import com.redis.ds.ds_redis.search.RedisSearch;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterion;
import com.redis.foundation.io.DataDocJSON;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridCSV;
import com.redis.foundation.io.DataGridConsole;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * The RedisSearchTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisSearchTest
{
    private final String APPLICATION_PREFIX_DEFAULT = "ASRS";

    private AppCtx mAppCtx;
    private RedisDS mRedisDS;

    @Before
    public void setup()
    {
        HashMap<String,Object> hmProperties = new HashMap<>();
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".host_name", Redis.HOST_NAME_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".port_number", Redis.PORT_NUMBER_DEFAULT);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".ssl_enabled", StrUtl.STRING_FALSE);
        hmProperties.put(Redis.CFG_PROPERTY_PREFIX + ".application_prefix", Redis.APPLICATION_PREFIX_DEFAULT);
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

    private DataDoc loadSchema(String aPathFileName)
    {
        DataDocXML dataDocXML = new DataDocXML();
        try
        {
            dataDocXML.load(aPathFileName);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }

        DataDoc schemaDoc = dataDocXML.getDataDoc();
        Assert.assertTrue(schemaDoc.count() > 0);

        return schemaDoc;
    }

    // In case you illustrate how to load JSON - might want to just save it as JSON with fixed date strings
    public void exerciseJSONLoad()
    {
        Date itemDate;
        DataItem dataItem;
        List<DataDoc> jsonDataDocList;
        Optional<DataItem> optDataItem;

        try
        {
            DataDocJSON dataDocJSON = new DataDocJSON();
            jsonDataDocList = dataDocJSON.loadList("data/product_electronics.json");
            for (DataDoc jsonDataDoc : jsonDataDocList)
            {
                optDataItem = jsonDataDoc.getItemByNameOptional("activeUpdateDate");
                if (optDataItem.isPresent())
                {
                    dataItem = optDataItem.get();
                    if (dataItem.isValueNotEmpty())
                    {
                        itemDate = Data.createDate(dataItem.getValue(), Data.FORMAT_ISO8601DATETIME_NOMILLI);
                        if (itemDate != null)
                        {
                            dataItem.setType(Data.Type.DateTime);
                            dataItem.setValue(itemDate);
                        }
                    }
                }
                jsonDataDoc.addFeature(Redis.FEATURE_SCORE, Redis.SEARCH_SCORE_DEFAULT);
            }
        }
        catch (IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseProductSynonyms(RedisSearch aRedisSearch)
    {
        DataDoc dataDoc;
        String termValues;

        try
        {
            DataGridCSV dataGridCSV = new DataGridCSV();
            Optional<DataGrid> optDataGrid = dataGridCSV.load("data/product_synonyms.csv", true);
            Assert.assertTrue(optDataGrid.isPresent());
            DataGrid dataGrid = optDataGrid.get();
            dataGrid.setName("Product Synonyms");
            DataItem synonymItem = new DataItem.Builder().name("synonym_terms").title("Synonym Terms").build();
            mRedisDS.startMarker("Add Synonyms");
            int rowCount = dataGrid.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                synonymItem.clearValues();
                dataDoc = dataGrid.getRowAsDoc(row);
                termValues = dataDoc.getValueByName("synonym_term");
                if (StringUtils.countMatches(termValues, StrUtl.CHAR_COMMA) > 0)
                {
                    String[] termItems = termValues.split(",");
                    for (String termItem : termItems)
                        synonymItem.addValue(termItem.trim());
                    Assert.assertTrue(aRedisSearch.addSynonyms(synonymItem));
                }
            }
            mRedisDS.finishMarker();
            mRedisDS.startMarker("Load Synonyms");
            dataGrid = aRedisSearch.loadSynonyms();
            mRedisDS.finishMarker();
            Assert.assertEquals(1901, dataGrid.rowCount());
//            DataGridConsole dataGridConsole = new DataGridConsole();
//            PrintWriter printWriter = new PrintWriter(System.out, true);
//            dataGridConsole.write(dataGrid, printWriter, "Product Synonyms", 40, 1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    // ToDo: Jedis 4.x does not support Suggestions - fix when available
    public void exerciseExtendedSuggestions(RedisSearch aRedisSearch)
    {
        DataDoc dataDoc;
        DataItem nameItem, skuItem;
        Optional<DataItem> optNameItem, optSKUItem;

        try
        {
            DataDoc dataSchemaDoc = loadSchema("data/product_electronics_hash.xml");
            DataGridCSV dataGridCSV = new DataGridCSV(dataSchemaDoc);
            dataGridCSV.load("data/product_electronics.csv", false);
            DataGrid dataGrid1 = dataGridCSV.getDataGrid();

            mRedisDS.startMarker("Add Suggestions");
            int rowCount = dataGrid1.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                dataDoc = dataGrid1.getRowAsDoc(row);
                optSKUItem = dataDoc.getItemByNameOptional("sku");
                optNameItem = dataDoc.getItemByNameOptional("name");
                if ((optNameItem.isPresent()) && (optSKUItem.isPresent()))
                {
                    skuItem = optSKUItem.get();
                    nameItem = optNameItem.get();
                    if ((nameItem.isValueNotEmpty()) && (skuItem.isValueNotEmpty()))
                        Assert.assertTrue(aRedisSearch.addSuggestionWithShingles(nameItem, skuItem) > 0);
                }
            }
            mRedisDS.finishMarker();

            String prefixString = "cyber";
            DataGrid dataGrid2 = aRedisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);
            String gridTitle = String.format("%s - '%s'", dataGrid2.getName(), prefixString);
            dataGridConsole.write(dataGrid2, printWriter, gridTitle, 60, 1);

            prefixString = "batte";
            dataGrid2 = aRedisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
            printWriter = new PrintWriter(System.out, true);
            gridTitle = String.format("%s - '%s'", dataGrid2.getName(), prefixString);
            dataGridConsole.write(dataGrid2, printWriter, gridTitle, 80, 1);

            prefixString = "whirl";
            dataGrid2 = aRedisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
            printWriter = new PrintWriter(System.out, true);
            gridTitle = String.format("%s - '%s'", dataGrid2.getName(), prefixString);
            dataGridConsole.write(dataGrid2, printWriter, gridTitle, 80, 1);

// Unfortunately, dropping the search index does not remove the dictionary key.

        }
        catch (IOException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseSuggestions(RedisSearch aRedisSearch)
    {
        mRedisDS.startMarker("Query Suggestions");
        String prefixString = "cyber";
        DataGrid dataGrid = aRedisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
        DataGridConsole dataGridConsole = new DataGridConsole();
        PrintWriter printWriter = new PrintWriter(System.out, true);
        String gridTitle = String.format("%s - '%s'", dataGrid.getName(), prefixString);
        dataGridConsole.write(dataGrid, printWriter, gridTitle, 60, 1);

        prefixString = "ener";
        dataGrid = aRedisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
        printWriter = new PrintWriter(System.out, true);
        gridTitle = String.format("%s - '%s'", dataGrid.getName(), prefixString);
        dataGridConsole.write(dataGrid, printWriter, gridTitle, 80, 1);

        prefixString = "whirl";
        dataGrid = aRedisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
        printWriter = new PrintWriter(System.out, true);
        gridTitle = String.format("%s - '%s'", dataGrid.getName(), prefixString);
        dataGridConsole.write(dataGrid, printWriter, gridTitle, 80, 1);

        mRedisDS.finishMarker();
    }

    public void exerciseProductQueries(RedisSearch aRedisSearch)
    {
        DataGrid dataGrid;
        DSCriterion dsCriterion;

        try
        {
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);

            DSCriteria dsCriteria = new DSCriteria("Product Criteria");
            dsCriteria.add(Redis.RS_QUERY_OFFSET, Data.Operator.EQUAL, 0);
            dsCriteria.add(Redis.RS_QUERY_OFFSET, Data.Operator.EQUAL, 10);
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "office");
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(7, dataGrid.rowCount());

            dsCriteria.reset();
            dsCriteria.add("name", Data.Operator.EQUAL, "Walker");
            dsCriteria.add("color", Data.Operator.EQUAL, "Brown");
            dataGrid = aRedisSearch.query(dsCriteria);
            if (aRedisSearch.getDocumentStoreType() == Redis.Document.JSON)
                Assert.assertEquals(1, dataGrid.rowCount());
            else
                Assert.assertEquals(2, dataGrid.rowCount());

            dsCriteria.reset();
            dsCriterion = new DSCriterion(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "Walker");
            dsCriteria.add("name", Data.Operator.HIGHLIGHT, true);
//            dsCriterion.disableFeature(RediSearch.FEATURE_IS_PHONETIC);
            dsCriteria.add(dsCriterion);
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(9, dataGrid.rowCount());
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_OFFSET, Data.Operator.EQUAL, 20);
            dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, 40);
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(40, dataGrid.rowCount());

            dsCriteria.reset();
            dsCriteria.add("name", Data.Operator.EQUAL, "Walker");
            dsCriteria.add("color", Data.Operator.EQUAL, "Brown");
            dsCriteria.add("regularPrice", Data.Operator.BETWEEN_INCLUSIVE, 100, 200);
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(1, dataGrid.rowCount());

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_OFFSET, Data.Operator.EQUAL, 0);
            dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, 10);
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dsCriteria.add("regularPrice", Data.Operator.SORT, Data.Order.DESCENDING.name());
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(10, dataGrid.rowCount());

            dsCriteria.reset();
            if (aRedisSearch.getDocumentStoreType() == Redis.Document.Hash)
            {
                dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "office");
                dsCriteria.add("activeUpdateDate", Data.Operator.LESS_THAN, "MAR-08-2017 11:00:00");
                dataGrid = aRedisSearch.query(dsCriteria);
                Assert.assertEquals(2, dataGrid.rowCount());
            }
            else
            {
                dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "office");
                dsCriteria.add("sku", Data.Operator.GREATER_THAN, 1000);
                dataGrid = aRedisSearch.query(dsCriteria);
                Assert.assertEquals(7, dataGrid.rowCount());
            }

            dsCriteria.reset();
            if (aRedisSearch.getDocumentStoreType() == Redis.Document.Hash)
            {
                dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "office");
                dsCriteria.add("longDescription", Data.Operator.HIGHLIGHT, true);
                dsCriteria.add("shortDescription", Data.Operator.HIGHLIGHT, true);
                dsCriteria.add("longDescription", Data.Operator.SNIPPET, true);
                dsCriteria.add("activeUpdateDate", Data.Operator.GREATER_THAN, "MAR-08-2016 11:00:00");
                dataGrid = aRedisSearch.query(dsCriteria);
                Assert.assertEquals(7, dataGrid.rowCount());
            }
            else
            {
                dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "office");
                dsCriteria.add("longDescription", Data.Operator.HIGHLIGHT, true);
                dsCriteria.add("shortDescription", Data.Operator.HIGHLIGHT, true);
                dsCriteria.add("longDescription", Data.Operator.SNIPPET, true);
                dsCriteria.add("color", Data.Operator.CONTAINS, "black");
                dataGrid = aRedisSearch.query(dsCriteria);
                Assert.assertEquals(7, dataGrid.rowCount());
            }

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dsCriteria.add("manufacturer", Data.Operator.FACET, true);
            dsCriteria.add("department", Data.Operator.FACET, true);
            dsCriteria.add(Redis.RS_QUERY_OFFSET, Data.Operator.EQUAL, 0);
            dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, 100);
            dataGrid = aRedisSearch.calculateFacets(dsCriteria);
            Assert.assertEquals(2, dataGrid.rowCount());
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 80, 1);
            dataGrid = aRedisSearch.calculateUIFacets(dsCriteria);
            Assert.assertEquals(43, dataGrid.rowCount());
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);

// The following queries assume that the RediSearch.FEATURE_SHADOW_FIELDS feature is enabled

            dsCriteria.reset();
            dsCriteria.add("sku", Data.Operator.EQUAL, 4195903);
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(1, dataGrid.rowCount());

            dsCriteria.reset();
            dsCriteria.add("sku", Data.Operator.NOT_EQUAL, 4195903);
            dataGrid = aRedisSearch.query(dsCriteria);
            Assert.assertEquals(10, dataGrid.rowCount());

            if (aRedisSearch.getDocumentStoreType() == Redis.Document.Hash)
            {
                dsCriteria.reset();
                dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "office");
                dsCriteria.add("activeUpdateDate", Data.Operator.BETWEEN, "JAN-01-2018 00:00:00", "FEB-01-2019 00:00:00");
//                dsCriteria.add("activeUpdateDate", Data.Operator.SORT, Data.Order.ASCENDING.name());
                dataGrid = aRedisSearch.query(dsCriteria);
                Assert.assertEquals(3, dataGrid.rowCount());
                dataGridConsole = new DataGridConsole();
                dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);
            }
        }
        catch (RedisDSException e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseProductSearchAsHashes()
    {
        DataDoc dataSchemaDoc = loadSchema("data/product_electronics_hash.xml");
        DataDoc searchSchemaDoc = loadSchema("data/product_electronics_search_hash.xml");
        searchSchemaDoc.addFeature(Redis.FEATURE_INDEX_NAME, "product_electronics");
        RedisSearch redisSearch = mRedisDS.createSearch(Redis.Document.Hash, dataSchemaDoc, searchSchemaDoc);
        try
        {
            DataGridCSV dataGridCSV = new DataGridCSV(dataSchemaDoc);
            dataGridCSV.load("data/product_electronics.csv", false);
            DataGrid dataGrid = dataGridCSV.getDataGrid();

            mRedisDS.startMarker("Add Documents");
            redisSearch.add(dataGrid);
            mRedisDS.finishMarker();

            exerciseProductSynonyms(redisSearch);
            exerciseProductQueries(redisSearch);
            exerciseSuggestions(redisSearch);

            mRedisDS.startMarker("Update Document");
            DataDoc dataDoc = dataGrid.getRowAsDoc(0);
            dataDoc.setValueByName("name", "Redis Labs");
            redisSearch.update(dataDoc);
            mRedisDS.finishMarker();

            mRedisDS.startMarker("Delete Document");
            dataDoc = dataGrid.getRowAsDoc(0);
            mRedisDS.createDoc().delete(dataDoc);
            mRedisDS.finishMarker();

            redisSearch.dropIndex(dataGrid);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseProductSearchAsJSON(boolean anIsFullJSONLoad)
    {
        try
        {
            boolean isOK;
            List<DataDoc> dataDocList;

            JsonDS jsonDS = new JsonDS(mAppCtx);
            jsonDS.loadSchema("data/product_electronics_json.xml");
            if (anIsFullJSONLoad)
            {
                DataDocJSON dataDocJSON = new DataDocJSON();
                dataDocList = dataDocJSON.loadList("data/product_electronics.json");
                Assert.assertNotNull(dataDocList);
                isOK = jsonDS.loadDataEvaluatePath(dataDocList);
            }
            else
            {
                dataDocList = null;
                isOK = jsonDS.loadDataEvaluatePath("data/product_electronics.json");
            }
            Assert.assertTrue(isOK);
            DataDoc dataSchemaDoc = jsonDS.getSchema();
            DataDoc searchSchemaDoc = loadSchema("data/product_electronics_search_json.xml");
            searchSchemaDoc.addFeature(Redis.FEATURE_INDEX_NAME, "product_electronics");
            RedisSearch redisSearch = mRedisDS.createSearch(Redis.Document.JSON, dataSchemaDoc, searchSchemaDoc);

            mRedisDS.startMarker("Create Schema");
            redisSearch.createIndexSaveSchema(searchSchemaDoc);
            mRedisDS.finishMarker();

            DataGrid dataGrid = jsonDS.getDataGrid();
            mRedisDS.startMarker("Add Documents");
            if (anIsFullJSONLoad)
                redisSearch.add(dataDocList);
            else
                redisSearch.add(dataGrid);
            mRedisDS.finishMarker();

            exerciseProductSynonyms(redisSearch);
            exerciseProductQueries(redisSearch);
            exerciseSuggestions(redisSearch);

            mRedisDS.startMarker("Update Document");
            DataDoc dataDoc = dataGrid.getRowAsDoc(0);
            dataDoc.setValueByName("name", "Redis Labs");
            redisSearch.update(dataDoc);
            mRedisDS.finishMarker();

            mRedisDS.startMarker("Delete Document");
            dataDoc = dataGrid.getRowAsDoc(0);
            mRedisDS.createDoc().delete(dataDoc);
            mRedisDS.finishMarker();

            redisSearch.dropIndex(dataGrid);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseRealEstateSearch()
    {
        DataDoc dataDoc;

        DataDoc dataSchemaDoc = loadSchema("data/real_estate.xml");
        DataDoc searchSchemaDoc = loadSchema("data/real_estate_search.xml");
        RedisSearch redisSearch = mRedisDS.createSearch(Redis.Document.Hash, dataSchemaDoc, searchSchemaDoc);
        try
        {
            mRedisDS.startMarker("Create Schema");
            redisSearch.createIndexSaveSchema(searchSchemaDoc);
            mRedisDS.finishMarker();

            DataGridCSV dataGridCSV = new DataGridCSV(dataSchemaDoc);
            dataGridCSV.load("data/real_estate.csv", false);
            DataGrid dataGrid = dataGridCSV.getDataGrid();

            mRedisDS.startMarker("Add Documents");
            int rowCount = dataGrid.rowCount();
            for (int row = 0; row < rowCount; row++)
            {
                dataDoc = dataGrid.getRowAsDoc(row);
                redisSearch.add(dataDoc);
            }
            mRedisDS.finishMarker();

            DSCriteria dsCriteria = new DSCriteria("Real Estate Criteria");
            dsCriteria.add(Redis.RS_QUERY_OFFSET, Data.Operator.EQUAL, 0);
            dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, 10);
            dsCriteria.add("property_type", Data.Operator.EQUAL, "Residential");
            dsCriteria.add("location", Data.Operator.GEO_LOCATION, "38.621188", "38.621188", "0.15", "mi");
            dataGrid = redisSearch.query(dsCriteria);
            Assert.assertEquals(8, dataGrid.rowCount());
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);

// While we are saving the commands to a stream above, we use the RedisDS to save them to the file system.
// You validated via 'redis-cli' that these were created correctly.

            redisSearch.dropIndex(true);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void exerciseAppStudio()
    {
        String indexName = "hr_employee_records_1k";

        try
        {
            RedisSearch redisSearch = mRedisDS.createSearch(Redis.Document.Hash);
            DataDoc dataSchemaDoc = loadSchema("data/hr_employee_records.xml");
            dataSchemaDoc.setName(indexName);
            redisSearch.setDataSchema(dataSchemaDoc);
            DataDoc searchSchemaDoc = redisSearch.createSchemaDoc(dataSchemaDoc, indexName);
            searchSchemaDoc.getItemByName("father_name").enableFeature(Redis.FEATURE_IS_PHONETIC);
            searchSchemaDoc.getItemByName("mother_name").enableFeature(Redis.FEATURE_IS_PHONETIC);
            searchSchemaDoc.addFeature(Redis.FEATURE_INDEX_NAME, indexName);
            redisSearch.setSearchSchema(searchSchemaDoc);

            DataGridCSV dataGridCSV = new DataGridCSV(dataSchemaDoc);
            dataGridCSV.load("data/hr_employee_records_100.csv", false);
            DataGrid dataGrid = dataGridCSV.getDataGrid();

            mRedisDS.startMarker("Add data grid and create search index");
            redisSearch.add(dataGrid);
            Assert.assertTrue(redisSearch.schemaExists());
            mRedisDS.finishMarker();

            mRedisDS.startMarker("Exercise schema operations");
            Optional<DataDoc> optRedisDBSchema = redisSearch.loadSchema();
            Assert.assertTrue(optRedisDBSchema.isPresent());
            DataDoc redisDBSchemaDoc = optRedisDBSchema.get();
            DataDocDiff dataDocDiff = new DataDocDiff();
            dataDocDiff.compare(searchSchemaDoc, redisDBSchemaDoc);
            Assert.assertTrue(dataDocDiff.isEqual());
            PrintWriter printWriter = new PrintWriter(System.out, true);
            DataGridConsole dataGridConsole = new DataGridConsole();
            // Update and save the schema to Redis
            DataGrid schemaGrid = redisSearch.schemaDocToDataGrid(redisDBSchemaDoc, false);
            schemaGrid.setValueByRowName(2, "field_weight", 1.0);
            DataDoc uiSchemaDoc = schemaGrid.getRowAsDoc(2);
            Assert.assertTrue(redisSearch.updateSchema(uiSchemaDoc));
            redisSearch.saveSchemaDefinition();
            // Make sure facet changes are not seen as index rebuild events
            schemaGrid = redisSearch.schemaDocToDataGrid(redisSearch.getSearchSchema(), false);
            schemaGrid.setValueByRowName(1, Redis.FEATURE_IS_FACET_FIELD, true);
            uiSchemaDoc = schemaGrid.getRowAsDoc(1);
            Assert.assertFalse(redisSearch.updateSchema(uiSchemaDoc));
            redisSearch.saveSchemaDefinition();
            dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName(), 40, 1);
            mRedisDS.finishMarker();

            DSCriteria dsCriteria = new DSCriteria("HR Employee Criteria");
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dsCriteria.add("position_title", Data.Operator.FACET, true);
            dsCriteria.add("industry_focus", Data.Operator.FACET, true);
            dsCriteria.add("region", Data.Operator.FACET, true);
            dsCriteria.add("position_title", Data.Operator.EQUAL, "Accountant");
            DataGrid resultGrid = redisSearch.query(dsCriteria);
            String gridTitle = String.format("%s - 'Facet Search Filter Test'", resultGrid.getName());
            dataGridConsole.write(resultGrid, printWriter, gridTitle, 40, 1);

            dsCriteria.reset();
            DSCriterion dsCriterion = new DSCriterion("date_of_joining", Data.Operator.GREATER_THAN, "6/4/16");
            dsCriterion.setDataFormat(searchSchemaDoc.getItemByName("date_of_joining").getDataFormat());
            dsCriteria.add(dsCriterion);
            resultGrid = redisSearch.query(dsCriteria);
            gridTitle = String.format("%s - 'Shadow Date Test'", resultGrid.getName());
            dataGridConsole.write(resultGrid, printWriter, gridTitle, 40, 1);

            String prefixString = "mar";
            DataGrid suggestGrid = redisSearch.getSuggestions(prefixString, Redis.SUGGESTION_LIMIT_DEFAULT, false);
            gridTitle = String.format("%s - '%s'", suggestGrid.getName(), prefixString);
            dataGridConsole.write(suggestGrid, printWriter, gridTitle, 40, 1);

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, 15);
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dsCriteria.add("position_title", Data.Operator.FACET, true);
            dsCriteria.add("industry_focus", Data.Operator.FACET, true);
            dsCriteria.add("region", Data.Operator.FACET, true);
            DataGrid facetGrid = redisSearch.calculateUIFacets(dsCriteria);
            gridTitle = String.format("%s - 'Facet Test'", resultGrid.getName());
            dataGridConsole.write(facetGrid, printWriter, gridTitle, 40, 1);

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_LIMIT, Data.Operator.EQUAL, 15);
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dsCriteria.add("position_title", Data.Operator.FACET, true);
            dsCriteria.add("position_title", Data.Operator.EQUAL, "Accountant");
            dsCriteria.add("industry_focus", Data.Operator.FACET, true);
            dsCriteria.add("industry_focus", Data.Operator.EQUAL, "Subsidiary or Business Segment");
            dsCriteria.add("region", Data.Operator.FACET, true);
            dsCriteria.add("region", Data.Operator.EQUAL, "West");
            facetGrid = redisSearch.calculateUIFacets(dsCriteria);
            gridTitle = String.format("%s - 'Facet Test'", resultGrid.getName());
            dataGridConsole.write(facetGrid, printWriter, gridTitle, 40, 1);

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, "Manager");
            dsCriteria.add("position_title", Data.Operator.HIGHLIGHT, true);
            dsCriteria.add("job_description", Data.Operator.HIGHLIGHT, true);
            resultGrid = redisSearch.query(dsCriteria);
            gridTitle = String.format("%s - 'Highlight Test'", resultGrid.getName());
            dataGridConsole.write(resultGrid, printWriter, gridTitle, 40, 1);

            dsCriteria.reset();
            dsCriteria.add(Redis.RS_QUERY_STRING, Data.Operator.EQUAL, Redis.QUERY_ALL_DOCUMENTS);
            dsCriteria.add("full_name", Data.Operator.SORT, Data.Order.ASCENDING.name());
            resultGrid = redisSearch.query(dsCriteria);
            dataGridConsole.write(resultGrid, printWriter, resultGrid.getName(), 40, 1);

            DataDoc dataDoc = resultGrid.getRowAsDoc(5);
            mRedisDS.createDoc().delete(dataDoc);

            redisSearch.dropIndex(true);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    // Get the rest working above and then return to this for Hash/Json features
    public void exerciseHashJsonDocuments()
    {
        String indexName = "hr_employee_records_1k";

        PrintWriter printWriter = new PrintWriter(System.out, true);
        DataGridConsole dataGridConsole = new DataGridConsole();

        try
        {
            RedisSearch redisSearch = mRedisDS.createSearch(Redis.Document.Hash);
            DataDoc dataSchemaDoc = loadSchema("data/hr_employee_records.xml");
            dataSchemaDoc.setName(indexName);
            redisSearch.setDataSchema(dataSchemaDoc);
            DataDoc searchSchemaDoc = redisSearch.createSchemaDoc(dataSchemaDoc, indexName);
            DataGrid schemaGrid = redisSearch.schemaDocToDataGrid(searchSchemaDoc, false);
            dataGridConsole.write(schemaGrid, printWriter, schemaGrid.getName(), 40, 1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    @Test
    public void exercise()
        throws RedisDSException
    {
        mRedisDS.open(APPLICATION_PREFIX_DEFAULT);
        mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
        mRedisDS.createCore().flushDatabase();

        exerciseAppStudio();
        exerciseProductSearchAsJSON(false);
//        exerciseProductSearchAsJSON(true); - does not support shadow data fields in the search index
        exerciseProductSearchAsHashes();
        exerciseRealEstateSearch();
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
