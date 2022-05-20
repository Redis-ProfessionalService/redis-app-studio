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
import com.redis.ds.ds_redis.json.RedisJson;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.SmartClientXML;
import com.redis.foundation.io.DataDocJSON;
import com.redis.foundation.io.DataGridConsole;
import com.redis.foundation.std.StrUtl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * The RedisItemTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisJsonTest
{
    private final String APPLICATION_PREFIX_DEFAULT = "ASRJ";

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
        mRedisDS.setApplicationPrefix(APPLICATION_PREFIX_DEFAULT);
    }

    private void createAggregateProductData()
    {
        final int PRODUCT_COUNT_TOTAL = 1000;
        final int PRODUCT_PER_FILE_TOTAL = 44;
        final String folderPath = "/Users/acole/Dropbox/DataSets/e-commerce/bestbuy/products";
        final String[] fileNames = {
                "product_smart-tv.json", "product_kitchen.json", "product_laptops.json", "product_speakers.json",
                "product_electronics.json", "product_movies.json", "product_tablets.json", "product_cameras.json",
                "product_iphones.json", "product_office.json", "product_toys.json", "product_car.json",
                "product_google.json", "product_paper.json", "product_washers.json", "product_desktops.json",
                "product_gps.json", "product_phones.json", "product_watches.json", "product_drones.json",
                "product_ipads.json", "product_refrigerators.json", "product_wireless.json"
        };

        int totalProducts = 0;
        String bbPathFileName;
        DataDocJSON dataDocJSON = new DataDocJSON();
        List<DataDoc> productDataDocs = new ArrayList<>();
        String productPathFileName = "data/ecommerce_products_1k.json";

        for (String fileName : fileNames)
        {
            bbPathFileName = String.format("%s%c%s", folderPath, File.separatorChar, fileName);
            try
            {
                List<DataDoc> dataDocList = dataDocJSON.loadList(bbPathFileName);
                int dataDocCount = dataDocList.size();
                System.out.printf("[%d total products]: %s%n", dataDocCount, bbPathFileName);
                if (dataDocCount > 0)
                {
                    int offsetIncrement = Math.round(dataDocCount / PRODUCT_PER_FILE_TOTAL) - 1;
                    for (int offset = 0; offset < dataDocCount; offset += offsetIncrement)
                    {
                        productDataDocs.add(dataDocList.get(offset));
                        totalProducts++;
                    }
                    System.out.printf("[%d total products]: %s%n%n", totalProducts, productPathFileName);
                }
                System.gc();
                if (totalProducts >= PRODUCT_COUNT_TOTAL)
                    break;
            }
            catch (IOException e)
            {
                System.err.printf("%s: %s", bbPathFileName ,e.getMessage());
            }
        }
        try
        {
            dataDocJSON.save(productPathFileName, productDataDocs);
            System.out.printf("[%d total products saved]: %s%n", totalProducts, productPathFileName);
        }
        catch (Exception e)
        {
            System.err.printf("%s: %s", productPathFileName ,e.getMessage());
        }
    }

    private void exerciseSaveLoadDocuments()
    {
        String keyName;
        DataDoc rjDataDoc;
        Optional<DataDoc> optDataDoc;

        JsonDS jsonDS = new JsonDS(mAppCtx);
        try
        {
            jsonDS.loadSchema("data/product_fitness.xml");
            RedisJson redisJson = mRedisDS.createJson(jsonDS.getSchema());

            DataDocJSON dataDocJSON = new DataDocJSON();
            List<DataDoc> dataDocList = dataDocJSON.loadList("data/product_fitness.json");
            if (dataDocList.size() > 0)
            {
                for (DataDoc jsonDataDoc : dataDocList)
                {
                    redisJson.add(jsonDataDoc);
                    keyName = jsonDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
                    optDataDoc = redisJson.getDoc(keyName);
                    if (optDataDoc.isPresent())
                    {
                        rjDataDoc = optDataDoc.get();
                        Assert.assertTrue(rjDataDoc.count() > 0);
                        redisJson.delete(keyName);
                    }
                    else
                        System.err.printf("Unable to load JSON document by key name: %s", keyName);
                }
            }
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    public void dataValidationTest()
    {
        JsonDS jsonDS = new JsonDS(mAppCtx);
        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(true);
        PrintWriter printWriter = new PrintWriter(System.out, true);
        try
        {
            jsonDS.loadSchema("data/store_data.xml");
            if (jsonDS.loadDataEvaluatePath("data/store_data.json"))
            {
                RedisJson redisJson = mRedisDS.createJson(jsonDS.getSchema());
                Optional<DataDoc> optDataDoc = redisJson.getDoc("ASRJ:RJ:RJ:D:JSON Product Fitness Data (Row 1):806305194737550397");
                if (optDataDoc.isPresent())
                {
                    DataDoc dataDoc = optDataDoc.get();
                    dataDoc.count();
                }
            }
            else
                dataGridConsole.write(jsonDS.getMessagesGrid(), printWriter, jsonDS.getMessagesGrid().getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    private void saveSmartClientSchema()
    {
        JsonDS jsonDS = new JsonDS(mAppCtx, false);
        jsonDS.setErrorTrackingFlag(false);
        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(true);
        PrintWriter printWriter = new PrintWriter(System.out, true);
        try
        {
            jsonDS.loadSchema("data/product_fitness.xml");
            boolean isOK = jsonDS.loadDataEvaluatePath("data/product_fitness.json");
            DataGrid jsonDataGrid = jsonDS.getDataGrid();
            jsonDS.setName("Product Fitness Data");
            if (isOK)
            {
                dataGridConsole.write(jsonDataGrid, printWriter, jsonDataGrid.getName(), 40, 1);
                SmartClientXML smartClientXML = new SmartClientXML();
                smartClientXML.save("data/product_fitness_sc.xml", jsonDS.getSchema());
            }
            else
            {
                DataGrid msgGrid = jsonDS.getMessagesGrid();
                dataGridConsole.write(msgGrid, printWriter, jsonDataGrid.getName() + " - Errors", 40, 1);
            }
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    private void exerciseCRUDOperations()
    {
        JsonDS jsonDS = new JsonDS(mAppCtx, false);
        jsonDS.setErrorTrackingFlag(false);
        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(true);
        PrintWriter printWriter = new PrintWriter(System.out, true);
        try
        {
            jsonDS.loadSchema("data/product_fitness.xml");
            boolean isOK = jsonDS.loadDataEvaluatePath("data/product_fitness.json");
            DataGrid jsonDataGrid = jsonDS.getDataGrid();
            jsonDS.setName("Product Fitness Data");
            if (! isOK)
            {
                DataGrid msgGrid = jsonDS.getMessagesGrid();
                dataGridConsole.write(msgGrid, printWriter, jsonDataGrid.getName() + " - Errors", 40, 1);
            }

            RedisJson redisJson = mRedisDS.createJson(jsonDS.getSchema());
            redisJson.add(jsonDataGrid);
            String keyName = jsonDataGrid.getFeature(Redis.FEATURE_KEY_NAME);
            Optional<DataGrid> optDataGrid = redisJson.getGridData(keyName);
            if (optDataGrid.isPresent())
            {
                DataDoc dbDoc, jsonDoc;

                DataGrid dbDataGrid = optDataGrid.get();
                dbDataGrid.setName("Product Fitness Data");

                int rowCount = dbDataGrid.rowCount();
                for (int row = 0; row < rowCount; row++)
                {
                    dbDoc = dbDataGrid.getRowAsDoc(row);
                    jsonDoc = jsonDataGrid.getRowAsDoc(row);
                    if (! jsonDoc.isItemValuesEqual(dbDoc))
                    {
                        DataDocDiff dataDocDiff = new DataDocDiff();
                        dataDocDiff.compare(jsonDoc, dbDoc);
                        DataGrid diffGrid = dataDocDiff.getDetails();
                        dataGridConsole.write(diffGrid, printWriter, String.format("Row %d Difference", row), 40, 1);
                        Assert.assertTrue(jsonDoc.isItemValuesEqual(dbDoc));
                    }
                    else if (row == 0)
                    {
                        jsonDoc.addFeature(Redis.FEATURE_KEY_NAME, redisJson.getDataDocKeyNameByRowOffset(keyName, row));
                        jsonDoc.setValueByName("productId", 1000);
                        redisJson.update(dbDoc, jsonDoc);
                    }
                }
                DataDoc addJsonDoc = new DataDoc(dbDataGrid.getRowAsDoc(10));
                addJsonDoc.setName(String.format("Product Fitness Data (Row %d)", rowCount+1));
                addJsonDoc.setValueByName("sku", 10);
                addJsonDoc.setValueByName("productId", 100);
                redisJson.add(jsonDataGrid, addJsonDoc);
                String docKeyName = addJsonDoc.getFeature(Redis.FEATURE_KEY_NAME);
                Optional<DataDoc> optDataDoc = redisJson.getDoc(docKeyName);
                if (optDataDoc.isPresent())
                {
                    DataDoc dbJsonDoc = optDataDoc.get();
                    Assert.assertTrue(dbJsonDoc.count() > 0);
                }
                else
                    System.err.printf("Unable to load JSON document by key name: %s", docKeyName);

                jsonDoc = jsonDataGrid.getRowAsDoc(25);
                redisJson.delete(dbDataGrid, 25);
                jsonDoc.count();

                redisJson.delete(dbDataGrid);
            }
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    private void exerciseGridOperations()
    {
        JsonDS jsonDS = new JsonDS(mAppCtx, false);
        jsonDS.setErrorTrackingFlag(false);
        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(true);
        PrintWriter printWriter = new PrintWriter(System.out, true);
        try
        {
            jsonDS.loadSchema("data/product_fitness.xml");
            boolean isOK = jsonDS.loadDataEvaluatePath("data/product_fitness.json");
            DataGrid jsonDataGrid1 = jsonDS.getDataGrid();
            jsonDS.setName("Product Fitness Data");
            if (! isOK)
            {
                DataGrid msgGrid = jsonDS.getMessagesGrid();
                dataGridConsole.write(msgGrid, printWriter, jsonDataGrid1.getName() + " - Errors", 40, 1);
            }

            RedisJson redisJson = mRedisDS.createJson(jsonDS.getSchema());
            redisJson.add(jsonDataGrid1);
            String keyName = jsonDataGrid1.getFeature(Redis.FEATURE_KEY_NAME);
            Optional<DataGrid> optDataGrid = redisJson.getGridData(keyName);
            if (optDataGrid.isPresent())
            {
                DataGrid dbDataGrid = optDataGrid.get();
                dbDataGrid.setName("Product Fitness Data");

                mRedisDS.startMarker("Load Grid Range");
                DataGrid jsonDataGrid2 = new DataGrid(jsonDataGrid1.getName());
                jsonDataGrid2.setColumns(jsonDataGrid1.getColumns());
                jsonDataGrid2.addFeature(Redis.FEATURE_KEY_NAME, keyName);
                redisJson.loadGridPipeline(jsonDataGrid2, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
                int rowCount1 = jsonDataGrid1.rowCount();
                int rowCount2 = jsonDataGrid2.rowCount();
                Assert.assertEquals(rowCount1, rowCount2);
                Assert.assertTrue(jsonDataGrid1.isGridRowValuesEqual(jsonDataGrid2));
                mRedisDS.finishMarker();

                redisJson.delete(dbDataGrid);
            }
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    @Test
    public void exercise()
        throws RedisDSException, IOException
    {
        mRedisDS.open(APPLICATION_PREFIX_DEFAULT);
        mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
        mRedisDS.createCore().flushDatabase();
//        createAggregateProductData();
        exerciseGridOperations();
        exerciseCRUDOperations();
        saveSmartClientSchema();
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
