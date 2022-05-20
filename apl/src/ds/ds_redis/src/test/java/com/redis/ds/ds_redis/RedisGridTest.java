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

import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.std.StrUtl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;
import redis.clients.jedis.Jedis;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * The RedisGridTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisGridTest
{
    private final String APPLICATION_PREFIX_DEFAULT = "ASRC";

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

    public void exerciseGrid()
        throws RedisDSException, IOException, DSException, ParserConfigurationException, SAXException
    {
        RedisGrid redisGrid = mRedisDS.createGrid();

        GridDS gridDS = new GridDS(mAppCtx);
        gridDS.loadSchema("data/hr_employee_records.xml");
        gridDS.loadData("data/hr_employee_records_1k.csv", false);
        gridDS.setName("Employee Records");
        gridDS.setPrimaryKey("employee_id");

        DataGrid dataGrid1 = gridDS.getDataGrid();
        Assert.assertEquals(1000, dataGrid1.rowCount());
        mRedisDS.startMarker("Add Grid");
        redisGrid.add(dataGrid1);
        mRedisDS.finishMarker(redisGrid.memoryUsage(dataGrid1));

        /* Standalone test of new deletion logic
        mRedisDS.startMarker("Delete Grid Row");
        DataDoc delDataDoc = dataGrid1.getRowAsDoc(10);
        redisGrid.delete(dataGrid1, delDataDoc);
        mRedisDS.finishMarker();
        */

        mRedisDS.startMarker("Get Grid Schema");
        String keyName1 = dataGrid1.getFeature(Redis.FEATURE_KEY_NAME);
        Optional<DataGrid> optDataGrid2 = redisGrid.getGridSchema(keyName1);
        Assert.assertTrue(optDataGrid2.isPresent());
        DataGrid dataGrid2 = optDataGrid2.get();
        Assert.assertEquals(dataGrid1.colCount(), dataGrid2.colCount());
        mRedisDS.finishMarker();

        mRedisDS.startMarker("Get Grid");
        keyName1 = dataGrid1.getFeature(Redis.FEATURE_KEY_NAME);
        optDataGrid2 = redisGrid.getGridData(keyName1);
        Assert.assertTrue(optDataGrid2.isPresent());
        dataGrid2 = optDataGrid2.get();
        int rowCount1 = dataGrid1.rowCount();
        int rowCount2 = dataGrid2.rowCount();
        Assert.assertEquals(rowCount1, rowCount2);
        Assert.assertTrue(dataGrid1.isGridRowValuesEqual(dataGrid2));
        mRedisDS.finishMarker(redisGrid.memoryUsage(dataGrid1));

        mRedisDS.startMarker("Load Grid");
        dataGrid2.emptyRows();
        redisGrid.loadGrid(dataGrid2);
        rowCount2 = dataGrid2.rowCount();
        Assert.assertEquals(rowCount1, rowCount2);
        Assert.assertTrue(dataGrid1.isGridRowValuesEqual(dataGrid2));
        mRedisDS.finishMarker(redisGrid.memoryUsage(dataGrid1));

        mRedisDS.startMarker("Load Grid Range");
        dataGrid2.emptyRows();
        redisGrid.loadGridPipeline(dataGrid2, Redis.GRID_RANGE_START, Redis.GRID_RANGE_FINISH);
        rowCount2 = dataGrid2.rowCount();
        Assert.assertEquals(rowCount1, rowCount2);
        Assert.assertTrue(dataGrid1.isGridRowValuesEqual(dataGrid2));
        mRedisDS.finishMarker(redisGrid.memoryUsage(dataGrid2));

        mRedisDS.startMarker("Get Row 5 in Grid");
        Optional<DataDoc> optDataDoc = redisGrid.getRowAsDoc(dataGrid1, 5);
        mRedisDS.finishMarker();
        if (optDataDoc.isPresent())
        {
            RedisCore redisCore = mRedisDS.createCore();
            DataItem lockItem = redisCore.createLock(dataGrid1.getFeature(Redis.FEATURE_KEY_NAME),
                                                        Redis.LOCK_RELEASE_TIMEOUT_DEFAULT,
                                                        Redis.LOCK_WAITFOR_TIMEOUT_DEFAULT);
            mRedisDS.startMarker("Lock delete/insert/update/delete/add");
            Assert.assertTrue(redisCore.acquireLock(lockItem));
            DataDoc dataDoc = optDataDoc.get();
            redisGrid.delete(dataGrid1, 5);
            redisGrid.insert(dataGrid1, 4, dataDoc);
            dataDoc.setValueByName("first_name", "Al Cole");
            redisGrid.update(dataGrid1, 5, dataDoc);
            redisGrid.delete(dataGrid1, 5);
            redisGrid.add(dataGrid1, dataDoc);
            redisCore.releaseLock(lockItem);
            mRedisDS.finishMarker();
        }
        mRedisDS.startMarker("Delete Grid");
        redisGrid.delete(dataGrid1);
        mRedisDS.finishMarker();

        String streamKeyName = mRedisDS.streamKeyName();
        String streamPathFileName = String.format("log%credis_grid_commands.txt", File.separatorChar);
        mRedisDS.saveStreamAsFile(streamKeyName, streamPathFileName);

        GridDS redisCmdDocDS = new GridDS(mAppCtx);
        redisCmdDocDS.loadData("data/redis_commands.csv", true);
        DataGrid redisCmdDocGrid = redisGrid.loadGridCommandsFromStream(streamKeyName, Redis.STREAM_START_DEFAULT, Redis.STREAM_FINISH_DEFAULT,
                                                                        100, redisCmdDocDS);
        Assert.assertEquals(97, redisCmdDocGrid.rowCount());
    }

    @Test
    public void exercise()
        throws RedisDSException, IOException, DSException, ParserConfigurationException, SAXException
    {
        mRedisDS.open(APPLICATION_PREFIX_DEFAULT);
        mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
        mRedisDS.createCore().flushDatabase();
        exerciseGrid();
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
