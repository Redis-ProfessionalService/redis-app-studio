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

import com.redis.ds.ds_redis.core.RedisDoc;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * The RedisDocTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisDocTest
{
    private final String APPLICATION_PREFIX_DEFAULT = "ASRC";

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
        AppCtx appCtx = new AppCtx(hmProperties);
        mRedisDS = new RedisDS(appCtx);
    }

    public void exerciseDocuments()
        throws RedisDSException, IOException
    {
        RedisDoc redisDoc = mRedisDS.createDoc(true);
        mRedisDS.openCaptureWithFile(String.format("log%credis_doc_commands.txt", File.separatorChar));

        mRedisDS.startMarker("Add/Get DataDoc");
        DataDoc dataDoc1 = new DataDoc("Data Doc Test");
        dataDoc1.add(new DataItem.Builder().name("name1").value("value1").isSecret(true).build());
        dataDoc1.add(new DataItem.Builder().name("name two").value("value2").build());
        dataDoc1.add(new DataItem.Builder().name("name3").value("value3").build());
        dataDoc1.add(new DataItem.Builder().name("name4").value("value4").build());
        dataDoc1.add(new DataItem.Builder().name("name5").value("value five").build());
        dataDoc1.add(new DataItem.Builder().name("name6").value("value6").build());
        dataDoc1.add(new DataItem.Builder().name("name7").value("value7").build());
        dataDoc1.add(new DataItem.Builder().name("name8").value("value8").build());
        dataDoc1.add(new DataItem.Builder().name("name9").value("value9").build());
        String[] strValues = {"valueA", "valueB", "valueC", "valueD"};
        dataDoc1.add(new DataItem.Builder().name("name10").values(strValues).build());
        dataDoc1.add(new DataItem.Builder().name("name11").value(11).build());
        redisDoc.add(dataDoc1);

        DataDoc infoDoc = mRedisDS.createCore().info();
        Assert.assertTrue(infoDoc.count() > 0);

        String keyName = dataDoc1.getFeature(Redis.FEATURE_KEY_NAME);
        Optional<DataDoc> optDataDoc = redisDoc.getDoc(keyName);
        Assert.assertTrue(optDataDoc.isPresent());
        DataDoc dataDoc2 = optDataDoc.get();
        assertEquals(dataDoc1.count(), dataDoc2.count());
        mRedisDS.finishMarker(redisDoc.memoryUsage(dataDoc1));

        mRedisDS.startMarker("Update DataDoc");
        dataDoc2 = new DataDoc(dataDoc1);
        dataDoc2.resetValues();
        redisDoc.loadDoc(dataDoc2);
        Assert.assertTrue(dataDoc1.isItemValuesEqual(dataDoc2));

        dataDoc1.disableItemFeature(Data.FEATURE_IS_UPDATED);
        dataDoc1.setValueByName("name6", "value06");
        redisDoc.update(dataDoc1);
        dataDoc2.resetValues();
        redisDoc.loadDoc(dataDoc2);
        Assert.assertTrue(dataDoc1.isItemValuesEqual(dataDoc2));
        mRedisDS.finishMarker(redisDoc.memoryUsage(dataDoc1));
        redisDoc.delete(dataDoc1);

        mRedisDS.startMarker("Another & Update DataDoc");
        dataDoc1 = new DataDoc("Data Doc Test 1");
        dataDoc1.add(new DataItem.Builder().name("name1").value("value1").build());
        dataDoc1.add(new DataItem.Builder().name("name two").value("value2").build());
        dataDoc1.add(new DataItem.Builder().name("name3").value("value3").build());
        dataDoc1.add(new DataItem.Builder().name("name4").value("value4").build());
        dataDoc1.add(new DataItem.Builder().name("name5").value("value five").build());
        redisDoc.add(dataDoc1);

        dataDoc2 = new DataDoc("Data Doc Test 2");
        dataDoc2.add(new DataItem.Builder().name("name1").value("1").build());
        dataDoc2.add(new DataItem.Builder().name("name two").value("2").build());
        dataDoc2.add(new DataItem.Builder().name("name3").value("3").build());
        dataDoc2.add(new DataItem.Builder().name("name20").value("20").build());
        redisDoc.update(dataDoc1, dataDoc2);
        mRedisDS.finishMarker(redisDoc.memoryUsage(dataDoc1));
        redisDoc.delete(dataDoc1);

        mRedisDS.startMarker("Hierarchical DataDoc");
        DataDoc dataDocCB = new DataDoc("Data Doc Test B");
        dataDocCB.add(new DataItem.Builder().name("name1").value("1").build());
        dataDocCB.add(new DataItem.Builder().name("name two").value("2").build());
        dataDocCB.add(new DataItem.Builder().name("name3").value("3").build());
        dataDocCB.add(new DataItem.Builder().name("name20").value("20").build());

        DataDoc dataDocCA = new DataDoc("Data Doc Test A");
        dataDocCA.add(new DataItem.Builder().name("name1").value("1").build());
        dataDocCA.add(new DataItem.Builder().name("name two").value("2").build());
        dataDocCA.add(new DataItem.Builder().name("name3").value("3").build());
        dataDocCA.add(new DataItem.Builder().name("name20").value("20").build());
        dataDocCA.addChild("Child Of A", dataDocCB);

        dataDoc1.addChild("Child of 1", dataDocCA);
        redisDoc.add(dataDoc1);
        keyName = dataDoc1.getFeature(Redis.FEATURE_KEY_NAME);
        optDataDoc = redisDoc.getDoc(keyName);
        Assert.assertTrue(optDataDoc.isPresent());
        mRedisDS.finishMarker(redisDoc.memoryUsage(dataDoc1));
        redisDoc.delete(dataDoc1);

        dataDoc1.addFeature(Redis.FEATURE_KEY_NAME, "DOES NOT EXIST");
        redisDoc.delete(dataDoc1);
    }

    @Test
    public void exercise()
        throws RedisDSException, IOException
    {
        mRedisDS.open(APPLICATION_PREFIX_DEFAULT);
        mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
        mRedisDS.createCore().flushDatabase();
        exerciseDocuments();
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
