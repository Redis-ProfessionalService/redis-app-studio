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

import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.core.RedisItem;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
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

/**
 * The RedisItemTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisItemTest
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

    public void exerciseKeys()
    {
        RedisKey redisKey = mRedisDS.createKey();

        DataItem dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").value("Another message or readiness").build();
        String keyName = redisKey.moduleCore().redisString().dataObject(dataItem1).name();
        Optional<DataItem> optDataItem = redisKey.toDataItem(keyName);
        Assert.assertTrue(optDataItem.isPresent());
        DataItem dataItem2 = optDataItem.get();
        Assert.assertEquals(dataItem1.getName(), dataItem2.getName());

        String[] testValues = {"Hello World!", "Good Morning!", "Good Afternoon!"};
        DataItem dataItem = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").values(testValues).build();
        redisKey.reset();
        keyName = redisKey.moduleCore().redisString().dataObject(dataItem).name();
        optDataItem = redisKey.toDataItem(keyName);
        Assert.assertTrue(optDataItem.isPresent());
        dataItem2 = optDataItem.get();
        Assert.assertTrue(dataItem2.isFeatureTrue(Data.FEATURE_IS_MULTIVALUE));

        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").value("Another message or readiness").build();
        dataItem1.enableFeature(Data.FEATURE_IS_SECRET);
        keyName = redisKey.moduleCore().redisString().dataObject(dataItem1).name();
        optDataItem = redisKey.toDataItem(keyName);
        Assert.assertTrue(optDataItem.isPresent());
        dataItem2 = optDataItem.get();
        Assert.assertTrue(dataItem2.isFeatureTrue(Data.FEATURE_IS_SECRET));

        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").value("Another message or readiness").build();
        keyName = redisKey.moduleCore().redisString().dataObject(dataItem1).randomId().name();
        optDataItem = redisKey.toDataItem(keyName);
        Assert.assertTrue(optDataItem.isPresent());
        dataItem2 = optDataItem.get();
        Assert.assertEquals(dataItem1.getName(), dataItem2.getName());

        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").value("Another message or readiness").build();
        keyName = redisKey.moduleCore().redisString().dataObject(dataItem1).hashId().name();
        optDataItem = redisKey.toDataItem(keyName);
        Assert.assertTrue(optDataItem.isPresent());
        dataItem2 = optDataItem.get();
        Assert.assertEquals(dataItem1.getName(), dataItem2.getName());
    }

    public void exerciseItems()
        throws RedisDSException, IOException
    {
        RedisItem redisItem = mRedisDS.createItem();
        mRedisDS.openCaptureWithFile(String.format("log%credis_item_commands.txt", File.separatorChar));

        mRedisDS.startMarker("Add & Get DataItem");
        String[] testValues = {"Hello World!", "Good Morning!", "Good Afternoon!"};
        DataItem dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").values(testValues).build();
        redisItem.add(dataItem1);
        Optional<DataItem> optDataItem2 = redisItem.getItem(dataItem1);
        Assert.assertTrue(optDataItem2.isPresent());
        DataItem dataItem2 = optDataItem2.get();
        Assert.assertTrue(dataItem1.isEqual(dataItem2));
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));

        mRedisDS.startMarker("Update DataItem");
        dataItem1.addValue("Good Evening!");
        redisItem.update(dataItem1);
        optDataItem2 = redisItem.getItem(dataItem1);
        Assert.assertTrue(optDataItem2.isPresent());
        dataItem2 = optDataItem2.get();
        Assert.assertTrue(dataItem1.isEqual(dataItem2));
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));
        redisItem.delete(dataItem1);

        mRedisDS.startMarker("Message DataItem");
        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").value("Another message or readiness").build();
        redisItem.add(dataItem1);
        optDataItem2 = redisItem.getItem(dataItem1);
        Assert.assertTrue(optDataItem2.isPresent());
        dataItem2 = optDataItem2.get();
        Assert.assertTrue(dataItem1.isEqual(dataItem2));
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));
        redisItem.delete(dataItem1);

        mRedisDS.startMarker("Multi-value DataItem");
        String[] fruitValues = {"Apple", "Orange", "Banana", "Peach"};
        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("fruit_field").title("Fruit").values(fruitValues).build();
        redisItem.addSet(dataItem1);
        optDataItem2 = redisItem.getSet(dataItem1);
        Assert.assertTrue(optDataItem2.isPresent());
        dataItem2 = optDataItem2.get();
        // The value order can be different, so just compare value counts
        Assert.assertEquals(dataItem1.getValues().size(), dataItem2.getValues().size());
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));
        redisItem.delete(dataItem1);

        mRedisDS.startMarker("Secret DataItem");
        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").value("Another message or readiness").build();
        dataItem1.enableFeature(Data.FEATURE_IS_SECRET);
        redisItem.add(dataItem1);
        optDataItem2 = redisItem.getItem(dataItem1);
        Assert.assertTrue(optDataItem2.isPresent());
        dataItem2 = optDataItem2.get();
        Assert.assertTrue(dataItem1.isEqual(dataItem2));
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));
        redisItem.delete(dataItem1);

        mRedisDS.startMarker("Secret Multi-value DataItem");
        String[] hiddenValues = {"Secret 1", "Secret 2", "Secret 3"};
        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("message_field").title("Message").values(hiddenValues).build();
        dataItem1.enableFeature(Data.FEATURE_IS_SECRET);
        redisItem.add(dataItem1);
        optDataItem2 = redisItem.getItem(dataItem1);
        Assert.assertTrue(optDataItem2.isPresent());
        dataItem2 = optDataItem2.get();
        Assert.assertTrue(dataItem1.isEqual(dataItem2));
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));
        redisItem.delete(dataItem1);

        mRedisDS.startMarker("Unique DataItem Values");
        String[] fruitArray = {"Apple", "Orange", "Banana", "Peach", "Apple"};
        dataItem1 = new DataItem.Builder().type(Data.Type.Text).name("fruit_field").title("Fruit").build();
        for (String fruitItem : fruitArray)
        {
            dataItem1.setValue(fruitItem);
            redisItem.addCounter(dataItem1);
        }
        long uniqueFruit = redisItem.getCounter(dataItem1);
        Assert.assertEquals(uniqueFruit, 4);
        mRedisDS.finishMarker(redisItem.memoryUsage(dataItem1));
        redisItem.delete(dataItem1);

        RedisCore redisCore = mRedisDS.createCore();
        String keyName = "DeleteTest";
        redisCore.set(keyName, "Value");
        Assert.assertEquals(redisCore.delete(keyName), 1);
        Assert.assertEquals(redisCore.delete("Missing Key"), 0);
    }

    @Test
    public void exercise()
        throws RedisDSException, IOException
    {
        mRedisDS.open(APPLICATION_PREFIX_DEFAULT);
        mRedisDS.openCaptureWithStream(mRedisDS.streamKeyName());
        mRedisDS.createCore().flushDatabase();
        exerciseKeys();
        exerciseItems();
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
