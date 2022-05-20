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

import com.redis.ds.ds_redis.time_series.RedisTimeseries;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridCSV;
import com.redis.foundation.io.DataGridConsole;
import com.redis.foundation.std.Sleep;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang.time.DateUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

/**
 * The RedisTimeSeriesTest class will exercise classes and methods for
 * the Redis data source library package.
 */
public class RedisTimeSeriesTest
{
    private final String APPLICATION_PREFIX_DEFAULT = "ASRT";

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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public String createLogPathFile()
    {
        String logPathFileName = String.format("log%credis_timeseries_commands.txt", File.separatorChar);

        File logFile = new File (logPathFileName);
        if (logFile.exists())
            logFile.delete();

        return logPathFileName;
    }

    private DataGrid loadStocks(String aPathFileName)
    {
        Optional<DataGrid> optDataGrid;
        DataGridCSV dataGridCSV = new DataGridCSV();
        dataGridCSV.setDateTimeFormat(Data.FORMAT_SQLISODATE_DEFAULT);
        try
        {
            optDataGrid = dataGridCSV.load(aPathFileName, true);
        }
        catch (Exception e)
        {
            optDataGrid = Optional.empty();
            System.err.printf("Exception: %s", e.getMessage());
        }
        Assert.assertTrue(optDataGrid.isPresent());

        return optDataGrid.get();
    }

    private DataDoc loadLabels(String aPathFileName)
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

    private void add_stock_prices(RedisTimeseries aRedisTS, String anExchange, String aSymbol,
                                  String anItemName, boolean aIsRuleEnabled)
        throws RedisDSException
    {
        String srcKeyName = aRedisTS.timeSeriesKeyName(String.format("%s:%s:%s", anExchange, aSymbol, anItemName));
        String dstKeyName = aRedisTS.timeSeriesKeyName(String.format("%s:rule", srcKeyName));

        DataDoc tsLabels = loadLabels(String.format("data/%s_%s.xml", anExchange, aSymbol));
        tsLabels.setValueByName("stock_event", Data.nameToTitle(anItemName));
        if (aIsRuleEnabled)
        {
            tsLabels.addFeature(Redis.FEATURE_KEY_NAME, srcKeyName);
            aRedisTS.create(tsLabels);
            tsLabels.addFeature(Redis.FEATURE_KEY_NAME, dstKeyName);
            aRedisTS.create(tsLabels);
            Assert.assertTrue(aRedisTS.createRule(srcKeyName, dstKeyName, Redis.Function.AVG, DateUtils.MILLIS_PER_DAY * 365));
        }

        DataGrid dgSamples = loadStocks(String.format("data/stock_%s.csv", aSymbol));
        dgSamples.addFeature(Redis.FEATURE_KEY_NAME, srcKeyName);
        DataDoc ddSamples = dgSamples.getColumns();
        ddSamples.getItemByName(anItemName).enableFeature(Redis.FEATURE_IS_VALUE);
        ddSamples.getItemByName("transaction_date").enableFeature(Redis.FEATURE_IS_TIMESTAMP);
        ddSamples.addChild(Redis.RT_CHILD_LABELS, tsLabels);
        Assert.assertTrue(aRedisTS.add(dgSamples));
    }

    private void load_exchange_stocks(RedisTimeseries aRedisTS)
    {
        try
        {
            add_stock_prices(aRedisTS, "nasdaq", "ebay", "price_open", true);
            add_stock_prices(aRedisTS, "nasdaq", "ebay", "price_high", true);
            add_stock_prices(aRedisTS, "nasdaq", "ebay", "price_low", true);
            add_stock_prices(aRedisTS, "nasdaq", "ebay", "price_close", true);

            add_stock_prices(aRedisTS, "nasdaq", "appl", "price_open", false);
            add_stock_prices(aRedisTS, "nasdaq", "appl", "price_high", false);
            add_stock_prices(aRedisTS, "nasdaq", "appl", "price_low", false);
            add_stock_prices(aRedisTS, "nasdaq", "appl", "price_close", false);

            add_stock_prices(aRedisTS, "nyse", "ge", "price_open", false);
            add_stock_prices(aRedisTS, "nyse", "ge", "price_high", false);
            add_stock_prices(aRedisTS, "nyse", "ge", "price_low", false);
            add_stock_prices(aRedisTS, "nyse", "ge", "price_close", false);

            add_stock_prices(aRedisTS, "nyse", "gm", "price_open", false);
            add_stock_prices(aRedisTS, "nyse", "gm", "price_high", false);
            add_stock_prices(aRedisTS, "nyse", "gm", "price_low", false);
            add_stock_prices(aRedisTS, "nyse", "gm", "price_close", false);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    private void delete_exchange_stocks(RedisTimeseries aRedisTS)
    {
        try
        {
            String srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "ebay", "price_open"));
            String dstKeyName = aRedisTS.timeSeriesKeyName(String.format("%s:rule", srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(dstKeyName));

            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "ebay", "price_high"));
            dstKeyName = aRedisTS.timeSeriesKeyName(String.format("%s:rule", srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(dstKeyName));

            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "ebay", "price_low"));
            dstKeyName = aRedisTS.timeSeriesKeyName(String.format("%s:rule", srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(dstKeyName));

            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "ebay", "price_close"));
            dstKeyName = aRedisTS.timeSeriesKeyName(String.format("%s:rule", srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            Assert.assertEquals(1, aRedisTS.delete(dstKeyName));

            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "appl", "price_open"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "appl", "price_high"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "appl", "price_low"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "appl", "price_close"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            Sleep.forMilliseconds(500);

            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "ge", "price_open"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "ge", "price_high"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "ge", "price_low"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "ge", "price_close"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            Sleep.forMilliseconds(500);

            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "gm", "price_open"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "gm", "price_high"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "gm", "price_low"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
            srcKeyName = aRedisTS.timeSeriesKeyName(String.format("nyse:%s:%s", "gm", "price_close"));
            Assert.assertEquals(1, aRedisTS.delete(srcKeyName));
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_queries(RedisTimeseries aRedisTS)
    {
        DataGrid dataGrid;

        try
        {
            String keyName = aRedisTS.timeSeriesKeyName(String.format("nasdaq:%s:%s", "ebay", "price_open"));
            DataGridConsole dataGridConsole = new DataGridConsole();
            PrintWriter printWriter = new PrintWriter(System.out, true);

// Find the first 25 samples sorted in ascending order.

            DSCriteria dsCriteria = new DSCriteria("Product Criteria");
            dsCriteria.addFeature(Redis.FEATURE_KEY_NAME, keyName);
            dsCriteria.addFeature(Redis.FEATURE_START_TIMESTAMP, 0);
            dsCriteria.addFeature(Redis.FEATURE_FINISH_TIMESTAMP, new Date().getTime());
            dsCriteria.addFeature(Redis.FEATURE_SAMPLE_COUNT, 25);
            dsCriteria.addFeature(Redis.FEATURE_SORT_ORDER, Data.Order.ASCENDING.name());
            dataGrid = aRedisTS.queryRange(dsCriteria);
            Assert.assertEquals(25, dataGrid.rowCount());

// Find the first 25 samples sorted in descending order.

            dsCriteria.addFeature(Redis.FEATURE_SORT_ORDER, Data.Order.DESCENDING.name());
            dataGrid = aRedisTS.queryRange(dsCriteria);
            Assert.assertEquals(25, dataGrid.rowCount());
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);

// Find the average of all samples.

            dsCriteria.reset();
            dsCriteria.addFeature(Redis.FEATURE_KEY_NAME, keyName);
            dsCriteria.addFeature(Redis.FEATURE_TIME_BUCKET, DateUtils.MILLIS_PER_DAY * 365);
            dsCriteria.addFeature(Redis.FEATURE_FUNCTION_NAME, Redis.Function.AVG.name());
            dataGrid = aRedisTS.queryRange(dsCriteria);
            Assert.assertEquals(6, dataGrid.rowCount());

            dsCriteria.reset();
            dsCriteria.add("exchange_name", Data.Operator.NOT_EQUAL, "NYSE");
            dsCriteria.add("stock_symbol", Data.Operator.EQUAL, "EBAY");
            dsCriteria.addFeature(Redis.FEATURE_TIME_BUCKET, DateUtils.MILLIS_PER_DAY * 365);
            dsCriteria.addFeature(Redis.FEATURE_FUNCTION_NAME, Redis.Function.AVG.name());
            dataGrid = aRedisTS.queryRange(dsCriteria);
            Assert.assertEquals(8, dataGrid.rowCount());
            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);

// Underlying Java API call does not work properly when compared to the 'redis-cli' command
//            dsCriteria.reset();
//            dsCriteria.add("exchange_name", Data.Operator.NOT_EQUAL, "NASDAQ");
//            dataGrid = aRedisTS.queryKeys(dsCriteria);
//            dataGridConsole.write(dataGrid, printWriter, dataGrid.getName(), 40, 1);
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s%n", e.getMessage());
        }
    }

    public void exercise_create_add_query_delete()
    {
        RedisTimeseries redisTimeseries = mRedisDS.createTimeseries();
        Assert.assertNotNull(redisTimeseries);

        try
        {
            load_exchange_stocks(redisTimeseries);
            exercise_queries(redisTimeseries);
            delete_exchange_stocks(redisTimeseries);
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
        exercise_create_add_query_delete();
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
