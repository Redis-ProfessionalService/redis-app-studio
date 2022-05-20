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

package com.redis.ds.ds_json;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.io.DataDocJSON;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridConsole;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * The JsonDSTest class will exercise classes and methods for
 * the JsonDS package.
 */
public class JsonDSTest
{
    private final String JDS_PROPERTY_PREFIX = "jds";

    private AppCtx mAppCtx;

    @Before
    public void setup()
    {
        HashMap<String,Object> hmProperties = new HashMap<>();
        hmProperties.put(JDS_PROPERTY_PREFIX + ".host_name", "localhost");
        hmProperties.put(JDS_PROPERTY_PREFIX + ".port_number", 4455);
        hmProperties.put(JDS_PROPERTY_PREFIX + ".application_name", "JsonDS");
        mAppCtx = new AppCtx(hmProperties);
    }

    private void exerciseLoadData()
    {
        JsonDS jsonDS = new JsonDS(mAppCtx);
        DataGridConsole dataGridConsole = new DataGridConsole();
        dataGridConsole.setFormattedFlag(true);
        try
        {
            jsonDS.loadSchema("data/product_fitness.xml");
            jsonDS.loadData("data/product_fitness.json");
            jsonDS.setName("JSON Product Fitness Data");
            Assert.assertEquals(349, jsonDS.getDataGrid().rowCount());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    // https://github.com/json-path/JsonPath
    private void exerciseLoadDataEvaluatePath()
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
                jsonDS.setName("JSON Store Data");
                dataGridConsole.write(jsonDS.getDataGrid(), printWriter, jsonDS.getName());
            }
            else
                dataGridConsole.write(jsonDS.getMessagesGrid(), printWriter, jsonDS.getMessagesGrid().getName());
        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    private void exerciseJSONLoadSaveSchema()
        throws IOException
    {
        DataDocJSON dataDocJSON = new DataDocJSON();
        List<DataDoc> dataDocList = dataDocJSON.loadList("data/product_fitness.json");
        if (dataDocList.size() > 0)
        {
            DataDoc schemaDoc = new DataDoc(dataDocList.get(0));
            schemaDoc.resetValues();
            DataDocXML dataDocXML = new DataDocXML(schemaDoc);
            dataDocXML.save("data/product_fitness.xml");
        }
    }

    @Test
    public void exercise()
    {
        try
        {
            exerciseJSONLoadSaveSchema();
            exerciseLoadData();
            exerciseLoadDataEvaluatePath();

        }
        catch (Exception e)
        {
            System.err.printf("Exception: %s", e.getMessage());
        }
    }

    @After
    public void cleanup()
    {
    }
}
