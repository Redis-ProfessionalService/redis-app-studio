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

package com.redis.foundation.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class DataItemTest
{
    @Before
    public void setup()
    {
    }

    @Test
    public void builderShouldAssignAllFields()
    {
        String[] testValues = {"Hello World!", "I am ready for you...", "Come and get me!"};
        DataItem dataItem = new DataItem.Builder()
                .type(Data.Type.Text)
                .name("field_name")
                .title("Field Title")
                .isRequired(true)
                .displaySize(100)
                .isStored(true)
                .storedSize(255)
                .values(testValues)
                .build();
        assertEquals(Data.Type.Text, dataItem.getType());
        assertEquals(255, dataItem.getStoredSize());
        assertEquals(100, dataItem.getDisplaySize());
        assertTrue(dataItem.isFeatureTrue(Data.FEATURE_IS_REQUIRED));
        assertTrue(dataItem.isFeatureTrue(Data.FEATURE_IS_STORED));
        assertEquals("field_name", dataItem.getName());
        assertEquals("Field Title", dataItem.getTitle());
        int index = 0;
        for (String value : dataItem.getValues())
        {
            assertEquals(testValues[index], value);
            index++;
        }
        dataItem.setValue("Smile Today!");
        assertEquals("Smile Today!", dataItem.getValue());
        dataItem.setValue(true);
        dataItem.setValue(100);
        dataItem.setValue(1000L);
    }

    @Test
    public void validateClone()
    {
        String[] testValues = {"Hello World!", "I am ready for you...", "Come and get me!"};
        DataItem dataItem1 = new DataItem.Builder()
                .type(Data.Type.Text)
                .name("field_name")
                .title("Field Title")
                .isVisible(true)
                .isRequired(true)
                .displaySize(100)
                .isStored(true)
                .storedSize(255)
                .values(testValues)
                .build();

        DataItem dataItem2 = new DataItem(dataItem1);
        dataItem2.clearValues();
        dataItem2.clearFeatures();

        dataItem1.featureCount();
    }

    @After
    public void cleanup()
    {
    }
}
