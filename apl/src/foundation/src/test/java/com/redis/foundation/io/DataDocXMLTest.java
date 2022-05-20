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

package com.redis.foundation.io;

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class DataDocXMLTest
{
	@Before
	public void setup()
	{
	}

	private void addMenuItem(DataGrid aGrid, String aName, boolean aIsVegetarian, int aCalories, String aType)
	{
		aGrid.newRow();
		aGrid.setValueByName("name", aName);
		aGrid.setValueByName("vegetarian", aIsVegetarian);
		aGrid.setValueByName("calories", aCalories);
		aGrid.setValueByName("type", aType);
		aGrid.addRow();
	}

	private DataGrid createDataGrid()
	{
		DataDoc dataSchema = new DataDoc("Menu Dish Schema");
		dataSchema.add(new DataItem.Builder().name("name").title("Dish Name").build());
		dataSchema.add(new DataItem.Builder().type(Data.Type.Boolean).name("vegetarian").title("Is Vegetarian").build());
		dataSchema.add(new DataItem.Builder().type(Data.Type.Integer).name("calories").title("Calories").build());
		dataSchema.add(new DataItem.Builder().name("type").title("Meal Type").build());

		DataGrid dataGrid = new DataGrid("Menu Dishes", dataSchema);
		addMenuItem(dataGrid, "pork", false, 800, "MEAT");
		addMenuItem(dataGrid, "beef", false, 700, "MEAT");
		addMenuItem(dataGrid, "chicken", false, 400, "MEAT");
		addMenuItem(dataGrid, "french fries", true, 530, "OTHER");
		addMenuItem(dataGrid, "rice", true, 350, "OTHER");
		addMenuItem(dataGrid, "season fruit", true, 120, "OTHER");
		addMenuItem(dataGrid, "pizza", true, 550, "OTHER");
		addMenuItem(dataGrid, "prawns", false, 300, "FISH");
		addMenuItem(dataGrid, "salmon", false, 450, "FISH");

		return dataGrid;
	}

	public void saveAndLoadDataDocAsXML()
		throws Exception
	{
		DataGrid dataGrid = createDataGrid();
		List<DataDoc> dataDocList = dataGrid.getRowsAsDocList();

		DataDoc dataDoc1 = dataDocList.get(0);
		for (DataItem dataItem : dataDoc1.getItems())
		{
			if (dataItem.getName().equals("type"))
			{
				if (dataItem.getValue().equals("MEAT"))
				{
					dataItem.addFeature("isMeatEater", StrUtl.STRING_TRUE);
					dataItem.addFeature(Data.FEATURE_IS_HIDDEN, StrUtl.STRING_FALSE);
					dataItem.addFeature(Data.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
				}
			}
		}
		DataDocXML dataDocXML = new DataDocXML(dataDoc1);
		String pathFileName = "data/datadoc.xml";
		dataDocXML.save(pathFileName);

		dataDocXML = new DataDocXML();
		dataDocXML.load(pathFileName);
		DataDoc dataDoc2 = dataDocXML.getDataDoc();
		File xmlFile = new File(pathFileName);
		xmlFile.delete();

		Assert.assertTrue(dataDoc1.isItemValuesEqual(dataDoc2));
	}

	public void saveAndLoadDataDocAsXMLUsingString()
		throws Exception
	{
		DataGrid dataGrid = createDataGrid();
		List<DataDoc> dataDocList = dataGrid.getRowsAsDocList();

		DataDoc dataDoc1 = dataDocList.get(0);
		for (DataItem dataItem : dataDoc1.getItems())
		{
			if (dataItem.getName().equals("type"))
			{
				if (dataItem.getValue().equals("MEAT"))
				{
					dataItem.addFeature("isMeatEater", StrUtl.STRING_TRUE);
					dataItem.addFeature(Data.FEATURE_IS_HIDDEN, StrUtl.STRING_FALSE);
					dataItem.addFeature(Data.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
				}
			}
		}

		DataDocXML dataDocXML = new DataDocXML(dataDoc1);
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);
		dataDocXML.save(printWriter);
		String pathFileName = "data/datadoc.xml";
		FileOutputStream fileOutputStream = new FileOutputStream(pathFileName);
		fileOutputStream.write(stringWriter.toString().getBytes(StandardCharsets.UTF_8));

		dataDocXML = new DataDocXML();
		dataDocXML.load(pathFileName);
		DataDoc dataDoc2 = dataDocXML.getDataDoc();
		File xmlFile = new File(pathFileName);
		xmlFile.delete();

		Assert.assertTrue(dataDoc1.isItemValuesEqual(dataDoc2));
	}

	public void saveAndLoadProductXML()
		throws Exception
	{
		DataDocJSON dataDocJSON = new DataDocJSON();
		String inPathFileName = "data/ecommerce-all.json";
		Optional<DataDoc> optDataDoc = dataDocJSON.load(inPathFileName);
		Assert.assertTrue(optDataDoc.isPresent());
		DataDoc dataDoc1 = optDataDoc.get();

		dataDoc1.disableItemFeature(Data.FEATURE_IS_VISIBLE);
		dataDoc1.disableItemFeature(Data.FEATURE_IS_UPDATED);
		DataDocXML dataDocXML = new DataDocXML(dataDoc1);
		String pathFileName = "data/ecommerce-all.xml";
		dataDocXML.save(pathFileName);

		dataDocXML = new DataDocXML();
		dataDocXML.load(pathFileName);
		DataDoc dataDoc2 = dataDocXML.getDataDoc();
		File xmlFile = new File(pathFileName);
		xmlFile.delete();

		Assert.assertTrue(dataDoc1.isItemValuesEqual(dataDoc2));
	}

	public void loadAndValidateHeaderInfo()
		throws Exception
	{
		DataDocXML dataDocXML = new DataDocXML();
		dataDocXML.load("data/hr_employee_records_1k.xml");
		DataDoc dataDoc = dataDocXML.getDataDoc();
		Assert.assertEquals("HR Employee Records", dataDoc.getName());
	}

	@Test
	public void exerciseXMLOperations()
		throws Exception
	{
		loadAndValidateHeaderInfo();
//		saveAndLoadDataDocAsXML();
//		saveAndLoadDataDocAsXMLUsingString();
//		saveAndLoadProductXML();
	}

	@After
	public void cleanup()
	{
	}
}
