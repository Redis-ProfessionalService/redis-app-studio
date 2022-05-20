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

import com.redis.foundation.io.DataGridConsole;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.util.Optional;

public class DataDocTest
{
	@Before
	public void setup()
	{
	}

	public void exercise1()
	{
		DataDoc dataDoc = new DataDoc("Data Doc Test");
		dataDoc.add(new DataItem.Builder().name("name1").value("value1").build());
		dataDoc.add(new DataItem.Builder().name("name2").value("value2").build());
		dataDoc.add(new DataItem.Builder().name("name3").value("value3").build());
		for (DataItem dataItem : dataDoc.getItems())
			System.out.printf("Data Item: %s%n", dataItem.toString());
	}

	public void exercise2()
	{
		DataDoc dataDoc1 = new DataDoc("Data Doc Test");
		dataDoc1.add(new DataItem.Builder().name("name1").value("value1").build());
		dataDoc1.add(new DataItem.Builder().name("name2").value("value2").build());
		dataDoc1.add(new DataItem.Builder().name("name3").value("value3").build());

		DataDoc dataDoc2 = new DataDoc("Data Doc Test");
		dataDoc2.add(new DataItem.Builder().name("name1").value("value changed").build());
		dataDoc2.add(new DataItem.Builder().name("name3").value("value3").build());
		dataDoc2.add(new DataItem.Builder().name("name4").value("value4").build());
		dataDoc2.add(new DataItem.Builder().name("name5").value("value5").build());

		DataDocDiff dataDocDiff = new DataDocDiff();
		dataDocDiff.compare(dataDoc1, dataDoc2);
		if (! dataDocDiff.isEqual())
		{
			DataGrid dataGrid = dataDocDiff.getDetails();
			DataGridConsole dataGridConsole = new DataGridConsole();
			dataGridConsole.write(dataGrid, new PrintWriter(System.out, true), "Document Differences");
			Optional<DataDoc> optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_ADDED);
			if (optDataDoc.isPresent())
			{
				DataDoc changeDataDoc = optDataDoc.get();
				changeDataDoc.count();
				assertTrue(changeDataDoc.count() == 2);
			}
			optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_UPDATED);
			if (optDataDoc.isPresent())
			{
				DataDoc changeDataDoc = optDataDoc.get();
				changeDataDoc.count();
				assertTrue(changeDataDoc.count() == 1);
			}
			optDataDoc = dataDocDiff.changedItems(Data.DIFF_STATUS_DELETED);
			if (optDataDoc.isPresent())
			{
				DataDoc changeDataDoc = optDataDoc.get();
				changeDataDoc.count();
				assertTrue(changeDataDoc.count() == 1);
			}
		}
	}

	@Test
	public void exercise()
	{
		exercise1();
		exercise2();
	}

	@After
	public void cleanup()
	{
	}
}
