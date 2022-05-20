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

import java.util.List;
import java.util.Optional;

/**
 * The Data Analyzer class will examine small-to-medium sized
 * data sets to determine their type and value composition.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataAnalyzer
{
	public final int SAMPLE_COUNT_DEFAULT = 10;

	private DataDoc mDoc;
	private DataGrid mGrid;
	private int mSampleCount;

	/**
	 * Constructor where the data document definition is used to identify
	 * the items and a default sample count is used.
	 *
	 * @param aSchemaDoc Data document instance.
	 */
	public DataAnalyzer(DataDoc aSchemaDoc)
	{
		initMembers(aSchemaDoc, SAMPLE_COUNT_DEFAULT);
	}

	/**
	 * Constructor where the data document definition is used to identify
	 * the items and a the sample count is driven by the parameter.
	 *
	 * @param aSchemaDoc Data document instance.
	 * @param aSampleCount Sample value count.
	 */
	public DataAnalyzer(DataDoc aSchemaDoc, int aSampleCount)
	{
		initMembers(aSchemaDoc, aSampleCount);
	}

	private boolean isTypesDifferent(DataDoc aSchemaDoc)
	{
		for (DataItem dataItem : aSchemaDoc.getItems())
		{
			if (! Data.isText(dataItem.getType()))
				return true;
		}

		return false;
	}

	private void initMembers(DataDoc aSchemaDoc, int aSampleCount)
	{
		String itemName;
		DataItemAnalyzer dataItemAnalyzer;

		mDoc = aSchemaDoc;
		mSampleCount = aSampleCount;
		boolean isTypesDifferent = isTypesDifferent(aSchemaDoc);
		dataItemAnalyzer = new DataItemAnalyzer("Data Item Analyzer");
		mGrid = new DataGrid(dataItemAnalyzer.createDefinition(aSampleCount));
		for (DataItem dataItem : aSchemaDoc.getItems())
		{
			itemName = dataItem.getName();
			if (isTypesDifferent)
				dataItemAnalyzer = new DataItemAnalyzer(itemName, dataItem.getType());
			else
				dataItemAnalyzer = new DataItemAnalyzer(itemName);
			mGrid.addProperty(itemName, dataItemAnalyzer);
		}
	}

	/**
	 * Scans the data item values from within the data document instance
	 * to determine their type and metric information.
	 *
	 * @param aDoc Data document instance.
	 */
	public void scan(DataDoc aDoc)
	{
		DataItemAnalyzer dataItemAnalyzer;

		for (DataItem dataItem : aDoc.getItems())
		{
			Optional<Object> optObject = mGrid.getProperty(dataItem.getName());
			if (optObject.isPresent())
			{
				dataItemAnalyzer = (DataItemAnalyzer) optObject.get();
				dataItemAnalyzer.scan(dataItem.getValue());
			}
		}
	}

	/**
	 * Scans the data item values from within the list of data documents
	 * to determine their type and metric information.
	 *
	 * @param aDataDocList List of data documents
	 */
	public void scan(List<DataDoc> aDataDocList)
	{
		aDataDocList.forEach(this::scan);
	}

	/**
	 * Scans the data item values from within the data grid instance
	 * to determine their type and metric information.
	 *
	 * @param aGrid Data grid instance.
	 */
	public void scan(DataGrid aGrid)
	{
		int rowCount = aGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
			scan(aGrid.getRowAsDoc(row));
	}

	/**
	 * Returns a data grid of items describing the scanned value data.
	 * The table will contain the item name, derived type, populated
	 * and unique counts, null count and a sample count of values (with
	 * overall percentages) that repeated most often.
	 *
	 * @return Data grid of analysis details.
	 */
	public DataGrid getDetails()
	{
		DataDoc dfaDoc;
		DataItemAnalyzer dataItemAnalyzer;

		for (DataItem dataItem : mDoc.getItems())
		{
			Optional<Object> optObject = mGrid.getProperty(dataItem.getName());
			if (optObject.isPresent())
			{
				dataItemAnalyzer = (DataItemAnalyzer) optObject.get();
				dfaDoc = dataItemAnalyzer.getDetails(mSampleCount);
				mGrid.addRow(dfaDoc);
			}
		}
		mGrid.clearGridProperties();

		return mGrid;
	}
}
