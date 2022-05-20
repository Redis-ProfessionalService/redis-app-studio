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

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;

/**
 * The DataDocDiff is responsible for comparing two DataDoc
 * instances and determining if they differ and how.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataDocDiff
{
	private DataGrid mDataGrid;
	private boolean mIsCompared;
	private boolean mIsFeatureCompared;

	public DataDocDiff()
	{
		initGrid();
	}

	public DataDocDiff(boolean anIsFeaturesCompared)
	{
		initGrid();
		setFeatureComparisonFlag(anIsFeaturesCompared);
	}

	private void initGrid()
	{
		mDataGrid = new DataGrid("Data Doc Differences");
		mDataGrid.addCol(new DataItem.Builder().name("name").title("Name").build());
		mDataGrid.addCol(new DataItem.Builder().name("status").title("Status").build());
		mDataGrid.addCol(new DataItem.Builder().name("description").title("Description").build());
	}

	public void setFeatureComparisonFlag(boolean anIsFeaturesCompared)
	{
		mIsFeatureCompared = anIsFeaturesCompared;
	}

	private void add(String aName, String aStatus, String aDescription)
	{
		mDataGrid.newRow();
		mDataGrid.setValueByName("name", aName);
		mDataGrid.setValueByName("status", aStatus);
		mDataGrid.setValueByName("description", aDescription);
		mDataGrid.addRow();
	}

	private void add(DataItem anItem, String aStatus, String aDescription)
	{
		String itemName = anItem.getName();
		mDataGrid.addProperty(itemName, anItem);

		mDataGrid.newRow();
		mDataGrid.setValueByName("name", itemName);
		mDataGrid.setValueByName("status", aStatus);
		mDataGrid.setValueByName("description", aDescription);
		mDataGrid.addRow();
	}

	/**
	 * Resets the state of the comparison logic in anticipation
	 * of another comparison operation.
	 */
	public void reset()
	{
		mIsCompared = false;
		mDataGrid.emptyRows();
		mDataGrid.clearGridProperties();
	}

	/**
	 * Compares the two data items for differences.  The comparison will
	 * include meta data and values of the items.  The internal data grid
	 * will capture the details regarding the comparison.
	 *
	 * @param aDataItem1 Data item 1 baseline instance.
	 * @param aDataItem2 Data item 2 updated instance.
	 */
	public void compare(DataItem aDataItem1, DataItem aDataItem2)
	{
		if (aDataItem1.getType() != aDataItem2.getType())
			add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item data type differs.");
		if (! StringUtils.equals(aDataItem1.getTitle(), aDataItem2.getTitle()))
			add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item title differs.");
		if (aDataItem1.getDisplaySize() != aDataItem2.getDisplaySize())
			add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item display size differs.");
		if (aDataItem1.getSortOrder() != aDataItem2.getSortOrder())
			add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item sort order differs.");
		if (aDataItem1.isRangeAssigned() == aDataItem2.isRangeAssigned())
		{
			if ((aDataItem1.isRangeAssigned()) && (! aDataItem1.getRange().isEqual(aDataItem2.getRange())))
				add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item ranges differ.");
		}
		if (! aDataItem1.isValueEqual(aDataItem2))
			add(aDataItem2, Data.DIFF_STATUS_UPDATED, "Item values differ.");
		if (mIsFeatureCompared)
		{
			int featureCount1 = aDataItem1.featureCount();
			int featureCount2 = aDataItem2.featureCount();
			if (featureCount1 != featureCount2)
				add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item features differ.");
			else if (featureCount1 > 0)
			{
				String keyName1, keyValue1;

				for (Map.Entry<String, String> featureEntry : aDataItem1.getFeatures().entrySet())
				{
					keyName1 = featureEntry.getKey();
					keyValue1 = featureEntry.getValue();
					if (! aDataItem2.getFeature(keyName1).equals(keyValue1))
					{
						add(aDataItem1.getName(), Data.DIFF_STATUS_UPDATED, "Item features differ.");
						break;
					}
				}
			}
		}
	}

	/**
	 * Compares the two data documents for differences.  The comparison will
	 * include meta data and values of the items.  The internal data grid
	 * will capture the details regarding the comparison.
	 *
	 * @param aDataDoc1 Data document 1 base line instance
	 * @param aDataDoc2 Data document 2 updated instance
	 */
	public void compare(DataDoc aDataDoc1, DataDoc aDataDoc2)
	{
		reset();
		if ((aDataDoc1 != null) && (aDataDoc2 != null))
		{
			if (! StringUtils.equals(aDataDoc1.getName(), aDataDoc2.getName()))
				add(aDataDoc1.getName(), Data.DIFF_STATUS_UPDATED, "Document names differ.");
			if (! StringUtils.equals(aDataDoc1.getTitle(), aDataDoc2.getTitle()))
				add(aDataDoc1.getName(), Data.DIFF_STATUS_UPDATED, "Document titles differ.");
			if (! StringUtils.equals(aDataDoc1.getAction(), aDataDoc2.getAction()))
				add(aDataDoc1.getName(), Data.DIFF_STATUS_UPDATED, "Document actions differ.");
			if (mIsFeatureCompared)
			{
				int featureCount1 = aDataDoc1.featureCount();
				int featureCount2 = aDataDoc2.featureCount();
				if (featureCount1 != featureCount2)
					add(aDataDoc1.getName(), Data.DIFF_STATUS_UPDATED, "Document features differ.");
				else if (featureCount1 > 0)
				{
					String keyName1, keyValue1;

					for (Map.Entry<String, String> featureEntry : aDataDoc1.getFeatures().entrySet())
					{
						keyName1 = featureEntry.getKey();
						keyValue1 = featureEntry.getValue();
						if (! aDataDoc2.getFeature(keyName1).equals(keyValue1))
						{
							add(aDataDoc1.getName(), Data.DIFF_STATUS_UPDATED, "Document features differ.");
							break;
						}
					}
				}
			}

			boolean isFound;
			for (DataItem dataItem1 : aDataDoc1.getItems())
			{
				isFound = false;
				for (DataItem dataItem2 : aDataDoc2.getItems())
				{
					if (dataItem1.getName().equals(dataItem2.getName()))
					{
						compare(dataItem1, dataItem2);
						isFound = true;
						break;
					}
				}
				if (! isFound)
					add(dataItem1, Data.DIFF_STATUS_DELETED, "Item not found in second documemt.");
			}

			for (DataItem dataItem2 : aDataDoc2.getItems())
			{
				isFound = false;
				for (DataItem dataItem1 : aDataDoc1.getItems())
				{
					if (dataItem2.getName().equals(dataItem1.getName()))
					{
						isFound = true;
						break;
					}
				}
				if (! isFound)
					add(dataItem2, Data.DIFF_STATUS_ADDED, "Item added to second document.");
			}
			mIsCompared = true;
		}
	}

	/**
	 * Determines if the previously compared data documents are equal.
	 *
	 * @return <i>true</i> if equal, <i>false</i> otherwise.
	 */
	public boolean isEqual()
	{
		return (mIsCompared) && (mDataGrid.rowCount() == 0);
	}

	/**
	 * Returns a data document containing data items that have been changed
	 * based on its status: Data.DIFF_STATUS_ADDED, Data.DIFF_STATUS_DELETED
	 * or Data.DIFF_STATUS_UPDATED (values only).
	 *
	 * @param aStatus Status to apply to the filter
	 *
	 * @return Optional data document instance.
	 */
	public Optional<DataDoc> changedItems(String aStatus)
	{
		Optional<DataDoc> optDataDoc = Optional.empty();
		int rowCount = mDataGrid.rowCount();
		if ((mIsCompared) && (rowCount > 0))
		{
			String itemName;
			DataDoc dataDoc;
			DataItem dataItem;
			Optional<Object> optObject;

			DataDoc diffDoc = new DataDoc(String.format("%s Items", aStatus));
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = mDataGrid.getRowAsDoc(row);
				if (StringUtils.equals(dataDoc.getValueByName("status"), aStatus))
				{
					itemName = dataDoc.getValueByName("name");
					optObject = mDataGrid.getProperty(itemName);
					if (optObject.isPresent())
					{
						dataItem = (DataItem) optObject.get();
						diffDoc.add(dataItem);
					}
				}
			}
			if (diffDoc.count() > 0)
				optDataDoc = Optional.of(diffDoc);
		}

		return optDataDoc;
	}

	/**
	 * Returns a details data grid containing the results of a previously
	 * compared data document operation.
	 *
	 * @return Data grid with details.
	 */
	public final DataGrid getDetails()
	{
		return mDataGrid;
	}
}
