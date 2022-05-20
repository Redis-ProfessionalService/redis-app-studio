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

import com.redis.foundation.std.DatUtl;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The Data Item Analyzer class will examine small-to-medium sized
 * data sets to determine their type and value composition.
 *
 * A good enhancement to this package would be the inclusion of the
 * Apache Commons Math features - see link below.
 *
 * @see <a href="http://commons.apache.org/proper/commons-math/userguide/stat.html">Apache Commons Math</a>
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("WrapperTypeMayBePrimitive")
public class DataItemAnalyzer
{
	private String mName;
	private int mNullCount;
	private boolean mIsDate;
	private boolean mIsFloat;
	private int mTotalValues;
	private boolean mIsInteger;
	private boolean mIsBoolean;
	private final Data.Type mType;
	private Map<String,Integer> mValueCount;
	private DescriptiveStatistics mStatistics;

	/**
	 * Constructor with a unique field name.
	 *
	 * @param aName Field name.
	 */
	public DataItemAnalyzer(String aName)
	{
		reset(aName);
		mType = Data.Type.Undefined;
	}

	/**
	 * Constructor with a unique field name.
	 *
	 * @param aName Field name.
	 * @param aType Data type.
	 */
	public DataItemAnalyzer(String aName, Data.Type aType)
	{
		reset(aName);
		mType = aType;
	}

	/**
	 * Use this if you wish to reuse the object instance.
	 *
	 * @param aName Field name.
	 */
	public void reset(String aName)
	{
		mName = aName;
		mNullCount = 0;
		mIsDate = true;
		mIsFloat = true;
		mTotalValues = 0;
		mIsInteger = true;
		mIsBoolean = true;
		mValueCount = new HashMap<>();
		mStatistics = new DescriptiveStatistics();
	}

	/**
	 * Convenience method for the DataAnalyzer class to generate a
	 * grid definition instance.
	 *
	 * @param aSampleCount Sample of top counts of data values.
	 *
	 * @return DataDoc instance.
	 */
	public DataDoc createDefinition(int aSampleCount)
	{
		String itemName, itemTitle;

		DataDoc dataDoc = new DataDoc(mName);
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("name").title("Name").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("type").title("Type").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Integer).name("total_count").title("Total Count").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Integer).name("unique_count").title("Unique Count").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Integer).name("null_count").title("Null Count").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("minimum").title("Minimum").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("maximum").title("Maximum").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("mean").title("Mean").build());
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("median").title("Median").build());	// GridDS
		dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name("standard_deviation").title("Deviation").build());

		for (int col = 0; col < aSampleCount; col++)
		{
			itemName = String.format("value_%02d", col+1);
			itemTitle = String.format("Value %02d", col+1);
			dataDoc.add(new DataItem.Builder().type(Data.Type.Text).name(itemName).title(itemTitle).build());
			itemName = String.format("count_%02d", col+1);
			itemTitle = String.format("Count %02d", col+1);
			dataDoc.add(new DataItem.Builder().type(Data.Type.Integer).name(itemName).title(itemTitle).value(0).build());
			itemName = String.format("percent_%02d", col+1);
			itemTitle = String.format("Percent %02d", col+1);
			dataDoc.add(new DataItem.Builder().type(Data.Type.Double).name(itemName).title(itemTitle).value(0.0).build());
		}

		return dataDoc;
	}

	private boolean isNumberType()
	{
		return mIsFloat || mIsInteger;
	}

	private void scanType(String aValue)
	{
		if (isNumberType())
		{
			if (NumberUtils.isParsable(aValue))
			{
				int offset = aValue.indexOf(StrUtl.CHAR_DOT);
				if ((mIsInteger) && (offset != -1))
					mIsInteger = false;
			}
			else
			{
				if (mIsInteger)
					mIsInteger = false;
				if (mIsFloat)
					mIsFloat = false;
			}
		}
		if (mIsDate)
		{
			Date fieldDate = DatUtl.detectCreateDate(aValue);
			if (fieldDate == null)
				mIsDate = false;
		}
		if (mIsBoolean)
		{
			if ((! aValue.equalsIgnoreCase(StrUtl.STRING_TRUE)) &&
				(! aValue.equalsIgnoreCase(StrUtl.STRING_YES)) &&
				(! aValue.equalsIgnoreCase(StrUtl.STRING_FALSE)) &&
				(! aValue.equalsIgnoreCase(StrUtl.STRING_NO)))
				mIsBoolean = false;
		}
	}

	/**
	 * Scans the data value to determine its type and metric information.
	 *
	 * @param aValue Data value.
	 */
	public void scan(String aValue)
	{
		mTotalValues++;
		if (StringUtils.isNotEmpty(aValue))
		{
			scanType(aValue);
			if (isNumberType())
				mStatistics.addValue(Data.createDouble(aValue));
			mValueCount.merge(aValue, 1, Integer::sum);
		}
		else
			mNullCount++;
	}

	/**
	 * Returns the derived type information once the scanning process
	 * is complete.
	 *
	 * @return Field type.
	 */
	public Data.Type getType()
	{
		if (mType != Data.Type.Undefined)
			return mType;
		else if (mIsBoolean)
			return Data.Type.Boolean;
		else if (mIsInteger)
			return Data.Type.Integer;
		else if (mIsFloat)
			return Data.Type.Float;
		else if (mIsDate)
			return Data.Type.DateTime;
		else
			return Data.Type.Text;
	}

	/**
	 * Returns a DataDoc of items describing the scanned value data.
	 * The DataDoc will contain the item name, derived type, populated
	 * count, null count and a sample count of values (with overall
	 * percentages) that repeated most often.
	 *
	 * @param aSampleCount Identifies the top count of values.
	 *
	 * @return DataDoc instance of analysis details.
	 */
	public DataDoc getDetails(int aSampleCount)
	{
		Date dateValue;
		DataItem dataItem;
		Integer valueCount;
		Optional<DataItem> optDataItem;
		String itemName, itemTitle, dataValue;
		Double valuePercentage, minValue, maxValue;

		Data.Type dataType = getType();
		int uniqueValues = mValueCount.size();
		DataDoc dataDoc = new DataDoc(mName);
		dataDoc.add(new DataItem.Builder().name("name").title("Name").value(mName).build());
		dataDoc.add(new DataItem.Builder().name("type").title("Type").value(Data.typeToString(dataType)).build());
		dataDoc.add(new DataItem.Builder().name("total_count").title("Total Count").value(mTotalValues).build());
		dataDoc.add(new DataItem.Builder().name("unique_count").title("Unique Count").value(uniqueValues).build());
		dataDoc.add(new DataItem.Builder().name("null_count").title("Null Count").value(mNullCount).build());

// Create a table from the values map and use sorting to get our top sample size.

		DataGrid valuesGrid = new DataGrid(mName);
		valuesGrid.addCol(new DataItem.Builder().type(Data.Type.Text).name("value").title("Value").build());
		valuesGrid.addCol(new DataItem.Builder().type(Data.Type.Integer).name("count").title("Count").build());
		valuesGrid.addCol(new DataItem.Builder().type(Data.Type.Double).name("percentage").title("Percentage").build());

		minValue = Double.MAX_VALUE;
		maxValue = Double.MIN_VALUE;
		for (Map.Entry<String, Integer> entry : mValueCount.entrySet())
		{
			valuesGrid.newRow();
			dataValue = entry.getKey();
			valueCount = entry.getValue();
			if (mTotalValues == 0)
				valuePercentage = 0.0;
			else
				valuePercentage = valueCount.doubleValue() / mTotalValues * 100.0;

			valuesGrid.newRow();
			valuesGrid.setValueByName("value", dataValue);
			valuesGrid.setValueByName("count", valueCount);
			valuesGrid.setValueByName("percentage", String.format("%.2f", valuePercentage));
			if (Data.isText(dataType))
			{
				minValue = Math.min(minValue, dataValue.length());
				maxValue = Math.max(maxValue, dataValue.length());
			}
			else if (Data.isNumber(dataType))
			{
				minValue = Math.min(minValue, Double.parseDouble(dataValue));
				maxValue = Math.max(maxValue, Double.parseDouble(dataValue));
			}
			else if (Data.isDateOrTime(dataType))
			{

// While we are decomposing the date to milliseconds of time, you can do a Date(milliseconds)
// reconstruction.

				dateValue = DatUtl.detectCreateDate(dataValue);
				if (dataValue != null)
				{
					minValue = Math.min(minValue, dateValue.getTime());
					maxValue = Math.max(maxValue, dateValue.getTime());
				}
			}
			valuesGrid.addRow();
		}
		DataGrid sortedValuesGrid = valuesGrid.sortByColumnName("count", Data.Order.DESCENDING);

		if (Data.isBoolean(dataType))
		{
			dataDoc.add(new DataItem.Builder().name("minimum").title("Minimum").value(StrUtl.STRING_FALSE).build());
			dataDoc.add(new DataItem.Builder().name("maximum").title("Maximum").value(StrUtl.STRING_TRUE).build());
		}
		else if (dataType == Data.Type.Date)
		{
			dataDoc.add(new DataItem.Builder().name("minimum").title("Minimum").value(Data.dateValueFormatted(new Date(minValue.longValue()), Data.FORMAT_DATE_DEFAULT)).build());
			dataDoc.add(new DataItem.Builder().name("maximum").title("Maximum").value(Data.dateValueFormatted(new Date(maxValue.longValue()), Data.FORMAT_DATE_DEFAULT)).build());
		}
		else if (dataType == Data.Type.DateTime)
		{
			dataDoc.add(new DataItem.Builder().name("minimum").title("Minimum").value(Data.dateValueFormatted(new Date(minValue.longValue()), Data.FORMAT_DATETIME_DEFAULT)).build());
			dataDoc.add(new DataItem.Builder().name("maximum").title("Maximum").value(Data.dateValueFormatted(new Date(maxValue.longValue()), Data.FORMAT_DATETIME_DEFAULT)).build());
		}
		else
		{
			dataDoc.add(new DataItem.Builder().name("minimum").title("Minimum").value(String.format("%.2f", minValue)).build());
			dataDoc.add(new DataItem.Builder().name("maximum").title("Maximum").value(String.format("%.2f", maxValue)).build());
			if (Data.isNumber(dataType))
			{
				if (Double.isNaN(mStatistics.getMean()))
					dataDoc.add(new DataItem.Builder().name("mean").title("Mean").build());
				else
					dataDoc.add(new DataItem.Builder().name("mean").title("Mean").value(String.format("%.2f", mStatistics.getMean())).build());
				if (Double.isNaN(mStatistics.getStandardDeviation()))
					dataDoc.add(new DataItem.Builder().name("standard_deviation").title("Deviation").build());
				else
					dataDoc.add(new DataItem.Builder().name("standard_deviation").title("Deviation").value(String.format("%.2f", mStatistics.getStandardDeviation())).build());
			}
		}

// Create columns for the top sample sizes (value, matching count, matching percentage)

		int adjCount = Math.min(aSampleCount, sortedValuesGrid.rowCount());
		adjCount = Math.min(adjCount, uniqueValues);
		for (int row = 0; row < adjCount; row++)
		{
			itemName = String.format("value_%02d", row+1);
			itemTitle = String.format("Value %02d", row+1);
			optDataItem = sortedValuesGrid.getItemByRowNameOptional(row, "value");
			if (optDataItem.isPresent())
			{
				dataItem = optDataItem.get();
				dataValue = StringUtils.trim(dataItem.getValue());
			}
			else
				dataValue = StringUtils.EMPTY;
			dataDoc.add(new DataItem.Builder().name(itemName).title(itemTitle).value(dataValue).build());
			itemName = String.format("count_%02d", row+1);
			itemTitle = String.format("Count %02d", row+1);
			optDataItem = sortedValuesGrid.getItemByRowNameOptional(row, "count");
			if (optDataItem.isPresent())
			{
				dataItem = optDataItem.get();
				dataValue = dataItem.getValue();
			}
			else
				dataValue = StringUtils.EMPTY;
			dataDoc.add(new DataItem.Builder().type(Data.Type.Integer).name(itemName).title(itemTitle).value(dataValue).build());
			itemName = String.format("percent_%02d", row+1);
			itemTitle = String.format("Percent %02d", row+1);
			optDataItem = sortedValuesGrid.getItemByRowNameOptional(row, "percentage");
			if (optDataItem.isPresent())
			{
				dataItem = optDataItem.get();
				dataValue = dataItem.getValue();
			}
			else
				dataValue = StringUtils.EMPTY;
			dataDoc.add(new DataItem.Builder().type(Data.Type.Double).name(itemName).title(itemTitle).value(dataValue).build());
		}

		return dataDoc;
	}
}
