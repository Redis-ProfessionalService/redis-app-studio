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

import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * A DataGrid manages a collection of columns and rows of data items.
 * The grid is typically instantiated and populated with rows of data,
 * but was not designed for the selective updating, inserting and
 * deletion of rows.  These features may be added in the future.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class DataGrid
{
	private String mName;
	private String mSortColumnName = StringUtils.EMPTY;
	private transient HashMap<String, Object> mProperties;
	private DataDoc mColumns = new DataDoc("Data Grid");
	protected HashMap<String, String> mFeatures = new HashMap<String, String>();
	private Optional<HashMap<String, ArrayList<String>>> mNewRowMap = Optional.empty();
	protected ArrayList<HashMap<String, ArrayList<String>>> mRows = new ArrayList<HashMap<String, ArrayList<String>>>();

	/**
	 * Constructor that accepts a name for the data grid.
	 *
	 * @param aName Name of the data grid
	 */
	public DataGrid(String aName)
	{
		mName = aName;
	}

	/**
	 * Constructor that accepts a data document instance representing
	 * the columns.
	 *
	 * @param aSchemaDoc Data document instance
	 */
	public DataGrid(DataDoc aSchemaDoc)
	{
		mName = aSchemaDoc.getName();
		setColumns(aSchemaDoc);
	}

	/**
	 * Constructor that accepts a name and a data document instance representing
	 * the columns.
	 *
	 * @param aName Name of the grid
	 * @param aSchemaDoc Data document instance
	 */
	public DataGrid(String aName, DataDoc aSchemaDoc)
	{
		setColumns(aSchemaDoc);
		setName(aName);
	}

	/**
	 * Clones and existing data grid instance.
	 *
	 * @param aDataGrid Data grid instance
	 */
	public DataGrid(final DataGrid aDataGrid)
	{
		if (aDataGrid != null)
		{
			setName(aDataGrid.getName());
			mSortColumnName = aDataGrid.mSortColumnName;
			setColumns(new DataDoc(aDataGrid.getColumns()));
			aDataGrid.stream().forEach(dd -> {
				DataDoc ndd = new DataDoc(dd);
				addRow(ndd);
			});
			mNewRowMap = Optional.empty();
			this.mFeatures = new HashMap<String, String>(aDataGrid.getFeatures());
		}
	}

	/**
	 * Creates a DataGrid from array list of data documents.  The logic assumes
	 * that the first document in the list can be used to define the overall
	 * schema for the data grid.
	 *
	 * @param aName Name of data grid
	 * @param aDataDocList Data document list
	 */
	public DataGrid(String aName, ArrayList<DataDoc> aDataDocList)
	{
		if ((aDataDocList != null) && (aDataDocList.size() > 0))
		{
			DataDoc dataDoc = new DataDoc(aDataDocList.get(0));
			dataDoc.resetValues();
			setName(aName);
			setColumns(dataDoc);
			aDataDocList.forEach(dd -> {
				DataDoc ndd = new DataDoc(dd);
				addRow(ndd);
			});
		}
	}

	/*
	public DataGrid(DataDoc aSchemaDoc, ArrayList<DataDoc> aDataDocList)
	{
		// TODO: Build this constructor using Java JsonPath: https://github.com/json-path/JsonPath
		// https://www.baeldung.com/guide-to-jayway-jsonpath
		// Assume a feature definition like jsonPath="$['tool']['jsonpath']['creator']['name']" for each column
		// PS Reporting Utility: data/harvest_time_entries.json
	}
	*/

	/**
	 * Returns a string summary representation of a DataGrid.
	 *
	 * @return String summary representation of this DataGrid.
	 */
	@Override
	public String toString()
	{
		String idName;

		if (StringUtils.isEmpty(mName))
			idName = "Data Table";
		else
			idName = mName;

		return String.format("%s [%d cols x %d rows]", idName, mColumns.count(), mRows.size());
	}

	/**
	 * Returns the name of the DataGrid.
	 *
	 * @return Grid name.
	 */
	public String getName()
	{
		return mName;
	}

	/**
	 * Assigns the name of the DataGrid.
	 *
	 * @param aName Grid name.
	 */
	public void setName(String aName)
	{
		mName = aName;
		if (mColumns != null)
			mColumns.setName(aName);
	}

	/**
	 * Assigns the columns for the DataGrid.
	 *
	 * @param aColumns Data document instance.
	 */
	public void setColumns(DataDoc aColumns)
	{
		mColumns = aColumns;
		mName = mColumns.getName();
	}

	/**
	 * Adds a data item to the document instance.
	 *
	 * @param anItem Data item instance
	 */
	public void addCol(DataItem anItem)
	{
		if (rowCount() == 0)
			mColumns.add(anItem);
	}

	/**
	 * Count of data grid columns.
	 *
	 * @return Count of data grid columns
	 */
	public int colCount()
	{
		return mColumns.count();
	}

	/**
	 * Returns the columns of the data grid as a data document instance
	 *
	 * @return Data document instance
	 */
	public DataDoc getColumns()
	{
		return mColumns;
	}

	private ArrayList<String> cellValues(String aValue)
	{
		ArrayList<String> cellValues = new ArrayList<>();
		if (StringUtils.isNotEmpty(aValue))
			cellValues.add(aValue);

		return cellValues;
	}

	/**
	 * Returns a map representing a new row of cells with default values assigned.
	 *
	 * @return Map representing a row of cell values
	 */
	public Map<String,ArrayList<String>> newRow()
	{
		LinkedHashMap<String, ArrayList<String>> newRowMap = new LinkedHashMap<String, ArrayList<String>>();
		for (DataItem dataItem : mColumns.getItems())
			newRowMap.put(dataItem.getName(), cellValues(dataItem.getDefaultValue()));
		mNewRowMap = Optional.of(newRowMap);

		return newRowMap;
	}

	private LinkedHashMap<String, ArrayList<String>> dataDocToRowMap(DataDoc aDataDoc)
	{
		LinkedHashMap<String, ArrayList<String>> rowMap = new LinkedHashMap<String, ArrayList<String>>();
		for (DataItem dataItem : aDataDoc.getItems())
			rowMap.put(dataItem.getName(), dataItem.getValues());

		return rowMap;
	}

	/**
	 * Adds the data document instance as a new row to the data grid instance.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> if the add was successful or <i>false</i> otherwise
	 */
	public boolean addRow(final DataDoc aDataDoc)
	{
		if ((aDataDoc != null) && (aDataDoc.count() > 0))
		{
			if (mColumns.count() == 0)
			{
				DataDoc ddSchema = new DataDoc(aDataDoc);
				ddSchema.resetValues();
				setColumns(ddSchema);
			}
			LinkedHashMap<String, ArrayList<String>> rowMap = dataDocToRowMap(aDataDoc);
			mRows.add(rowMap);
			return true;
		}
		return false;
	}

	private boolean isCellItemValuePopulated(DataItem aCellItem)
	{
		DataItem dataItem;
		Optional<DataItem> optDataItem;

		for (int row = 0; row < mRows.size(); row++)
		{
			optDataItem = getItemByRowNameOptional(row, aCellItem.getName());
			if (optDataItem.isPresent())
			{
				dataItem = optDataItem.get();
				if (dataItem.isEqual(aCellItem))
					return true;
			}
		}

		return false;
	}

	/**
	 * Adds the rows from the <i>DataGrid</i> parameter to this
	 * data grid instance.  A quick check is performed to ensure
	 * the schemas match and the logic will enforce uniqueness if
	 * an item has been designated a primary key.
	 *
	 * @param aDataGrid Source data grid instance
	 *
	 * @return <i>true</i> if some rows were added successfully or <i>false</i> otherwise
	 */
	public boolean addRows(DataGrid aDataGrid)
	{
		boolean isOK;
		DataDoc dataDoc;
		DataItem diPrimaryKey;

		isOK = false;
		if ((aDataGrid != null) && (aDataGrid.rowCount() > 0))
		{
			DataDoc dgSchema = aDataGrid.getColumns();
			String dgSchemaHash = dgSchema.generateUniqueHash(true);
			if (StringUtils.equals(getColumns().generateUniqueHash(true), dgSchemaHash))
			{
				Optional<DataItem> optDataItem = dgSchema.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
				if (optDataItem.isPresent())
					diPrimaryKey = optDataItem.get();
				else
					diPrimaryKey = null;

				int startingRowCount = rowCount();
				int rowCount = aDataGrid.rowCount();
				for (int row = 0; row < rowCount; row++)
				{
					dataDoc = aDataGrid.getRowAsDoc(row);
					if (diPrimaryKey == null)
						addRow(dataDoc);
					else
					{
						if (! isCellItemValuePopulated(dataDoc.getItemByName(diPrimaryKey.getName())))
							addRow(dataDoc);
					}
				}
				isOK = rowCount() > startingRowCount;
			}
		}

		return isOK;
	}

	/**
	 * Inserts the data document instance into the data grid as the row offset.
	 *
	 * @param aRow Row offset
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> if the insert was successful or <i>false</i> otherwise
	 */
	public boolean insertRow(int aRow, final DataDoc aDataDoc)
	{
		if ((aDataDoc != null) && (aDataDoc.count() > 0) && (aRow >= 0) && (aRow < rowCount()))
		{
			LinkedHashMap<String, ArrayList<String>> rowMap = dataDocToRowMap(aDataDoc);
			mRows.add(aRow, rowMap);
			return true;
		}
		return false;
	}

	/**
	 * Updates the data document instance in the data grid as the row offset specified.
	 *
	 * @param aRow Row offset
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> if the update was successful or <i>false</i> otherwise
	 */
	public boolean updateRow(int aRow, final DataDoc aDataDoc)
	{
		if ((aDataDoc != null) && (aDataDoc.count() > 0) && (aRow >= 0) && (aRow < rowCount()))
		{
			HashMap<String, ArrayList<String>> rowMap = mRows.get(aRow);
			if (rowMap != null)
			{
				for (DataItem dataItem : aDataDoc.getItems())
					rowMap.put(dataItem.getName(), dataItem.getValues());
				return true;
			}
		}
		return false;
	}

	/**
	 * Uses the primary key to locate the row where the update should occur and applies
	 * the changes to the data grid.  One column must have the Data.FEATURE_IS_PRIMARY
	 * enabled.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> if the update was successful or <i>false</i> otherwise
	 */
	public boolean update(DataDoc aDataDoc)
	{
		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataDoc dataDoc;
			DataItem diPrimaryKey = optDataItem.get();
			int rowCount = rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = getRowAsDoc(row);
				if (dataDoc.getValueByName(diPrimaryKey.getName()).equals(diPrimaryKey.getValue()))
					return updateRow(row, aDataDoc);
			}
		}
		return false;
	}

	/**
	 * Deletes the row from the data grid instance.
	 *
	 * @param aRow Row offset
	 *
	 * @return <i>true</i> if the delete was successful or <i>false</i> otherwise
	 */
	public boolean deleteRow(int aRow)
	{
		if ((aRow >= 0) && (aRow < rowCount()))
		{
			mRows.remove(aRow);
			return true;
		}
		return false;
	}

	/**
	 * Uses the primary key to locate the row where the delete should occur and applies
	 * the changes to the data grid.  One column must have the Data.FEATURE_IS_PRIMARY
	 * enabled.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> if the delete was successful or <i>false</i> otherwise
	 */
	public boolean delete(DataDoc aDataDoc)
	{
		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataDoc dataDoc;
			DataItem diPrimaryKey = optDataItem.get();
			int rowCount = rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = getRowAsDoc(row);
				if (dataDoc.getValueByName(diPrimaryKey.getName()).equals(diPrimaryKey.getValue()))
					return deleteRow(row);
			}
		}
		return false;
	}

	/**
	 * Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, String aValue)
	{
		if ((aRow >= 0) && (aRow < rowCount()))
		{
			HashMap<String, ArrayList<String>> rowMap = mRows.get(aRow);
			if (rowMap != null)
				rowMap.put(aName, cellValues(aValue));
		}
	}

	/**
	 * Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, int aValue)
	{
		setValueByRowName(aRow, aName, Integer.toString(aValue));
	}

	/**
	 * Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, long aValue)
	{
		setValueByRowName(aRow, aName, Long.toString(aValue));
	}

	/**
	 *  Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, float aValue)
	{
		setValueByRowName(aRow, aName, Float.toString(aValue));
	}

	/**
	 * Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, double aValue)
	{
		setValueByRowName(aRow, aName, Double.toString(aValue));
	}

	/**
	 * Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, boolean aValue)
	{
		setValueByRowName(aRow, aName, StrUtl.booleanToString(aValue));
	}

	/**
	 * Assigns a cell value by name within at the row number specified.
	 *
	 * @param aRow Row offset
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowName(int aRow, String aName, Date aValue)
	{
		Optional<DataItem> optDataItem = mColumns.getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			if (dataItem.getType() == Data.Type.Date)
				setValueByRowName(aRow, aName, Data.dateValueFormatted(aValue, Data.FORMAT_DATE_DEFAULT));
			else
				setValueByRowName(aRow, aName, Data.dateValueFormatted(aValue, Data.FORMAT_DATETIME_DEFAULT));
		}
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aRow Row offset
	 * @param aCol Column offset
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByRowCol(int aRow, int aCol, String aValue)
	{
		Optional<DataItem> optDataItem = getItemByRowColOptional(aRow, aCol);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			HashMap<String, ArrayList<String>> rowMap = mRows.get(aRow);
			if (rowMap != null)
				rowMap.put(dataItem.getName(), cellValues(aValue));
		}
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, String aValue)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, cellValues(aValue)));
	}

	/**
	 * Assigns a cell values by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValues Values to assign to the cell
	 */
	public void setValuesByName(String aName, ArrayList<String> aValues)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, aValues));
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, Boolean aValue)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, cellValues(aValue.toString())));
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, Integer aValue)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, cellValues(aValue.toString())));
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, Long aValue)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, cellValues(aValue.toString())));
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, Float aValue)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, cellValues(aValue.toString())));
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, Double aValue)
	{
		mNewRowMap.ifPresent(r -> r.put(aName, cellValues(aValue.toString())));
	}

	/**
	 * Assigns a cell value by name to a previously staged row instance.  You should call
	 * the newRow() method prior to using this row.
	 *
	 * @param aName Name of the cell to assign the value to
	 * @param aValue Value to assign to the cell
	 */
	public void setValueByName(String aName, Date aValue)
	{
		Optional<DataItem> optDataItem = mColumns.getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			if (dataItem.getType() == Data.Type.Date)
				mNewRowMap.ifPresent(r -> r.put(aName, cellValues(Data.dateValueFormatted(aValue, Data.FORMAT_DATE_DEFAULT))));
			else
				mNewRowMap.ifPresent(r -> r.put(aName, cellValues(Data.dateValueFormatted(aValue, Data.FORMAT_DATETIME_DEFAULT))));
		}
	}

	/**
	 * Adds a row of cell values to the data grid.  You should have called the newRow()
	 * and setValueByName() methods prior to calling this method.
	 *
	 * @return <i>true</i> if the add was successful or <i>false</i> otherwise
	 */
	public boolean addRow()
	{
		if (mNewRowMap.isPresent())
		{
			mRows.add(mNewRowMap.get());
			mNewRowMap = Optional.empty();
			return true;
		}
		return false;
	}

	/**
	 * Count of rows in the data grid.
	 *
	 * @return Row count
	 */
	public int rowCount()
	{
		return mRows.size();
	}

	/**
	 * Returns an list of maps that represent the cell values in the data grid.
	 *
	 * @return List of all rows from the data grid
	 */
	public ArrayList<HashMap<String, ArrayList<String>>> getRows()
	{
		return mRows;
	}

	/**
	 * Returns a data item representing a cell from the data grid.
	 *
	 * @param anEntry Representing a cell in the data grid row
	 *
	 * @return Data item instance
	 *
	 * @throws NoSuchElementException If the name cannot be matched to a cell
	 */
	public DataItem rowEntryToDataItem(Map.Entry<String, ArrayList<String>> anEntry)
		throws NoSuchElementException
	{
		DataItem curDataItem = mColumns.getItemByName(anEntry.getKey());
		DataItem newDataItem = new DataItem(curDataItem);
		newDataItem.setValues(anEntry.getValue());

		return newDataItem;
	}

	/**
	 * Returns a data item representing a cell from the data grid.
	 *
	 * @param anEntry Representing a cell in the data grid row
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> rowEntryToDataItemOptional(Map.Entry<String, ArrayList<String>> anEntry)
	{
		Optional<DataItem> optDataItem = mColumns.getItemByNameOptional(anEntry.getKey());
		if (optDataItem.isPresent())
		{
			DataItem dataItem = new DataItem(optDataItem.get());
			dataItem.setValues(anEntry.getValue());
			return Optional.of(dataItem);
		}
		else
			return optDataItem;
	}

	/**
	 * Returns a data document representing a row from the data grid.
	 *
	 * @param aRowOffset Row offset in the data grid
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> getRowAsDocOptional(int aRowOffset)
	{
		DataDoc dataDoc = null;

		if (aRowOffset < mRows.size())
		{
			dataDoc = new DataDoc(mColumns);
			dataDoc.setName(String.format("%s [Row %d]", getName(), aRowOffset+1));
			for (Map.Entry<String, ArrayList<String>> entry : mRows.get(aRowOffset).entrySet())
			{
				Optional<DataItem> optDataItem = rowEntryToDataItemOptional(entry);
				if (optDataItem.isPresent())
					dataDoc.add(optDataItem.get());
			}
		}

		return Optional.ofNullable(dataDoc);
	}

	/**
	 * Returns a data document representing a row from the data grid.
	 *
	 * @param aRowOffset Row offset in the data grid
	 *
	 * @return Data document instance
	 */
	public DataDoc getRowAsDoc(int aRowOffset)
	{
		DataDoc dataDoc = null;

		if (aRowOffset < mRows.size())
		{
			dataDoc = new DataDoc(mColumns);
			dataDoc.setName(String.format("%s [Row %d]", getName(), aRowOffset+1));
			for (Map.Entry<String, ArrayList<String>> entry : mRows.get(aRowOffset).entrySet())
			{
				Optional<DataItem> optDataItem = rowEntryToDataItemOptional(entry);
				if (optDataItem.isPresent())
					dataDoc.add(optDataItem.get());
			}
			return dataDoc;
		}
		else
			throw new NoSuchElementException(String.format("Row offset %d is out of range.", aRowOffset));
	}

	/**
	 * Returns a data item representing the cell (aRow, aCol) in the data grid.
	 *
	 * @param aRow Row offset in the data grid
	 * @param aCol Col offset in the data grid
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> getItemByRowColOptional(int aRow, int aCol)
	{
		DataItem dataItem = null;

		if (((aRow >= 0) && (aRow < rowCount())) && ((aCol >= 0) && (aCol < colCount())))
		{
			Optional<DataDoc> optDataDoc = getRowAsDocOptional(aRow);
			if (optDataDoc.isPresent())
			{
				DataDoc dataDoc = optDataDoc.get();
				if (aCol < dataDoc.getItems().size())
				{
					int colOffset = 0;
					for (DataItem dItem : dataDoc.getItems())
					{
						if (colOffset == aCol)
						{
							dataItem = new DataItem(dItem);
							break;
						}
						else
							colOffset++;
					}
				}
			}
		}

		return Optional.ofNullable(dataItem);
	}

	/**
	 * Returns a data item representing the cell (aRow, item name) in the data grid.
	 *
	 * @param aRow Row offset in the data grid
	 * @param aName Name of an item in the row
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> getItemByRowNameOptional(int aRow, String aName)
	{
		Optional<DataDoc> optDataDoc = getRowAsDocOptional(aRow);
		if (optDataDoc.isPresent())
		{
			DataDoc dataDoc = optDataDoc.get();
			return dataDoc.getItemByNameOptional(aName);
		}
		else
			return Optional.empty();
	}

	/**
	 * Returns a list of data document instances representing the rows in the data grid.
	 *
	 * @return List of data document instances
	 */
	public List<DataDoc> getRowsAsDocList()
	{
		Optional<DataDoc> optDataDoc;

		List<DataDoc> dataDocList = new ArrayList<>();
		int rowCount = rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			optDataDoc = getRowAsDocOptional(row);
			optDataDoc.ifPresent(dataDocList::add);
		}

		return dataDocList;
	}

	/**
	 * Will compare each row of the current grid against the rows within the
	 * grid provided as a parameter.
	 * <p>
	 * <b>Note:</b> If an item is found to differ, then a property called
	 * <i>Data.VALIDATION_FIELD_CHANGED</i> will be assigned a relevant message.
	 * </p>
	 *
	 * @param aDataGrid Grid of rows to compare.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isGridRowValuesEqual(DataGrid aDataGrid)
	{
		DataDoc curDataDoc, newDataDoc;

		int curRowCount = rowCount();
		int newRowCount = aDataGrid.rowCount();
		if (curRowCount != newRowCount)
			return false;

		clearGridProperties();
		boolean isEqual = true;
		for (int row = 0; row < curRowCount; row++)
		{
			curDataDoc = getRowAsDoc(row);
			newDataDoc = aDataGrid.getRowAsDoc(row);
			if (! curDataDoc.isItemValuesEqual(newDataDoc))
			{
				isEqual = false;
				addProperty(Data.VALIDATION_ITEM_CHANGED, String.format("Row %d: %s", row,
																		Data.VALIDATION_MESSAGE_ITEM_CHANGED));
			}
		}

		return isEqual;
	}

	/**
	 * Returns a stream of data documents representing the data grid.
	 *
	 * @return Stream of data documents
	 */
	public Stream<DataDoc> stream()
	{
		return getRowsAsDocList().stream();
	}

	private String ddStringByName(DataDoc aDataDoc)
	{
		return aDataDoc.getItemByName(mSortColumnName).getValue();
	}

	private Integer ddIntegerByName(DataDoc aDataDoc)
	{
		return aDataDoc.getItemByName(mSortColumnName).getValueAsInteger();
	}

	private Long ddLongByName(DataDoc aDataDoc)
	{
		return aDataDoc.getItemByName(mSortColumnName).getValueAsLong();
	}

	private Float ddFloatByName(DataDoc aDataDoc)
	{
		return aDataDoc.getItemByName(mSortColumnName).getValueAsFloat();
	}

	private Double ddDoubleByName(DataDoc aDataDoc)
	{
		return aDataDoc.getItemByName(mSortColumnName).getValueAsDouble();
	}

	private Long ddDateByName(DataDoc aDataDoc)
	{
		return aDataDoc.getValueAsDate(mSortColumnName).getTime();
	}

	private List<DataDoc> sortStringByNameOrder(Stream<DataDoc> aStream, Data.Order anOrder)
	{
		if (anOrder == Data.Order.DESCENDING)
			return aStream.sorted(comparing(this::ddStringByName).reversed()).collect(toList());
		else
			return aStream.sorted(comparing(this::ddStringByName)).collect(toList());
	}

	private List<DataDoc> sortIntegerByNameOrder(Stream<DataDoc> aStream, Data.Order anOrder)
	{
		if (anOrder == Data.Order.DESCENDING)
			return aStream.sorted(comparing(this::ddIntegerByName).reversed()).collect(toList());
		else
			return aStream.sorted(comparing(this::ddIntegerByName)).collect(toList());
	}

	private List<DataDoc> sortLongByNameOrder(Stream<DataDoc> aStream, Data.Order anOrder)
	{
		if (anOrder == Data.Order.DESCENDING)
			return aStream.sorted(comparing(this::ddLongByName).reversed()).collect(toList());
		else
			return aStream.sorted(comparing(this::ddLongByName)).collect(toList());
	}

	private List<DataDoc> sortFloatByNameOrder(Stream<DataDoc> aStream, Data.Order anOrder)
	{
		if (anOrder == Data.Order.DESCENDING)
			return aStream.sorted(comparing(this::ddFloatByName).reversed()).collect(toList());
		else
			return aStream.sorted(comparing(this::ddFloatByName)).collect(toList());
	}

	private List<DataDoc> sortDoubleByNameOrder(Stream<DataDoc> aStream, Data.Order anOrder)
	{
		if (anOrder == Data.Order.DESCENDING)
			return aStream.sorted(comparing(this::ddDoubleByName).reversed()).collect(toList());
		else
			return aStream.sorted(comparing(this::ddDoubleByName)).collect(toList());
	}

	private List<DataDoc> sortDateByNameOrder(Stream<DataDoc> aStream, Data.Order anOrder)
	{
		if (anOrder == Data.Order.DESCENDING)
			return aStream.sorted(comparing(this::ddDateByName).reversed()).collect(toList());
		else
			return aStream.sorted(comparing(this::ddDateByName)).collect(toList());
	}

	/**
	 * Creates a new DataGrid of rows and columns sorted by column name and order
	 * specified.
	 *
	 * @param aColumnName Column name
	 * @param anOrder Sort order
	 *
	 * @return DataGrid instance (sorted as specified)
	 */
	public DataGrid sortByColumnName(String aColumnName, Data.Order anOrder)
	{
		DataDoc dataDoc = getColumns();
		DataGrid dataGrid = new DataGrid(mName);
		dataGrid.setColumns(new DataDoc(dataDoc));
		dataGrid.mFeatures = new HashMap<String, String>(mFeatures);

		Optional<DataItem> optDataItem = dataDoc.getItemByNameOptional(aColumnName);
		if (optDataItem.isPresent())
		{
			List<DataDoc> sortedDocList;
			mSortColumnName = aColumnName;
			DataItem dataItem = optDataItem.get();
			switch (dataItem.getType())
			{
				case Integer:
					sortedDocList = sortIntegerByNameOrder(stream(), anOrder);
					break;
				case Long:
					sortedDocList = sortLongByNameOrder(stream(), anOrder);
					break;
				case Float:
					sortedDocList = sortFloatByNameOrder(stream(), anOrder);
					break;
				case Double:
					sortedDocList = sortDoubleByNameOrder(stream(), anOrder);
					break;
				case Date:
				case DateTime:
					sortedDocList = sortDateByNameOrder(stream(), anOrder);
					break;
				default:
					sortedDocList = sortStringByNameOrder(stream(), anOrder);
					break;
			}
			sortedDocList.forEach(dd -> {
				DataDoc ndd = new DataDoc(dd);
				dataGrid.addRow(ndd);
			});
		}

		return dataGrid;
	}

	/**
	 * Empties the table of any data rows.  The columns and other
	 * properties remain unchanged.
	 */
	public void emptyRows()
	{
		mRows = new ArrayList<HashMap<String, ArrayList<String>>>();
	}

	/**
	 * Empties the table of any data rows and columns while leaving the features unchanged.
	 */
	public void emptyAll()
	{
		emptyRows();
		mColumns = new DataDoc("Data Grid");
		if (mProperties != null)
			mProperties = null;
	}

	/**
	 * Add a unique feature to this bag.  A feature enhances the core
	 * capability of the data grid.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, String aValue)
	{
		mFeatures.put(aName, aValue);
	}

	/**
	 * Add a unique feature to this grid.  A feature enhances the core
	 * capability of the data grid.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, int aValue)
	{
		addFeature(aName, Integer.toString(aValue));
	}

	/**
	 * Add a unique feature to this grid.  A feature enhances the core
	 * capability of the data grid.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, long aValue)
	{
		addFeature(aName, Long.toString(aValue));
	}

	/**
	 * Enabling the feature will add the name and assign it a
	 * value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of the feature.
	 */
	public void enableFeature(String aName)
	{
		mFeatures.put(aName, StrUtl.STRING_TRUE);
	}

	/**
	 * Disabling a feature will remove its name and value
	 * from the internal list.
	 *
	 * @param aName Name of feature.
	 */
	public void disableFeature(String aName)
	{
		mFeatures.remove(aName);
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and assigned a value.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureAssigned(String aName)
	{
		return (getFeature(aName) != null);
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and assigned a value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureTrue(String aName)
	{
		return StrUtl.stringToBoolean(mFeatures.get(aName));
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and not assigned a value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureFalse(String aName)
	{
		return !StrUtl.stringToBoolean(mFeatures.get(aName));
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and its value matches the one provided as a
	 * parameter.
	 *
	 * @param aName Feature name.
	 * @param aValue Feature value to match.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureEqual(String aName, String aValue)
	{
		String featureValue = getFeature(aName);
		return StringUtils.equalsIgnoreCase(featureValue, aValue);
	}

	/**
	 * Count of unique features assigned to this bag.
	 *
	 * @return Feature count.
	 */
	public int featureCount()
	{
		return mFeatures.size();
	}

	/**
	 * Returns the String associated with the feature name or
	 * <i>null</i> if the name could not be found.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public String getFeature(String aName)
	{
		return mFeatures.get(aName);
	}

	/**
	 * Returns the int associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public int getFeatureAsInt(String aName)
	{
		return Data.createInt(getFeature(aName));
	}

	/**
	 * Returns the long associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public long getFeatureAsLong(String aName)
	{
		return Data.createLong(getFeature(aName));
	}

	/**
	 * Returns the float associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public float getFeatureAsFloat(String aName)
	{
		return Data.createFloat(getFeature(aName));
	}

	/**
	 * Returns the double associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public double getFeatureAsDouble(String aName)
	{
		return Data.createDouble(getFeature(aName));
	}

	/**
	 * Removes all features assigned to this object instance.
	 */
	public void clearFeatures()
	{
		mFeatures.clear();
	}

	/**
	 * Assigns the hash map of features to the list.
	 *
	 * @param aFeatures Feature list.
	 */
	public void setFeatures(HashMap<String, String> aFeatures)
	{
		if (aFeatures != null)
			mFeatures = new HashMap<String, String>(aFeatures);
	}

	/**
	 * Returns a read-only copy of the internal map containing
	 * feature list.
	 *
	 * @return Internal feature map instance.
	 */
	public final HashMap<String, String> getFeatures()
	{
		return mFeatures;
	}

	/**
	 * Add an application defined property to the data grid.
	 * <p>
	 * <b>Notes:</b>
	 * </p>
	 * <ul>
	 *     <li>The goal of the DataGrid is to strike a balance between
	 *     providing enough properties to adequately model application
	 *     related data without overloading it.</li>
	 *     <li>This method offers a mechanism to capture additional
	 *     (application specific) properties that may be needed.</li>
	 *     <li>Properties added with this method are transient and
	 *     will not be stored when saved or cloned.</li>
	 * </ul>
	 *
	 * @param aName Property name (duplicates are not supported).
	 * @param anObject Instance of an object.
	 */
	public void addProperty(String aName, Object anObject)
	{
		if (mProperties == null)
			mProperties = new HashMap<String, Object>();
		mProperties.put(aName, anObject);
	}

	/**
	 * Updates the property by name with the object instance.
	 *
	 * @param aName Name of the property
	 * @param anObject Instance of an object
	 */
	public void updateProperty(String aName, Object anObject)
	{
		if (mProperties == null)
			mProperties = new HashMap<String, Object>();
		mProperties.put(aName, anObject);
	}

	/**
	 * Removes a property from the data grid.
	 *
	 * @param aName Name of the property
	 */
	public void deleteProperty(String aName)
	{
		if (mProperties != null)
			mProperties.remove(aName);
	}

	/**
	 * Returns an Optional for an object associated with the property name.
	 *
	 * @param aName Name of the property.
	 * @return Optional instance of an object.
	 */
	public Optional<Object> getProperty(String aName)
	{
		if (mProperties == null)
			return Optional.empty();
		else
			return Optional.ofNullable(mProperties.get(aName));
	}

	/**
	 * Removes all application defined properties assigned to this data grid.
	 */
	public void clearGridProperties()
	{
		if (mProperties != null)
			mProperties.clear();
	}

	/**
	 * Returns the property map instance managed by the data document or <i>null</i>
	 * if empty.
	 *
	 * @return Hash map instance.
	 */
	public HashMap<String, Object> getProperties()
	{
		return mProperties;
	}

	/**
	 * Returns a descriptive statistics instance for the column of values identified
	 * by the name.
	 *
	 * @param aName Name of the column
	 *
	 * @return Descriptive statistics instance
	 *
	 * @throws NoSuchElementException If the name cannot be matched to a column
	 */
	public DescriptiveStatistics getDescriptiveStatistics(String aName)
		throws NoSuchElementException
	{
		DataDoc dataDoc;
		DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

		int rowCount = rowCount();
		DataItem dataItem = mColumns.getItemByName(aName);
		if (Data.isNumber(dataItem.getType()))
		{
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = getRowAsDoc(row);
				descriptiveStatistics.addValue(dataDoc.getItemByName(aName).getValueAsDouble());
			}
		}

		return descriptiveStatistics;
	}
}
