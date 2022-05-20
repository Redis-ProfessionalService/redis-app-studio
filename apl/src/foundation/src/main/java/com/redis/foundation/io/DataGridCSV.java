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

import com.redis.foundation.data.*;
import com.redis.foundation.std.StrUtl;
import com.redis.foundation.data.*;
import org.apache.commons.lang3.StringUtils;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The DataGridCSV provides a collection of methods that can generate/load
 * a CSV representation of a <i>DataGrid</i> object.
 *
 * @see <a href="https://github.com/super-csv/super-csv">SuperCSV GitHub</a>
 * @see <a href="http://super-csv.github.io/super-csv/">SuperCSV Documentation</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataGridCSV
{
	public enum FileFormat
	{
		Standard, MSExcel, Tabular
	}

	private boolean mIsFormatted;
	private String mDateTimeFormat;
	private final DataGrid mDataGrid;
	private boolean mIsFieldNamePreferred;
	private boolean mIsRowNumberPrimaryKey;
	private char mDelimiterChar = StrUtl.CHAR_NULL;
	private FileFormat mInputFormat = FileFormat.MSExcel;

	/**
	 * Default constructor
	 */
	public DataGridCSV()
	{
		mDataGrid = new DataGrid("Data Grid from CSV");
	}

	/**
	 * Constructor that accepts a data grid instance.
	 *
	 * @param aDataGrid Data grid instance
	 */
	public DataGridCSV(DataGrid aDataGrid)
	{
		mDataGrid = aDataGrid;
	}

	/**
	 * Constructor accepts a Data Document as the grid schema definition.
	 *
	 * @param aDataDoc Data document instance
	 */
	public DataGridCSV(DataDoc aDataDoc)
	{
		mDataGrid = new DataGrid(aDataDoc);
	}

	/**
	 * Assign the value formatting flag.  If <i>true</i>, then numbers
	 * and dates will be generated based on the format mask.
	 *
	 * @param aIsFormatted True or false
	 */
	public void setFormattedFlag(boolean aIsFormatted)
	{
		mIsFormatted = aIsFormatted;
	}

	/**
	 * Return an instance to the internally managed data grid.
	 *
	 * @return Data grid instance.
	 */
	public DataGrid getDataGrid()
	{
		return mDataGrid;
	}

	/**
	 * Assigns a file format for the parsing logic.  The options are:
	 * Standard - double quotes, comma, return and newline
	 * MSExcel - double quotes, comma, newline
	 * Tabular - double quotes, tab, newline
	 *
	 * @param anInputFormat Input format
	 */
	public void setFileFormat(FileFormat anInputFormat)
	{
		mInputFormat = anInputFormat;
	}

	public void setDateTimeFormat(String aDateTimeFormat)
	{
		mDateTimeFormat = aDateTimeFormat;
	}

	private CsvPreference fileFormatToCsvPreference()
	{
		switch (mInputFormat)
		{
			case MSExcel:
				return CsvPreference.EXCEL_PREFERENCE;
			case Tabular:
				return CsvPreference.TAB_PREFERENCE;
			default:
				return CsvPreference.STANDARD_PREFERENCE;
		}
	}

	/**
	 * If assigned to <i>true</i>, then the field names will be used
	 * for the header row.
	 *
	 * @param aIsFieldNamePreferred Field name preference flag.
	 */
	public void setFieldNamePreferred(boolean aIsFieldNamePreferred)
	{
		mIsFieldNamePreferred = aIsFieldNamePreferred;
	}

	/**
	 * If assigned to <i>true</i>, then a primary key column will
	 * be added to the schema and assigned the unique row number
	 * as a value.
	 *
	 * @param aIsRowNumberPrimaryKey If <i>true</i> then a primary
	 * key column will be added to the schema
	 */
	public void setRowNumberPrimaryKey(boolean aIsRowNumberPrimaryKey)
	{
		mIsRowNumberPrimaryKey = aIsRowNumberPrimaryKey;
	}

	/**
	 * Assigns a delimiter character for data items that are multi-value.
	 *
	 * @param aDelimiterChar Delimiter character.
	 */
	public void setMultiValueDelimiterChar(char aDelimiterChar)
	{
		mDelimiterChar = aDelimiterChar;
	}

	private DataItem dataTypeLabelToDataItem(String aDataTypeLabel, int aColumnOffset)
	{
		DataItem dataItem;
		Data.Type dataType = Data.Type.Text;

		if (StringUtils.isNotEmpty(aDataTypeLabel))
		{
			String columnName = aDataTypeLabel;
			String columnTitle = StringUtils.EMPTY;

			int typeOffsetStart = aDataTypeLabel.indexOf(StrUtl.CHAR_BRACKET_OPEN);
			int typeOffsetFinish = aDataTypeLabel.indexOf(StrUtl.CHAR_BRACKET_CLOSE);
			int labelOffsetStart = aDataTypeLabel.indexOf(StrUtl.CHAR_PAREN_OPEN);
			int labelOffsetFinish = aDataTypeLabel.indexOf(StrUtl.CHAR_PAREN_CLOSE);

			if ((typeOffsetStart > 0) && (typeOffsetFinish > 0))
			{
				columnName = aDataTypeLabel.substring(0, typeOffsetStart);
				String typeName = aDataTypeLabel.substring(typeOffsetStart+1, typeOffsetFinish);
				dataType = Data.stringToType(typeName);
			}
			if ((labelOffsetStart > 0) && (labelOffsetFinish > 0))
			{
				if (typeOffsetStart == -1)
					columnName = aDataTypeLabel.substring(0, labelOffsetStart);
				columnTitle = aDataTypeLabel.substring(labelOffsetStart+1, labelOffsetFinish);
			}
			dataItem = new DataItem.Builder().type(dataType).name(columnName).title(columnTitle).build();
		}
		else
		{
			String columnName = String.format("column_name_%02d", aColumnOffset);
			dataItem = new DataItem(dataType, columnName);
		}

		return dataItem;
	}

	/**
	 * Parses and tokenizes the CSV header string into data items and
	 * create a data document instance representing the data grid columns.
	 *
	 * @param aCSVHeader CSV header string.
	 *
	 * @return Data document instance representing the data grid columns.
	 */
	public DataDoc headerColumnsToDataDoc(String aCSVHeader)
	{
		DataDoc dataDoc = new DataDoc("CSV Document");

		if (StringUtils.isNotEmpty(aCSVHeader))
		{
			int colCount = 0;
			DataItem dataItem;

			if (mIsRowNumberPrimaryKey)
			{
				dataItem = new DataItem.Builder().type(Data.Type.Integer).name("row_id").title("Row Id").build();
				dataItem.enableFeature(Data.FEATURE_IS_HIDDEN);
				dataItem.enableFeature(Data.FEATURE_IS_PRIMARY);
				dataDoc.add(dataItem);
			}

			int commaCount = StringUtils.countMatches(aCSVHeader, StrUtl.CHAR_COMMA);
			if (commaCount == 0)
			{
				colCount++;
				dataItem = dataTypeLabelToDataItem(aCSVHeader, colCount);
				dataDoc.add(dataItem);
			}
			else
			{
				String[] columnItems = aCSVHeader.split(",");
				for (String columnName : columnItems)
				{
					colCount++;
					dataItem = dataTypeLabelToDataItem(columnName, colCount);
					dataDoc.add(dataItem);
				}
			}
		}

		return dataDoc;
	}

	/**
	 * Loads an optional data grid instance from an input reader stream.
	 *
	 * @param aReader Input reader stream
	 * @param aWithHeaders If <i>true</i>, then the first row will be read to identify the column headers
	 *
	 * @return Optional data grid instance
	 *
	 * @throws IOException I/O exception
	 */
	public Optional<DataGrid> load(Reader aReader, boolean aWithHeaders)
		throws IOException
	{
		try (CsvListReader csvListReader = new CsvListReader(aReader, fileFormatToCsvPreference()))
		{
			int colOffset;
			String cellValue;
			DataItem dataItem;
			List<String> rowCells;
			int colCount, adjColCount;
			ArrayList<String> valueList;
			String[] columnHeaders = null;

			if (aWithHeaders)
				columnHeaders = csvListReader.getHeader(aWithHeaders);
			colCount = mDataGrid.colCount();
			if ((columnHeaders != null) && (colCount == 0))
			{
				for (String columnName : columnHeaders)
				{
					colCount++;
					dataItem = dataTypeLabelToDataItem(columnName, colCount);
					mDataGrid.addCol(dataItem);
				}
			}

			do
			{
				rowCells = csvListReader.read();
				if (rowCells != null)
				{
					colOffset = 0;
					adjColCount = Math.min(rowCells.size(), colCount);
					mDataGrid.newRow();
					for (DataItem di : mDataGrid.getColumns().getItems())
					{
						if (colOffset < adjColCount)
						{
							cellValue = rowCells.get(colOffset++);
							if ((mDelimiterChar != StrUtl.CHAR_NULL) &&
								(StrUtl.isMultiValue(cellValue, mDelimiterChar)))
							{
								valueList = StrUtl.expandToList(cellValue, mDelimiterChar);
								mDataGrid.setValuesByName(di.getName(), valueList);
							}
							else
							{
								if ((Data.isDateOrTime(di.getType())) && (StringUtils.isNotEmpty(mDateTimeFormat)))
									mDataGrid.setValueByName(di.getName(), Data.createDate(cellValue, mDateTimeFormat));
								else
									mDataGrid.setValueByName(di.getName(), cellValue);
							}
						}
						else
							break;
					}
					mDataGrid.addRow();
				}
			}
			while (rowCells != null);
		}
		catch (Exception e)
		{
			throw new IOException(e.getMessage());
		}

		return Optional.ofNullable(mDataGrid);
	}

	/**
	 * Loads an optional data grid instance from the specified file name using
	 * the header row as a schema definition if set to <i>true</i>.
	 *
	 * @param aPathFileName Path file name identifying a CSV file
	 * @param aWithHeaders If <i>true</i>, then the first row will be read to identify the column headers
	 *
	 * @return Optional data grid instance
	 *
	 * @throws IOException I/O exception
	 */
	public Optional<DataGrid> load(String aPathFileName, boolean aWithHeaders)
		throws IOException
	{
		Optional<DataGrid> optDataGrid;

		File csvFile = new File(aPathFileName);
		if (! csvFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		try (FileReader fileReader = new FileReader(csvFile))
		{
			optDataGrid = load(fileReader, aWithHeaders);
			if (optDataGrid.isPresent())
			{
				DataDoc schemaDoc = optDataGrid.get().getColumns();
				if (StringUtils.isEmpty(schemaDoc.getName()))
					schemaDoc.setName(csvFile.getName());
			}
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}

		return optDataGrid;
	}

	/**
	 * Loads an optional data grid instance from the specified file name using
	 * an external schema definition file for the grid columns.
	 *
	 * @param aPathFileName Path file name identifying a CSV file
	 *
	 * @return Optional data grid instance
	 *
	 * @throws IOException I/O exception
	 * @throws ParserConfigurationException Parser exception
	 * @throws SAXException Parser exception
	 */
	public Optional<DataGrid> loadWithSchemaFile(String aPathFileName)
		throws IOException, ParserConfigurationException, SAXException
	{
		Optional<DataGrid> optDataGrid;

		File csvFile = new File(aPathFileName);
		if (! csvFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		String schemaPathFileName = StringUtils.replaceIgnoreCase(aPathFileName, ".csv", ".xml");
		File schemaFile = new File(schemaPathFileName);
		if (! schemaFile.exists())
			throw new IOException(schemaPathFileName + ": Does not exist.");

		DataDocXML dataDocXML = new DataDocXML();
		dataDocXML.load(schemaPathFileName);
		DataDoc schemaDataDoc = dataDocXML.getDataDoc();
		if (StringUtils.isEmpty(schemaDataDoc.getName()))
			schemaDataDoc.setName(schemaFile.getName());
		mDataGrid.setColumns(schemaDataDoc);

		try (FileReader fileReader = new FileReader(csvFile))
		{
			optDataGrid = load(fileReader, false);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}

		return optDataGrid;
	}

	/**
	 * This method will load a CSV file into memory, analyze it and
	 * return summary detail report grid.  This method can be resource
	 * intensive, so you should be careful about the size of the CSV
	 * file (e.g. number of columns and rows) - limit it to 10K rows
	 * and 100 columns.
	 *
	 * After the analysis is done and the details are returned as a
	 * grid, you can get the original data grid that was analyzed
	 * via the "getDataGrid()" method.
	 *
	 * @param aPathFileName CSV file name
     * @param aWithHeaders If <i>true</i>, then the first row will
	 *                     be read to identify the column headers
	 *
	 * @return Optional Data grid of analysis details
	 *
	 * @throws IOException Thrown if an I/O issue is detected
	 */
	public Optional<DataGrid> analyze(String aPathFileName, boolean aWithHeaders)
		throws IOException
	{
		Optional<DataGrid> optDataGrid;

		File csvFile = new File(aPathFileName);
		if (! csvFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		try (FileReader fileReader = new FileReader(csvFile))
		{
			load(fileReader, aWithHeaders);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}

		DataAnalyzer dataAnalyzer = new DataAnalyzer(mDataGrid.getColumns());
		dataAnalyzer.scan(mDataGrid);

		return Optional.ofNullable(dataAnalyzer.getDetails());
	}

	/**
	 * Generates a CSV column header name based on the data item.
	 *
	 * @param aDataItem Data item instance.
	 * @param anIsTitleOnly If true, then the column name will not include a field name.
	 *
	 * @return CSV column header name string
	 */
	public String dataItemToColumnName(DataItem aDataItem, boolean anIsTitleOnly)
	{
		String itemName = aDataItem.getName();
		String itemTitle = aDataItem.getTitle();

		if (anIsTitleOnly)
		{
			if (StringUtils.isEmpty(itemTitle))
				itemTitle = Data.nameToTitle(itemName);

			return itemTitle;
		}
		else
		{
			StringBuilder stringBuilder = new StringBuilder(itemName);
			stringBuilder.append(String.format("[%s]", aDataItem.getType().name()));
			if (StringUtils.isNotEmpty(itemTitle))
				stringBuilder.append(String.format("(%s)", itemTitle));

			return stringBuilder.toString();
		}
	}

	/**
	 * Creates a string of CSV header fields.
	 *
	 * @param aColumns Data document instance of columns.
	 * @param anIsTitleOnly Limit the column headers to just title strings.
	 *
	 * @return String representing the columns of the CSV data grid.
	 */
	public String dataDocToColumnNames(DataDoc aColumns, boolean anIsTitleOnly)
	{
		StringBuilder stringBuilder = new StringBuilder();
		if (aColumns.count() > 0)
		{
			for (DataItem dataItem : aColumns.getItems())
			{
				if (stringBuilder.length() > 0)
					stringBuilder.append(StrUtl.CHAR_COMMA);
				if (mIsFieldNamePreferred)
					stringBuilder.append(dataItem.getName());
				else
					stringBuilder.append(dataItemToColumnName(dataItem, anIsTitleOnly));
			}
		}

		return stringBuilder.toString();
	}

	/**
	 * Saves the previous assigned data grid (e.g. via constructor or set method)
	 * to the <i>PrintWriter</i> output stream.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aWriter Writer output stream.
	 * @param aWithHeaders If <i>true</i>, then column headers will be stored
	 *                     in the CSV file.
	 * @param anIsTitleOnly Limit the column headers to just title strings.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void save(DataGrid aDataGrid, Writer aWriter, boolean aWithHeaders, boolean anIsTitleOnly)
		throws IOException
	{
		if (aDataGrid == null)
			throw new IOException("Data Grid is null - cannot process");

		int colCount = aDataGrid.colCount();
		int rowCount = aDataGrid.rowCount();
		if ((rowCount > 0) && (colCount > 0))
		{
			DataDoc dataDoc;
			Optional<DataDoc> optDataDoc;

			try (CsvListWriter csvListWriter = new CsvListWriter(aWriter, fileFormatToCsvPreference()))
			{
				if (aWithHeaders)
				{
					int colOffset = 0;
					String headerName;

					String[] headerColumns = new String[colCount];
					for (DataItem dataItem : aDataGrid.getColumns().getItems())
					{
						if (mIsFieldNamePreferred)
							headerName = dataItem.getName();
						else
							headerName = dataItemToColumnName(dataItem, anIsTitleOnly);
						headerColumns[colOffset++] = headerName;
					}
					csvListWriter.writeHeader(headerColumns);
				}
				String[] rowCells = new String[colCount];
				for (int row = 0; row < rowCount; row++)
				{
					optDataDoc = aDataGrid.getRowAsDocOptional(row);
					if (optDataDoc.isPresent())
					{
						int colOffset = 0;
						dataDoc = optDataDoc.get();

						for (DataItem dataItem : dataDoc.getItems())
						{
							if (dataItem.isValueAssigned())
							{
								if (dataItem.isMultiValue())
								{
									if (mDelimiterChar == StrUtl.CHAR_NULL)
										rowCells[colOffset++] = dataItem.getValuesCollapsed();
									else
										rowCells[colOffset++] = dataItem.getValuesCollapsed(mDelimiterChar);
								}
								else if (mIsFormatted)
									rowCells[colOffset++] = dataItem.getValueFormatted();
								else
									rowCells[colOffset++] = dataItem.getValue();
							}
							else
								rowCells[colOffset++] = StringUtils.EMPTY;
						}
						csvListWriter.write(rowCells);
					}
				}
			}
			catch (Exception e)
			{
				throw new IOException(e.getMessage());
			}
		}
	}

	/**
	 * Saves the previous assigned table (e.g. via constructor or set method)
	 * to the <i>PrintWriter</i> output stream.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aPathFileName Absolute path/file name.
	 * @param aWithHeaders If <i>true</i>, then column headers will be stored
	 *                     in the CSV file.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void save(DataGrid aDataGrid, String aPathFileName, boolean aWithHeaders)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
		{
			save(aDataGrid, printWriter, aWithHeaders, false);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}

	/**
	 * Saves the data grid instance to the <i>aPathFileName</i> using the column
	 * titles in the header row.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aPathFileName Absolute path/file name.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void saveWithTitleHeader(DataGrid aDataGrid, String aPathFileName)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
		{
			save(aDataGrid, printWriter, true, true);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}

	/**
	 * Saves the data grid instance to the <i>aPathFileName</i> with schema details
	 * in the header row.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aPathFileName Absolute path/file name.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void saveWithSchemaHeader(DataGrid aDataGrid, String aPathFileName)
		throws IOException
	{
		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
		{
			save(aDataGrid, printWriter, true, false);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}

	/**
	 * Saves the data grid instance to the <i>aPathFileName</i> with schema details
	 * define in an external XML file.
	 *
	 * @param aDataGrid Data grid instance.
	 * @param aPathFileName Absolute path/file name.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void saveWithSchemaFile(DataGrid aDataGrid, String aPathFileName)
		throws IOException
	{
		String schemaPathFileName = StringUtils.replaceIgnoreCase(aPathFileName, ".csv", ".xml");
		DataDocXML dataDocXML = new DataDocXML(mDataGrid.getColumns());
		dataDocXML.save(schemaPathFileName);

		try (PrintWriter printWriter = new PrintWriter(aPathFileName, StrUtl.CHARSET_UTF_8))
		{
			save(aDataGrid, printWriter, false, false);
		}
		catch (Exception e)
		{
			throw new IOException(aPathFileName + ": " + e.getMessage());
		}
	}
}
