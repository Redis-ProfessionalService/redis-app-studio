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

package com.redis.ds.ds_grid;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.*;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.io.DataGridCSV;
import com.redis.foundation.io.DataGridConsole;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Optional;

/**
 * The grid data source manages a row x column grid of
 * <i>DataItem</i> cells in memory.  It implements the five CRUD+S
 * methods and supports advanced queries using a <i>DSCriteria</i>.
 * In addition, this data source offers save and load methods for
 * data values.  This can be a useful data source if your grid
 * size is small in nature and there is sufficient heap space
 * available.
 *
 * @since 1.0
 * @author Al Cole
 */
public class GridDS
{
	protected final int SEARCH_LIMIT_DEFAULT = 10;
	protected final int SUGGEST_LIMIT_DEFAULT = 5;

	protected AppCtx mAppCtx;
	protected DataGrid mDataGrid;

	/**
	 * Constructs the Grid data source using default application properties.
	 *
	 * @param anAppCtx Application context
	 */
	public GridDS(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
		mDataGrid = new DataGrid("Grid Data Source");
	}

	/**
	 * Constructs the Grid data source using default application properties
	 * and assigns the data document instance as the schema.
	 *
	 * @param anAppCtx Application context
	 * @param aDataDoc Data document instance
	 */
	public GridDS(AppCtx anAppCtx, DataDoc aDataDoc)
	{
		mAppCtx = anAppCtx;
		mDataGrid = new DataGrid(aDataDoc);
	}

	/**
	 * Constructs the Grid data source using default application properties
	 * and assigns the data document instance as the schema.
	 *
	 * @param anAppCtx Application context
	 * @param aDataGrid Data grid instance
	 */
	public GridDS(AppCtx anAppCtx, DataGrid aDataGrid)
	{
		mAppCtx = anAppCtx;
		mDataGrid = new DataGrid(aDataGrid);
	}

	/**
	 * Returns a string summary representation of the data source.
	 *
	 * @return String summary representation of the data source
	 */
	@Override
	public String toString()
	{
		return mDataGrid.toString();
	}

	/**
	 * Returns the file name (derived from the internal data source name).
	 *
	 * @return Data source schema definition file name
	 */
	public String createSchemaFileName()
	{
		String fileName = StrUtl.removeAllChar(mDataGrid.getName().toLowerCase(), StrUtl.CHAR_SPACE);
		return String.format("ds_schema_%s.xml", fileName);
	}

	/**
	 * Returns the path/file name derived from the path name, value format type
	 * (e.g. xml or csv) and the internal data source name.
	 *
	 * @param aPathName Path name where the file should be written.
	 *
	 * @return Data source values path/file name.
	 */
	public String createSchemaPathFileName(String aPathName)
	{
		return String.format("%s%c%s", aPathName, File.separatorChar, createSchemaFileName());
	}

	/**
	 * Returns the SmartClient path/file name (derived from the path name parameter
	 * and the internal data source name).
	 *
	 * @param aPathName Path name where the file should be written
	 *
	 * @return Smart GWT data source definition path/file name
	 */
	public String createSmartClientPathFileName(String aPathName)
	{
		String dsName = StrUtl.removeAllChar(mDataGrid.getName().toLowerCase(), StrUtl.CHAR_SPACE);
		return String.format("%s%c%s.ds.xml", aPathName, File.separatorChar, dsName);
	}

	/**
	 * Stores the schema definition of the underlying data source
	 * (formatted in XML) to the file system.
	 *
	 * @param aPathFileName Path/file schema should be written to
	 *
	 * @throws IOException I/O related exception
	 */
	public void saveSchema(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDocXML dataDocXML = new DataDocXML(mDataGrid.getColumns());
		dataDocXML.save(aPathFileName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the schema definition file and assigns it to the internal
	 * data grid instance.
	 *
	 * <b>Note:</b> This operation will result in the emptying of
	 * the data grid.
	 *
	 * @param aPathFileName Schema path file name
	 *
	 * @throws IOException I/O exception
	 * @throws ParserConfigurationException Parser exception
	 * @throws SAXException DOM exception
	 */
	public void loadSchema(String aPathFileName)
		throws IOException, ParserConfigurationException, SAXException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDocXML dataDocXML = new DataDocXML();
		dataDocXML.load(aPathFileName);
		DataDoc schemaDoc = dataDocXML.getDataDoc();
		mDataGrid.emptyAll();
		if (StringUtils.isEmpty(schemaDoc.getName()))
			schemaDoc.setName(mDataGrid.getName());
		mDataGrid.setColumns(dataDocXML.getDataDoc());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the data items captured in the <i>DataDoc</i>
	 * against the schema.  The data items must be derived
	 * from the schema definition.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aSchemaDoc Data document instance
	 *
	 * @return <i>true</i> on success and <i>false</i> otherwise
	 */
	public boolean updateSchema(DataDoc aSchemaDoc)
	{
		String itemName, featureName, featureValue;
		Logger appLogger = mAppCtx.getLogger(this, "updateSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = false;
		DataDoc schemaDoc = mDataGrid.getColumns();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aSchemaDoc.getValueByName("item_name"));
		if (optDataItem.isPresent())
		{
			DataItem schemaItem = optDataItem.get();
			schemaItem.clearFeatures();
			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				itemName = dataItem.getName();
				if (itemName.equals("item_title"))
				{
					schemaItem.setTitle(dataItem.getValue());
					if (! isOK) isOK = true;
				}
				else if (! itemName.startsWith("item_"))
				{
					featureName = dataItem.getName();
					featureValue = dataItem.getValue();
					if (featureName.startsWith("is"))
					{
						if (Data.isValueTrue(featureValue))
						{
							if ((featureName.equals(Data.FEATURE_IS_SEARCH)) || (featureName.equals(Data.FEATURE_IS_SUGGEST)))
							{
								if (Data.isText(schemaItem.getType()))
								{
									schemaItem.addFeature(featureName, featureValue);
									if (! isOK) isOK = true;
								}
							}
							else
							{
								schemaItem.addFeature(featureName, featureValue);
								if (! isOK) isOK = true;
							}
						}
					}
					else
					{
						schemaItem.addFeature(featureName, featureValue);
						if (! isOK) isOK = true;
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Saves the schema of the underlying data source (formatted
	 * as a SmartClient DS XML file.  The name of the file is
	 * derived from the schema of the data source and the location
	 * of the file is specified as a parameter.
	 *
	 * <b>Note:</b> Developers are encouraged to override this
	 * method if you have a specialized storage scenario.
	 *
	 * @param aPathName Path name where the file should be written
	 *
	 * @throws IOException I/O related exception.
	 */
	public void saveSmartClient(String aPathName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveSmartClient");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String pathFileName = createSmartClientPathFileName(aPathName);
		SmartClientXML smartClientXML = new SmartClientXML();
		smartClientXML.save(pathFileName, mDataGrid.getColumns());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Loads the data (formatted as a CSV file) into the internal
	 * data grid.
	 *
	 * @param aPathFileName CSV path file name
	 * @param aIsHeaderSchema If <i>true</i>, then the header will parsed as the schema
	 *
	 * @throws DSException Data source exception
	 * @throws IOException I/O exception
	 */
	public void loadData(String aPathFileName, boolean aIsHeaderSchema)
		throws DSException, IOException
	{
		DataGridCSV dataGridCSV;
		Logger appLogger = mAppCtx.getLogger(this, "loadData");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isNotEmpty(aPathFileName))
		{
			if (aIsHeaderSchema)
			{
				mDataGrid.emptyAll();
				dataGridCSV = new DataGridCSV();
			}
			else
			{
				mDataGrid.emptyRows();
				DataDoc schemaDoc = mDataGrid.getColumns();
				dataGridCSV = new DataGridCSV(schemaDoc);
				Optional<DataItem> optDataItem = schemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
				if (optDataItem.isEmpty())
					dataGridCSV.setRowNumberPrimaryKey(true);
			}
			Optional<DataGrid> optDataGrid = dataGridCSV.load(aPathFileName, aIsHeaderSchema);
			if (optDataGrid.isPresent())
			{
				DataGrid dataGrid = optDataGrid.get();
				dataGrid.setName(mDataGrid.getName());
				mDataGrid = dataGrid;
			}
			else
				throw new DSException("Unable to load CSV file: " + aPathFileName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Saves the data (formatted as a CSV file) to the file system.
	 *
	 * @param aPathFileName CSV path file name
	 * @param aIsHeaderSchema If <i>true</i>, then the header will saved as the schema
	 *
	 * @throws DSException Data source exception
	 * @throws IOException I/O exception
	 */
	public void saveData(String aPathFileName, boolean aIsHeaderSchema)
		throws DSException, IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveData");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isNotEmpty(aPathFileName))
		{
			DataGridCSV dataGridCSV = new DataGridCSV();
			dataGridCSV.save(mDataGrid, aPathFileName, aIsHeaderSchema);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Assign a name to the data source.
	 *
	 * @param aName Name of the data source
	 */
	public void setName(String aName)
	{
		mDataGrid.setName(aName);
	}

	/**
	 * Returns the name of the data source.
	 *
	 * @return Data source name
	 */
	public String getName()
	{
		return mDataGrid.getName();
	}

	/**
	 * Assigns column name serving as the unique primary key.
	 *
	 * @param aName Name of the column
	 */
	public void setPrimaryKey(String aName)
	{
		DataDoc schemaDoc = mDataGrid.getColumns();
		Optional<DataItem> optDataItem = schemaDoc.getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			dataItem.enableFeature(Data.FEATURE_IS_PRIMARY);
		}
	}

	/**
	 * Returns the gird data source schema data document instance.
	 *
	 * @return Data document instance
	 */
	public DataDoc getSchema()
	{
		return mDataGrid.getColumns();
	}

	/**
	 * Assigns a data grid instance to replace the internally
	 * managed one.
	 *
	 * @param aDatGrid Data grid instance
	 */
	public void setDatGrid(DataGrid aDatGrid)
	{
		mDataGrid = aDatGrid;
	}

	/**
	 * Returns the internally managed data grid instance
	 *
	 * @return Data grid instance
	 */
	public DataGrid getDataGrid()
	{
		return mDataGrid;
	}

	/**
	 * Calculates a count (using a wildcard criteria) of all the
	 * rows stored in the content source and returns that value.
	 *
	 * @return Count of all rows in the data source
	 *
	 * @throws DSException Data source related exception
	 */
	public int count()
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "count");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int rowCount = mDataGrid.rowCount();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rowCount;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of all rows fetched
	 * from the underlying data source (using a wildcard criteria).
	 *
	 * @return DataGrid representing all rows in the data source
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid fetch()
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "fetch");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = mDataGrid;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * start at the offset specified with the row count limited
	 * by the parameter.
	 *
	 * @param anOffset    Starting offset into the matching content rows.
	 * @param aLimit      Limit on the total number of rows to extract from
	 *                    the content source during this fetch operation.
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the content source (based on the offset and limit values).
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid fetch(int anOffset, int aLimit)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "fetch");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DSCriteria dsCriteria = new DSCriteria(String.format("%s Criteria", getName()));
		GridCriteria gridCriteria = new GridCriteria(mAppCtx, mDataGrid.getColumns());
		gridCriteria.prepare(dsCriteria, anOffset, aLimit);
		DataGrid dataGrid = gridCriteria.execute(mDataGrid);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the <i>DSCriteria</i> specified in the parameter.
	 *
	 * @param aDSCriteria Data source criteria
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the content source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid fetch(DSCriteria aDSCriteria)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "fetch");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		GridCriteria gridCriteria = new GridCriteria(mAppCtx, mDataGrid.getColumns());
		gridCriteria.prepare(aDSCriteria);
		DataGrid dataGrid = gridCriteria.execute(mDataGrid);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the <i>DSCriteria</i> specified in the parameter.  In
	 * addition, this method offers a paging mechanism where the
	 * starting offset and a fetch limit can be applied to each
	 * content fetch query.
	 *
	 * @param aDSCriteria Data source criteria.
	 * @param anOffset    Starting offset into the matching content rows.
	 * @param aLimit      Limit on the total number of rows to extract from
	 *                    the content source during this fetch operation.
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the content source (based on the offset and limit values).
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid fetch(DSCriteria aDSCriteria, int anOffset, int aLimit)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "fetch");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		GridCriteria gridCriteria = new GridCriteria(mAppCtx, mDataGrid.getColumns());
		gridCriteria.prepare(aDSCriteria, anOffset, aLimit);
		DataGrid dataGrid = gridCriteria.execute(mDataGrid);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the suggestion fragment string (case insensitive),
	 * applying the logical operator and limiting the results
	 * returned.
	 *
	 * @param aFragment String fragment (case insensitive)
	 * @param anOperator Logical operator
	 * @param aLimit Result limit
	 *
	 * @return Data grid representing all rows that match the
	 * suggestion query against the data source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid suggest(String aFragment, Data.Operator anOperator, int aLimit)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "suggest");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = mDataGrid.getColumns();
		Optional<DataItem> optDataItem = schemaDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_SUGGEST);
		if (optDataItem.isEmpty())
			throw new DSException(String.format("Data Grid is missing data item '%s' feature.", Data.FEATURE_IS_SUGGEST));
		DataItem dataItem = optDataItem.get();
		DSCriteria dsCriteria = new DSCriteria(String.format("%s Suggest Criteria", getName()));
		dsCriteria.setCaseSensitive(false);
		GridCriteria gridCriteria = new GridCriteria(mAppCtx, schemaDoc);
		dsCriteria.add(new DSCriterion(dataItem.getName(), anOperator, aFragment));
		gridCriteria.prepare(dsCriteria, 0, aLimit);
		DataGrid dataGrid = gridCriteria.execute(mDataGrid);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the suggestion fragment string (case insensitive),
	 * applying the logical operator and limiting the results
	 * returned.
	 *
	 * @param aFragment String fragment (case insensitive)
	 * @param aLimit Result limit
	 *
	 * @return Data grid representing all rows that match the
	 * suggestion query against the data source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid suggest(String aFragment, int aLimit)
		throws DSException
	{
		return suggest(aFragment, Data.Operator.STARTS_WITH, aLimit);
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the suggestion fragment string (case insensitive),
	 * applying the logical operator and limiting the results
	 * returned.
	 *
	 * @param aFragment String fragment (case insensitive)
	 *
	 * @return Data grid representing all rows that match the
	 * suggestion query against the data source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid suggest(String aFragment)
		throws DSException
	{
		return suggest(aFragment,SUGGEST_LIMIT_DEFAULT);
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the search term(s) (case insensitive), applying
	 * the logical operator and limiting the results returned.
	 *
	 * @param aTerms Search terms (case insensitive)
	 * @param anOperator Logical operator
	 * @param anOffset Starting offset
	 * @param aLimit Result limit
	 *
	 * @return Data grid representing all rows that match the
	 * search terms query against the data source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid search(String aTerms, Data.Operator anOperator, int anOffset, int aLimit)
		throws DSException
	{
		DataGrid dataGrid;
		Logger appLogger = mAppCtx.getLogger(this, "search");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aTerms))
			return fetch(anOffset, aLimit);

		DataDoc schemaDoc = mDataGrid.getColumns();
		ArrayList<DataItem> dataItemList = schemaDoc.getItemByFeatureName(Data.FEATURE_IS_SEARCH);
		if (dataItemList.isEmpty())
			throw new DSException(String.format("Data Grid is missing data item '%s' feature.", Data.FEATURE_IS_SEARCH));
		DataGrid resultGrid = new DataGrid(schemaDoc);
		GridCriteria gridCriteria = new GridCriteria(mAppCtx, schemaDoc);
		DSCriteria dsCriteria = new DSCriteria(String.format("'%s' Search Criteria", aTerms));
		for (DataItem searchItem : dataItemList)
		{
			dsCriteria.reset();
			dsCriteria.setCaseSensitive(false);
			dsCriteria.add(new DSCriterion(searchItem.getName(), anOperator, aTerms));
			gridCriteria.prepare(dsCriteria, 0, aLimit);
			dataGrid = gridCriteria.execute(mDataGrid);
			if (dataGrid.rowCount() > 0)
				resultGrid.addRows(dataGrid);
		}
		int rowCount = resultGrid.rowCount();
		if (rowCount == 0)
			resultGrid.addFeature(DS.FEATURE_NEXT_OFFSET, 0);
		else
			resultGrid.addFeature(DS.FEATURE_NEXT_OFFSET, Math.min(rowCount-1, anOffset + aLimit));
		resultGrid.addFeature(DS.FEATURE_CUR_LIMIT, aLimit);
		resultGrid.addFeature(DS.FEATURE_CUR_OFFSET, anOffset);
		resultGrid.addFeature(DS.FEATURE_TOTAL_DOCUMENTS, rowCount);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return resultGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the search term(s) (case insensitive), starting at
	 * the offset and limiting the results returned.
	 *
	 * @param aTerms Search terms (case insensitive)
	 * @param anOffset Starting offset
	 * @param aLimit Result limit
	 *
	 * @return Data grid representing all rows that match the
	 * search terms query against the data source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid search(String aTerms, int anOffset, int aLimit)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "search");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = search(aTerms, Data.Operator.CONTAINS, anOffset, aLimit);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the rows that
	 * match the search term(s) (case insensitive).
	 *
	 * @param aTerms Search terms (case insensitive)
	 *
	 * @return Data grid representing all rows that match the
	 * search terms query against the data source.
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid search(String aTerms)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "search");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = search(aTerms, Data.Operator.CONTAINS, 0, SEARCH_LIMIT_DEFAULT);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Finds and returns an optional data document if its primary id can be
	 * matched in the internally managed data grid instance.
	 *
	 *  <b>Note:</b> The data document must designate a data item
	 * 	as a primary key and that value must be assigned prior to
	 * 	using this method.
	 *
	 * @param anId Primary id value
	 *
	 * @return Optional data document
	 */
	public Optional<DataDoc> findDataDocByPrimaryId(String anId)
	{
		Logger appLogger = mAppCtx.getLogger(this, "findDataDocByPrimaryId");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataDoc dataDoc = null;
		Optional<DataItem> optDataItem = mDataGrid.getColumns().getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataItem diPrimaryKey = optDataItem.get();
			DSCriteria dsCriteria = new DSCriteria("Criteria - Primary Key");
			dsCriteria.addFeature(Data.FEATURE_DS_OFFSET, 0);
			dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1);
			dsCriteria.add(diPrimaryKey.getName(), Data.Operator.EQUAL, anId);
			try
			{
				DataGrid dataGrid = fetch(dsCriteria);
				if (dataGrid.rowCount() == 1)
					dataDoc = dataGrid.getRowAsDoc(0);
				else
					appLogger.error(String.format("Unable to isolate data document by primary id '%s'.", diPrimaryKey.getValue()));

			}
			catch (DSException e)
			{
				appLogger.error(e.getMessage());
			}
		}
		else
			appLogger.error("Schema missing primary key item.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(dataDoc);
	}

	/**
	 * Finds and returns an optional data document if its primary id can be
	 * matched in the internally managed data grid instance.
	 *
	 *  <b>Note:</b> The data document must designate a data item
	 * 	as a primary key and that value must be assigned prior to
	 * 	using this method.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return Optional data document
	 */
	public Optional<DataDoc> findDataDocByPrimaryId(DataDoc aDataDoc)
	{
		Logger appLogger = mAppCtx.getLogger(this, "findDataDocByPrimaryId");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataDoc> optDataDoc = Optional.empty();
		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
		{
			DataItem diPrimaryKey = optDataItem.get();
			optDataDoc = findDataDocByPrimaryId(diPrimaryKey.getValue());
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return optDataDoc;
	}

	/**
	 * Adds the data items  captured in the <i>DataDoc</i> to
	 * the data source.  The data items must be derived from the
	 * same schema definition.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> on success and <i>false</i> otherwise
	 *
	 * @throws DSException Data source related exception
	 */
	public boolean add(DataDoc aDataDoc)
		throws DSException
	{
		DataItem diPrimaryKey;
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Determine if the primary key has been assigned - if not, then assign one now.

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
			throw new DSException("Data document missing primary key item.");

		String primaryKey = aDataDoc.getValueByName(diPrimaryKey.getName());
		if (StringUtils.isEmpty(primaryKey))
		{
			if (Data.isNumber(diPrimaryKey.getType()))
			{
				DescriptiveStatistics descriptiveStatistics = mDataGrid.getDescriptiveStatistics(diPrimaryKey.getName());
				double piMax = descriptiveStatistics.getMax();
				aDataDoc.setValueByName(diPrimaryKey.getName(), Math.round(piMax));
			}
			else
				aDataDoc.setValueByName(diPrimaryKey.getName(), aDataDoc.generateUniqueHash(false));
		}

		boolean isOK = mDataGrid.addRow(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Updates the data items captured in the <i>DataDoc</i>
	 * within the data source.  The data items must be derived
	 * from the schema definition.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> on success and <i>false</i> otherwise
	 *
	 * @throws DSException Data source related exception
	 */
	public boolean update(DataDoc aDataDoc)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = mDataGrid.update(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Convenience method that will pre-load a <i>DataDoc</i> from
	 * the data source, apply the changes identified in the method
	 * parameter, perform the update operation and return the full
	 * version of the data document instance.
	 *
	 * @param aDataDoc Data document instance (just items that changed)
	 *
	 * @return Data document instance reflect complete updates
	 *
	 * @throws DSException Data source related exception
	 */
	public DataDoc loadApplyUpdate(DataDoc aDataDoc)
		throws DSException
	{
		DataItem diPrimaryKey;
		Logger appLogger = mAppCtx.getLogger(this, "loadApplyUpdate");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
		if (optDataItem.isPresent())
			diPrimaryKey = optDataItem.get();
		else
			throw new DSException("Data document missing primary key item.");

		DSCriteria dsCriteria = new DSCriteria("Criteria - Primary Key");
		dsCriteria.addFeature(Data.FEATURE_DS_OFFSET, 0);
		dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, 1);
		dsCriteria.add(diPrimaryKey.getName(), Data.Operator.EQUAL, diPrimaryKey.getValue());
		DataGrid dataGrid = fetch(dsCriteria);
		if (dataGrid.rowCount() != 1)
			throw new DSException(String.format("Unable to isolate data document by primary id '%s'.", diPrimaryKey.getValue()));
		DataDoc dataDoc = dataGrid.getRowAsDoc(0);
		for (DataItem dataItem : aDataDoc.getItems())
			dataDoc.setValueByName(dataItem.getName(), dataItem.getValue());
		boolean isOK = update(dataDoc);
		if (! isOK)
			throw new DSException(String.format("Unable to update data document with primary id '%s'.", diPrimaryKey.getValue()));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataDoc;
	}

	/**
	 * Deletes the row identified by <i>DataDoc</i> within the
	 * data source.
	 *
	 * <b>Note:</b> The data document must designate a data item
	 * as a primary key and that value must be assigned prior to
	 * using this method.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return <i>true</i> on success and <i>false</i> otherwise
	 *
	 * @throws DSException Data source related exception
	 */
	public boolean delete(DataDoc aDataDoc)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "delete");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = mDataGrid.delete(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	private String locateMedian(String anItemName)
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "locateMedian");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String medianValue = StringUtils.EMPTY;
		int medianOffset = (int) Math.ceil((double)mDataGrid.rowCount() / 2);

		DSCriteria dsCriteria = new DSCriteria("Sort Criteria");
		dsCriteria.addFeature(Data.FEATURE_DS_OFFSET, 0);
		dsCriteria.addFeature(Data.FEATURE_DS_LIMIT, medianOffset);
		dsCriteria.add(anItemName, Data.Operator.SORT, Data.Order.ASCENDING.name());

		DataGrid dataGrid = fetch(dsCriteria);
		int rowCount = dataGrid.rowCount();
		if (rowCount > 1)
		{
			DataDoc medianDoc = dataGrid.getRowAsDoc(rowCount-1);
			medianValue = medianDoc.getValueByName(anItemName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return medianValue;
	}

	/**
	 * Analyzes the contents of the grid to determine its statistical
	 * composition.
	 *
	 * @param aSampleCount Sample value size
	 *
	 * @return DataGrid instance of item analysis details
	 *
	 * @throws DSException Data source related exception
	 */
	public DataGrid analyze(int aSampleCount)
		throws DSException
	{
		DataDoc detailDoc;
		Data.Type dataType;
		String medianValue;
		Logger appLogger = mAppCtx.getLogger(this, "analyze");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int dataRowCount = mDataGrid.rowCount();
		if (dataRowCount < 2)
			throw new DSException("The grid must have 2 or more rows to perform an analysis operation.");

// We will leverage the foundation data analyzer class for everything except the median selection.

		DataAnalyzer dataAnalyzer = new DataAnalyzer(mDataGrid.getColumns());
		dataAnalyzer.scan(mDataGrid);

// Identify the median selection using the GridDS criteria query features.

		DataGrid detailsGrid = dataAnalyzer.getDetails();
		int detailsRowsCount = detailsGrid.rowCount();
		for (int row = 0; row < detailsRowsCount; row++)
		{
			detailDoc = detailsGrid.getRowAsDoc(row);
			dataType = Data.stringToType(detailDoc.getValueByName("type"));
			if (Data.isNumber(dataType))
			{
				medianValue = locateMedian(detailDoc.getValueByName("name"));
				if (StringUtils.isNotEmpty(medianValue))
					detailsGrid.setValueByRowName(row, "median", medianValue);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return detailsGrid;
	}

	/**
	 * Convenience method to generate the contents of the internally managed
	 * grid to the console.
	 *
	 * @param aTitle Title of grid output
	 */
	public void consoleWrite(String aTitle)
	{
		String gridTitle;
		Logger appLogger = mAppCtx.getLogger(this, "consoleWrite");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isNotEmpty(aTitle))
			gridTitle = aTitle;
		else
			gridTitle = mDataGrid.getName();

		DataGridConsole dataGridConsole = new DataGridConsole();
		dataGridConsole.setFormattedFlag(true);
		PrintWriter printWriter = new PrintWriter(System.out, true);
		dataGridConsole.write(mDataGrid, printWriter, gridTitle, 40, 1);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
