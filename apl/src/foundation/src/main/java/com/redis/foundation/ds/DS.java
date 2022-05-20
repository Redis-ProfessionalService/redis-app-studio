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

package com.redis.foundation.ds;

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * The DS (data source) class captures the constants, enumerated types
 * and utility methods for the data source package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DS
{
	public static final String VALUE_FORMAT_TYPE_CSV = "csv";
	public static final String VALUE_FORMAT_TYPE_XML = "xml";
	public static final String VALUE_FORMAT_TYPE_TXT = "txt";

	public static final String TXT_GRID_ITEM_NAME = "item";
	public static final String TXT_GRID_ITEM_TITLE = "Item";

	public static final String CRITERION_VALUE_IS_NULL = "+[NULL]+";

	public static final String CRITERIA_ENTRY_TYPE_NAME = "item_type";
	public static final String CRITERIA_ENTRY_ITEM_NAME = "item_name";
	public static final String CRITERIA_VALUE_ITEM_NAME = "item_value";
	public static final String CRITERIA_BOOLEAN_ITEM_NAME = "boolean_operator";
	public static final String CRITERIA_OPERATOR_ITEM_NAME = "logical_operator";

	public static final String PROPERTY_INSTANCE = "Instance";
	public static final String PROPERTY_CONNECTION_POOL = "jdbc_connection_pool";

// Data source criteria item definitions

	public static final int CRITERIA_QUERY_LIMIT_DEFAULT = 10;
	public static final int CRITERIA_QUERY_OFFSET_DEFAULT = 0;
	public static final int CRITERIA_FACET_VALUE_COUNT_DEFAULT = 10;

	public static final String CRITERIA_PREFIX = "r1_";
	public static final String CRITERIA_QUERY_LIMIT = CRITERIA_PREFIX + "query_limit";
	public static final String CRITERIA_QUERY_OFFSET = CRITERIA_PREFIX + "query_offset";
	public static final String CRITERIA_QUERY_STRING = CRITERIA_PREFIX + "query_string";

// Data source UI fetch policy definitions

	public static final String FETCH_POLICY_PAGING = "paging";
	public static final String FETCH_POLICY_VIRTUAL = "virtual";

// Data source feature definitions

	public static final String FEATURE_CUR_LIMIT = "curLimit";
	public static final String FEATURE_CUR_OFFSET = "curOffset";
	public static final String FEATURE_ROW_NUMBER = "rowNumber";
	public static final String FEATURE_NEXT_OFFSET = "nextOffset";
	public static final String FEATURE_TOTAL_DOCUMENTS = "totalDocuments";

// Redis data source definitions

	public static final String REDIS_STORAGE_TYPE_DEFAULT = "LH";

// SQL field feature value constants.

	public static final String SQL_TABLE_TYPE_STORED = "Stored";
	public static final String SQL_TABLE_TYPE_MEMORY = "Memory";

	public static final String SQL_INDEX_UNDEFINED = "Undefined";
	public static final String SQL_INDEX_TYPE_STANDARD = "Standard";
	public static final String SQL_INDEX_TYPE_FULLTEXT = "FullText";

	public static final String SQL_INDEX_POLICY_UNIQUE = "Unique";
	public static final String SQL_INDEX_POLICY_DUPLICATE = "Duplicate";

	public static final String SQL_INDEX_MANAGEMENT_IMPLICIT = "Implicit";
	public static final String SQL_INDEX_MANAGEMENT_EXPLICIT = "Explicit";

// SQL field feature constants.

	public static final String FEATURE_TYPE_ID = "typeId";
	public static final String FEATURE_IS_INDEXED = "isIndexed";
	public static final String FEATURE_INDEX_TYPE = "indexType";
	public static final String FEATURE_IS_CONTENT = "isContent";
	public static final String FEATURE_STORED_SIZE = "storedSize";
	public static final String FEATURE_INDEX_POLICY = "indexPolicy";
	public static final String FEATURE_FUNCTION_NAME = "functionName";
	public static final String FEATURE_SEQUENCE_SEED = "sequenceSeed";
	public static final String FEATURE_TABLESPACE_NAME = "tableSpace";
	public static final String FEATURE_OPERATION_NAME = "operationName";
	public static final String FEATURE_INDEX_FIELD_TYPE = "indexFieldType";
	public static final String FEATURE_SEQUENCE_INCREMENT = "sequenceIncrement";
	public static final String FEATURE_SEQUENCE_MANAGEMENT = "sequenceManagement";

	private DS()
	{
	}

	/**
	 * Retrieves the application prefix from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria application prefix
	 */
	public static String appPrefixFromCriteria(DSCriteria aDSCriteria)
	{
		String appPrefix;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_APP_PREFIX)))
			appPrefix = aDSCriteria.getFeature(Data.FEATURE_DS_APP_PREFIX);
		else
			appPrefix = StringUtils.EMPTY;

		return appPrefix;
	}

	/**
	 * Retrieves the application prefix from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria application prefix
	 */
	public static String appPrefixFromCriteria(DGCriteria aDGCriteria)
	{
		String appPrefix;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_APP_PREFIX)))
			appPrefix = aDGCriteria.getFeature(Data.FEATURE_DS_APP_PREFIX);
		else
			appPrefix = StringUtils.EMPTY;

		return appPrefix;
	}

	/**
	 * Retrieves the data source structure from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria data source structure
	 */
	public static String structureFromCriteria(DSCriteria aDSCriteria)
	{
		String dsStructure;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_STRUCTURE)))
			dsStructure = aDSCriteria.getFeature(Data.FEATURE_DS_STRUCTURE);
		else
			dsStructure = StringUtils.EMPTY;

		return dsStructure;
	}

	/**
	 * Retrieves the data source structure from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria data source structure
	 */
	public static String structureFromCriteria(DGCriteria aDGCriteria)
	{
		String dsStructure;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_STRUCTURE)))
			dsStructure = aDGCriteria.getFeature(Data.FEATURE_DS_STRUCTURE);
		else
			dsStructure = StringUtils.EMPTY;

		return dsStructure;
	}

	/**
	 * Retrieves the data source title from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria data source title
	 */
	public static String titleFromCriteria(DSCriteria aDSCriteria)
	{
		String dsTitle;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_TITLE)))
			dsTitle = aDSCriteria.getFeature(Data.FEATURE_DS_TITLE);
		else
			dsTitle = StringUtils.EMPTY;

		return dsTitle;
	}

	/**
	 * Retrieves the data source title from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria data source title
	 */
	public static String titleFromCriteria(DGCriteria aDGCriteria)
	{
		String dsTitle;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_TITLE)))
			dsTitle = aDGCriteria.getFeature(Data.FEATURE_DS_TITLE);
		else
			dsTitle = StringUtils.EMPTY;

		return dsTitle;
	}

	/**
	 * Retrieves the data source storage from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria data source storage
	 */
	public static String storageFromCriteria(DSCriteria aDSCriteria)
	{
		String dsStorage;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_STORAGE)))
			dsStorage = aDSCriteria.getFeature(Data.FEATURE_DS_STORAGE);
		else
			dsStorage = StringUtils.EMPTY;

		return dsStorage;
	}

	/**
	 * Retrieves the data source storage from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria data source storage
	 */
	public static String storageFromCriteria(DGCriteria aDGCriteria)
	{
		String dsStorage;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_STORAGE)))
			dsStorage = aDGCriteria.getFeature(Data.FEATURE_DS_STORAGE);
		else
			dsStorage = StringUtils.EMPTY;

		return dsStorage;
	}

	/**
	 * Retrieves the action name from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria action name
	 */
	public static String actionFromCriteria(DSCriteria aDSCriteria)
	{
		String actionName;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_ACTION)))
			actionName = aDSCriteria.getFeature(Data.FEATURE_DS_ACTION);
		else
			actionName = StringUtils.EMPTY;

		return actionName;
	}

	/**
	 * Retrieves the action name from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria action name
	 */
	public static String actionFromCriteria(DGCriteria aDGCriteria)
	{
		String actionName;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_ACTION)))
			actionName = aDGCriteria.getFeature(Data.FEATURE_DS_ACTION);
		else
			actionName = StringUtils.EMPTY;

		return actionName;
	}

	/**
	 * Retrieves the offset value from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria offset value
	 */
	public static int offsetFromCriteria(DSCriteria aDSCriteria)
	{
		int offset;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_OFFSET)))
			offset = aDSCriteria.getFeatureAsInt(Data.FEATURE_DS_OFFSET);
		else
			offset = CRITERIA_QUERY_OFFSET_DEFAULT;

		return offset;
	}

	/**
	 * Retrieves the offset value from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria offset value
	 */
	public static int offsetFromCriteria(DGCriteria aDGCriteria)
	{
		int offset;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_OFFSET)))
			offset = aDGCriteria.getFeatureAsInt(Data.FEATURE_DS_OFFSET);
		else
			offset = CRITERIA_QUERY_OFFSET_DEFAULT;

		return offset;
	}

	/**
	 * Retrieves the limit value from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria limit value
	 */
	public static int limitFromCriteria(DSCriteria aDSCriteria)
	{
		int offset;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_LIMIT)))
			offset = aDSCriteria.getFeatureAsInt(Data.FEATURE_DS_LIMIT);
		else
			offset = CRITERIA_QUERY_LIMIT_DEFAULT;

		return offset;
	}

	/**
	 * Retrieves the limit value from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria limit value
	 */
	public static int limitFromCriteria(DGCriteria aDGCriteria)
	{
		int offset;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_LIMIT)))
			offset = aDGCriteria.getFeatureAsInt(Data.FEATURE_DS_LIMIT);
		else
			offset = CRITERIA_QUERY_LIMIT_DEFAULT;

		return offset;
	}

	/**
	 * Retrieves the criterion count from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Count of criterion entries
	 */
	public static int criterionCountFromCriteria(DGCriteria aDGCriteria)
	{
		int dgCriterionCount;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_DG_CRITERION_COUNT)))
			dgCriterionCount = aDGCriteria.getFeatureAsInt(Data.FEATURE_DS_DG_CRITERION_COUNT);
		else
			dgCriterionCount = 0;

		return dgCriterionCount;
	}

	/**
	 * Retrieves the search term(s) from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria search term(s)
	 */
	public static String searchFromCriteria(DSCriteria aDSCriteria)
	{
		String searchTerm;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_SEARCH)))
			searchTerm = aDSCriteria.getFeature(Data.FEATURE_DS_SEARCH);
		else
			searchTerm = StringUtils.EMPTY;

		return searchTerm;
	}

	/**
	 * Retrieves the search term(s) from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria search term(s)
	 */
	public static String searchFromCriteria(DGCriteria aDGCriteria)
	{
		String searchTerm;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_SEARCH)))
			searchTerm = aDGCriteria.getFeature(Data.FEATURE_DS_SEARCH);
		else
			searchTerm = StringUtils.EMPTY;

		return searchTerm;
	}

	/**
	 * Retrieves the suggest term(s) from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria suggest term(s)
	 */
	public static String suggestFromCriteria(DSCriteria aDSCriteria)
	{
		String suggestTerm;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_SUGGEST)))
			suggestTerm = aDSCriteria.getFeature(Data.FEATURE_DS_SUGGEST);
		else
			suggestTerm = StringUtils.EMPTY;

		return suggestTerm;
	}

	/**
	 * Retrieves the suggest term(s) from the DGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria suggest term(s)
	 */
	public static String suggestFromCriteria(DGCriteria aDGCriteria)
	{
		String suggestTerm;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_SUGGEST)))
			suggestTerm = aDGCriteria.getFeature(Data.FEATURE_DS_SUGGEST);
		else
			suggestTerm = StringUtils.EMPTY;

		return suggestTerm;
	}

	/**
	 * Retrieves the highlight flag from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return <i>true</i> if highlighting is enabled, <i>false</i> otherwise
	 */
	public static boolean highlightFromCriteria(DSCriteria aDSCriteria)
	{
		boolean isHighlighted;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_HIGHLIGHT)))
			isHighlighted = StrUtl.stringToBoolean(aDSCriteria.getFeature(Data.FEATURE_DS_HIGHLIGHT));
		else
			isHighlighted = false;

		return isHighlighted;
	}

	/**
	 * Retrieves the phonetic flag from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return <i>true</i> if highlighting is enabled, <i>false</i> otherwise
	 */
	public static boolean phoneticFromCriteria(DSCriteria aDSCriteria)
	{
		boolean isPhonetic;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_PHONETIC)))
			isPhonetic = StrUtl.stringToBoolean(aDSCriteria.getFeature(Data.FEATURE_DS_PHONETIC));
		else
			isPhonetic = false;

		return isPhonetic;
	}

	/**
	 * Retrieves the preferred format from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria action name
	 */
	public static String formatFromCriteria(DSCriteria aDSCriteria)
	{
		String preferredFormat;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_FORMAT)))
			preferredFormat = aDSCriteria.getFeature(Data.FEATURE_DS_FORMAT);
		else
			preferredFormat = StringUtils.EMPTY;

		return preferredFormat;
	}

	/**
	 * Retrieves the fetch policy from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria fetch policy ("virtual" or "paging")
	 */
	public static String fetchPolicyFromCriteria(DSCriteria aDSCriteria)
	{
		String fetchPolicy;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_FETCH_POLICY)))
			fetchPolicy = aDSCriteria.getFeature(Data.FEATURE_DS_FETCH_POLICY);
		else
			fetchPolicy = DS.FETCH_POLICY_VIRTUAL;

		return fetchPolicy;
	}

	/**
	 * Retrieves the fetch policy from the aDGCriteria.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 *
	 * @return Criteria fetch policy ("virtual" or "paging")
	 */
	public static String fetchPolicyFromCriteria(DGCriteria aDGCriteria)
	{
		String fetchPolicy;

		if ((aDGCriteria != null) && (aDGCriteria.isFeatureAssigned(Data.FEATURE_DS_FETCH_POLICY)))
			fetchPolicy = aDGCriteria.getFeature(Data.FEATURE_DS_FETCH_POLICY);
		else
			fetchPolicy = DS.FETCH_POLICY_VIRTUAL;

		return fetchPolicy;
	}

	/**
	 * Retrieves the Redis storage type from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Redis storage type
	 */
	public static String redisStorageTypeFromCriteria(DSCriteria aDSCriteria)
	{
		String redisStorageType;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_REDIS_STORAGE_TYPE)))
			redisStorageType = aDSCriteria.getFeature(Data.FEATURE_DS_REDIS_STORAGE_TYPE);
		else
			redisStorageType = DS.REDIS_STORAGE_TYPE_DEFAULT;

		return redisStorageType;
	}

	/**
	 * Retrieves the facet value count from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria facet value count
	 */
	public static int facetValueCountFromCriteria(DSCriteria aDSCriteria)
	{
		int facetValueCount;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_FACET_VALUE_COUNT)))
			facetValueCount = aDSCriteria.getFeatureAsInt(Data.FEATURE_DS_FACET_VALUE_COUNT);
		else
			facetValueCount = CRITERIA_FACET_VALUE_COUNT_DEFAULT;

		return facetValueCount;
	}

	/**
	 * Retrieves the facet name/value(s) from the DSCriteria.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Criteria facet name/value(s)
	 */
	public static String facetNameValueFromCriteria(DSCriteria aDSCriteria)
	{
		String facetNameValues;

		if ((aDSCriteria != null) && (aDSCriteria.isFeatureAssigned(Data.FEATURE_DS_FACET_NAME_VALUES)))
			facetNameValues = aDSCriteria.getFeature(Data.FEATURE_DS_FACET_NAME_VALUES);
		else
			facetNameValues = StringUtils.EMPTY;

		return facetNameValues;
	}

	/**
	 * Optionally returns the first row of a data grid.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Optional data document instance
	 */
	public static Optional<DataDoc> first(DataGrid aDataGrid)
	{
		DataDoc dataDoc;

		if ((aDataGrid != null) && (aDataGrid.rowCount() > 0))
			dataDoc = aDataGrid.getRowAsDoc(0);
		else
			dataDoc = null;

		return Optional.ofNullable(dataDoc);
	}

	/**
	 * Optionally returns the last row of a data grid.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Optional data document instance
	 */
	public static Optional<DataDoc> last(DataGrid aDataGrid)
	{
		DataDoc dataDoc = null;
		if (aDataGrid != null)
		{
			int rowCount = aDataGrid.rowCount();
			if (rowCount > 0)
				dataDoc = aDataGrid.getRowAsDoc(rowCount-1);
		}

		return Optional.ofNullable(dataDoc);
	}
}
