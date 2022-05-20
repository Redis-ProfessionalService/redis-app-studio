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

package com.redis.ds.ds_redis;

import com.redis.foundation.data.*;
import com.redis.foundation.ds.DS;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The Redis class is responsible for centralizing the constant
 * values used throughout the library and providing utility
 * methods.
 *
 * @since 1.0
 * @author Al Cole
 */
public class Redis
{
	public static final String CFG_PROPERTY_PREFIX = "redis";		// configuration property prefix

// General constants and default values for the Redis data source

	public static final String RESPONSE_OK = "OK";
	public static final int PIPELINE_BATCH_COUNT = 25;
	public static final String APPLICATION_PREFIX_DEFAULT = "ASRC";
	public static final String SHADOW_FIELD_MARKER_NAME = "_shadow";
	public static final String ENCRYPTION_SECRET_DEFAULT = "1c518a1e-redis-4ff0-8478-f319b887dca0";

// Redis connection parameters

	public static final int DBID_DEFAULT = 0;
	public static final int TIMEOUT_DEFAULT = 4000;							// Milliseconds
	public static final int PORT_NUMBER_DEFAULT = 6379;
	public static final int POOL_MAX_IDLE_CONNECTIONS = 8;
	public static final int POOL_MIN_IDLE_CONNECTIONS = 0;
	public static final int POOL_MAX_TOTAL_CONNECTIONS = 8;
	public static final String HOST_NAME_DEFAULT = "localhost";

// Redis data source type encoding constants

	public static final String DS_TYPE_GEO_NAME = "GE";
	public static final String DS_TYPE_SET_NAME = "SE";
	public static final String DS_TYPE_LIST_NAME = "LI";
	public static final String DS_TYPE_HASH_NAME = "HA";
	public static final String DS_TYPE_STRING_NAME = "SG";
	public static final String DS_TYPE_STREAM_NAME = "SM";
	public static final String DS_TYPE_JSON_DOC_NAME = "JD";
	public static final String DS_TYPE_HYPER_LOG_NAME = "HL";
	public static final String DS_TYPE_SORTED_SET_NAME = "SS";
	public static final String DS_TYPE_UNDEFINED = "UD";

// Redis grid related constants

	public static final long GRID_RANGE_SCHEMA = 0L;
	public static final long GRID_RANGE_START = 1L;
	public static final long GRID_RANGE_FINISH = -1L;

// Redis stream related constants

	public static final int STREAM_BATCH_COUNT = 100;
	public static final long STREAM_LIMIT_DEFAULT = 5000;
	public static final String STREAM_START_DEFAULT = "+";					// assumes descending (xrevrange)
	public static final String STREAM_FINISH_DEFAULT = "-";					// assumes descending (xrevrange)
	public static final String STREAM_COMMAND_DATA_NAME = "CommandStream";

// Redis lock related constants

	public static final long LOCK_RELEASE_TIMEOUT_DEFAULT = 2000;			// 2 seconds
	public static final long LOCK_WAITFOR_TIMEOUT_DEFAULT = 3000;			// 3 seconds - always should be greater than release

// Redis core feature related constants

	public static final String FEATURE_IS_KEY = "isKey";
	public static final String FEATURE_KEY_NAME = "redis_key_name";
	public static final String FEATURE_DS_TYPE_NAME = "redis_ds_type_name";	// data structure type name
	public static final String FEATURE_LOCK_RELEASE = "redis_lock_release";
	public static final String FEATURE_LOCK_WAITFOR = "redis_lock_wait_for";

// Redis key prefix constants

	public static final String KEY_MODULE_CORE = "RC";
	public static final String KEY_MODULE_JSON = "RJ";
	public static final String KEY_MODULE_GRAPH = "RG";
	public static final String KEY_MODULE_SEARCH = "RS";
	public static final String KEY_MODULE_TIME_SERIES = "RT";

	public static final String KEY_ID_METHOD_NAME = "MN";
	public static final String KEY_ID_METHOD_HASH = "MH";
	public static final String KEY_ID_METHOD_RANDOM = "MR";
	public static final String KEY_ID_METHOD_PRIMARY = "MP";

	public static final String KEY_REDIS_TYPE_SET = "SE";
	public static final String KEY_REDIS_TYPE_HASH = "HA";
	public static final String KEY_REDIS_TYPE_STRING = "SG";
	public static final String KEY_REDIS_TYPE_STREAM = "SM";
	public static final String KEY_REDIS_TYPE_HYPER_LOG = "HL";
	public static final String KEY_REDIS_TYPE_SORTED_SET = "SS";
	public static final String KEY_REDIS_TYPE_TIME_SERIES = "TS";
	public static final String KEY_REDIS_TYPE_SEARCH_INDEX = "SI";
	public static final String KEY_REDIS_TYPE_SEARCH_SCHEMA = "SC";
	public static final String KEY_REDIS_TYPE_SEARCH_SUGGEST = "SU";
	public static final String KEY_REDIS_TYPE_SEARCH_SYNONYM = "SY";
	public static final String KEY_REDIS_TYPE_JSON_SCHEMA = "JS";
	public static final String KEY_REDIS_TYPE_JSON_DOCUMENT = "JD";
	public static final String KEY_REDIS_TYPE_GRAPH_SCHEMA = "GS";
	public static final String KEY_REDIS_TYPE_GRAPH_PROPERTY = "GP";

	public static final String KEY_DATA_TYPE_KEY = "KE";
	public static final String KEY_DATA_TYPE_DATE = "DA";
	public static final String KEY_DATA_TYPE_LONG = "LO";
	public static final String KEY_DATA_TYPE_TEXT = "TE";
	public static final String KEY_DATA_TYPE_FLOAT = "FL";
	public static final String KEY_DATA_TYPE_DOUBLE = "DO";
	public static final String KEY_DATA_TYPE_BOOLEAN = "BO";
	public static final String KEY_DATA_TYPE_INTEGER = "IN";
	public static final String KEY_DATA_TYPE_DATETIME = "DT";

	public static final String KEY_VALUE_MULTI = "M";
	public static final String KEY_VALUE_SINGLE = "S";
	public static final String KEY_VALUE_PLAIN = "P";
	public static final String KEY_VALUE_ENCRYPTED = "E";

	public static final String KEY_DATA_OBJECT_ITEM = "DI";
	public static final String KEY_DATA_OBJECT_GRID = "DT";
	public static final String KEY_DATA_OBJECT_GRAPH = "DG";
	public static final String KEY_DATA_OBJECT_DOCUMENT = "DD";

// RediSearch general and feature related constants

	public static final int QUERY_LIMIT_DEFAULT = 10;
	public static final String QUERY_ALL_DOCUMENTS = "*";
	public static final double FIELD_WEIGHT_DEFAULT = 0.5;
	public static final double SEARCH_SCORE_DEFAULT = 1.0;
	public static final int SUGGESTION_LIMIT_DEFAULT = 5;
	public static final int SUGGESTION_MIN_TOKEN_SIZE = 5;
	public static final int SCHEMA_MAXIMUM_ALL_FIELDS = 1024;
	public static final int SCHEMA_MAXIMUM_TEXT_FIELDS = 128;

	public static final String RS_PREFIX = "rs_";
	public static final String RS_DOCUMENT_ID = RS_PREFIX + "doc_id";
	public static final String RS_QUERY_LIMIT = RS_PREFIX + DS.CRITERIA_QUERY_LIMIT;
	public static final String RS_QUERY_OFFSET = RS_PREFIX + DS.CRITERIA_QUERY_OFFSET;
	public static final String RS_QUERY_STRING = RS_PREFIX + DS.CRITERIA_QUERY_STRING;

	public static final String FEATURE_NO_FIELDS = "indexNoFields";
	public static final String FEATURE_NO_STOP_WORDS = "indexNoStopWords";
	public static final String FEATURE_INDEX_EXPIRATION = "indexExpiration";
	public static final String FEATURE_NO_TERM_OFFSETS = "indexNoTermOffsets";
	public static final String FEATURE_NO_TERM_FREQUENCIES = "indexNoTermFrequencies";

	public static final String FEATURE_SCORE = "searchScore";
	public static final String FEATURE_WEIGHT = "fieldWeight";
	public static final String FEATURE_GROUP_ID = "groupId";
	public static final String FEATURE_IS_TAG_FIELD = "isTag";
	public static final String FEATURE_IS_GEO_FIELD = "isGeo";
	public static final String FEATURE_IS_FACET_FIELD = "isFacet";
	public static final String FEATURE_INDEX_NAME = "indexName";
	public static final String FEATURE_IS_STEMMED = "isStemmed";
	public static final String FEATURE_STOP_WORDS = "stopWords";
	public static final String FEATURE_IS_PHONETIC = "isPhonetic";
	public static final String FEATURE_IS_HIGHLIGHTED = "isHighlighted";

	public static final String FEATURE_HL_TAG_OPEN = "highlightTagOpen";
	public static final String FEATURE_HL_TAG_CLOSE = "highlightTagClose";
	public static final String FEATURE_PHONETIC_ENGLISH = "dm:en";
	public static final String FEATURE_PHONETIC_MATCHER = "phoneticMatcher";
	public static final String FEATURE_TAG_FIELD_SEPARATOR = "tagFieldSeparator";

// RediSearch highlight component defaults.

	public static final String HIGHLIGHT_TAG_OPEN = "<b>";
	public static final String HIGHLIGHT_TAG_CLOSE = "</b>";

// RedisJSON general and feature related constants

	public static final String JSON_PATH_ROOT = "$";

// RedisGraph general and feature related constants

	public static final String RG_PREFIX = "rg_";
	public static final String RG_QUERY_LIMIT = RG_PREFIX + DS.CRITERIA_QUERY_LIMIT;
	public static final String RG_QUERY_OFFSET = RG_PREFIX + DS.CRITERIA_QUERY_OFFSET;

// RedisGraph result schema name identifiers.

	public static final String RESULT_NODE_SCHEMA = "n";
	public static final String RESULT_GRAPH_PATH_SCHEMA = "gp";
	public static final String RESULT_RELATIONSHIP_SCHEMA = "r";

// Data item RedisGraph feature constants.

	public static final String FEATURE_CYPHER_QUERY = "cypherQuery";
	public static final String FEATURE_REDISGRAPH_QUERY = "redisGraphQuery";
	public static final String FEATURE_IS_FULLTEXT_SEARCH = "isFullTextSearch";
	public static final String FEATURE_IS_PROPERTY_SEARCH = "isPropertySearch";

// RedisTimeSeries feature and item constants.

	public static final String RT_PREFIX = "rt_";
	public static final String RT_CHILD_LABELS = RT_PREFIX + "labels";

	public static final String FEATURE_SORT_ORDER = "sortOrder";
	public static final String FEATURE_IS_VALUE = "isSampleValue";
	public static final String FEATURE_TIME_BUCKET = "timeBucket";
	public static final String FEATURE_IS_TIMESTAMP = "isSampleTS";
	public static final String FEATURE_SAMPLE_COUNT = "sampleCount";
	public static final String FEATURE_FUNCTION_NAME = "functionName";
	public static final String FEATURE_RETENTION_TIME = "retentionTime";		// https://oss.redislabs.com/redistimeseries/configuration/#retention_policy
	public static final String FEATURE_IS_UNCOMPRESSED = "isUncompressed";
	public static final String FEATURE_START_TIMESTAMP = "startTimestamp";
	public static final String FEATURE_FINISH_TIMESTAMP = "finishTimestamp";
	public static final String FEATURE_MEMORY_CHUNK_SIZE = "memoryChunkSize";

	/**
	 * RedisCore encryption options
	 */
	public enum Encryption { None, Field, All }

	/**
	 * RedisCore geo units
	 */
	public enum DistanceUnit { Meter, Kilometer, Mile, Feet }

	/**
	 * RedisSearch document storage options
	 */
	public enum Document { Hash, JSON }

	/**
	 * RedisTimeSeries functions
	 */
	public enum Function
	{
		AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD_P, STD_S, VAR_P, VAR_S
	}

	private void Redis()
	{
	}

	/**
	 * Return <i>true</i> if the response message string is OK.
	 *
	 * @param aMsgResponse Response message string
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isResponseOK(String aMsgResponse)
	{
		return StringUtils.equals(aMsgResponse, Redis.RESPONSE_OK);
	}

	/**
	 * Identifies if the feature name has been enabled <i>true</i> and if it
	 * has not, will return the default value.
	 *
	 * @param aDataItem Data items instance
	 * @param aFeatureName Feature name
	 * @param aDefault Default value
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isFeatureTrue(DataItem aDataItem, String aFeatureName, boolean aDefault)
	{
		if ((aDataItem != null) && (aDataItem.isFeatureAssigned(aFeatureName)))
			return aDataItem.isFeatureTrue(aFeatureName);
		else
			return aDefault;
	}

	/**
	 * Identifies if the feature name has been disabled <i>false</i> and if it
	 * has not, will return the default value.
	 *
	 * @param aDataItem Data items instance
	 * @param aFeatureName Feature name
	 * @param aDefault Default value
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isFeatureFalse(DataItem aDataItem, String aFeatureName, boolean aDefault)
	{
		if ((aDataItem != null) && (aDataItem.isFeatureAssigned(aFeatureName)))
			return aDataItem.isFeatureFalse(aFeatureName);
		else
			return aDefault;
	}

	/**
	 * Returns the search score based on feature assignment or default value.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Search score value
	 */
	public static double getScore(DataItem aDataItem)
	{
		double searchScore = SEARCH_SCORE_DEFAULT;
		if ((aDataItem != null) && (aDataItem.isFeatureAssigned(FEATURE_SCORE)))
			searchScore = aDataItem.getFeatureAsDouble(FEATURE_SCORE);

		return searchScore;
	}

	/**
	 * Returns the search score based on feature assignment or default value.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return Search score value
	 */
	public static double getScore(DataDoc aDataDoc)
	{
		double searchScore = SEARCH_SCORE_DEFAULT;
		if ((aDataDoc != null) && (aDataDoc.isFeatureAssigned(FEATURE_SCORE)))
			searchScore = aDataDoc.getFeatureAsDouble(FEATURE_SCORE);

		return searchScore;
	}

	/**
	 * Creates a shadow field name for the search schema.
	 *
	 * @param aName Item name
	 *
	 * @return Shadow field name
	 */
	public static String shadowFieldName(String aName)
	{
		return String.format("%s_%s", aName, SHADOW_FIELD_MARKER_NAME);
	}

	/**
	 * Returns the schema field name based on data item features.
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Schema field name
	 */
	public static String schemaFieldName(DataItem aDataItem)
	{
		if (aDataItem.isFeatureAssigned(Data.FEATURE_JSON_PATH))
			return aDataItem.getFeature(Data.FEATURE_JSON_PATH);
		else
			return aDataItem.getName();
	}

	/**
	 * Creates a text field name for the search schema (non-stemmed).
	 *
	 * @param aName Item name
	 *
	 * @return Text field name
	 */
	public static String textFieldName(String aName)
	{
		return String.format("%s_text", aName);
	}

	/**
	 * Creates a stemmed text field name for the search schema.
	 *
	 * @param aName Item name
	 *
	 * @return Text field name
	 */
	public static String stemmedFieldName(String aName)
	{
		return String.format("%s_stemmed", aName);
	}

	/**
	 * Creates a phonetic text field name for the search schema.
	 *
	 * @param aName Item name
	 *
	 * @return Phonetic field name
	 */
	public static String phoneticFieldName(String aName)
	{
		return String.format("%s_phonetic", aName);
	}

	/**
	 * Creates a tag field name for the search schema.
	 *
	 * @param aName Item name
	 *
	 * @return Tag field name
	 */
	public static String tagFieldName(String aName)
	{
		return String.format("%s_tag", aName);
	}

	/**
	 * Creates a numeric field name for the search schema.
	 *
	 * @param aName Item name
	 *
	 * @return Numeric field name
	 */
	public static String numericFieldName(String aName)
	{
		return String.format("%s_number", aName);
	}

	/**
	 * Converts a search field name to a data document item name by
	 * stripping the schema type from the end of the string.
	 *
	 * @param aFieldName Search field name
	 *
	 * @return Item name or original field name
	 */
	public static String searchFieldToItemName(String aFieldName)
	{
		int offset = StringUtils.lastIndexOf(aFieldName, StrUtl.CHAR_UNDERLINE);
		if (offset > 0)
		{
			String schemaType = aFieldName.substring(offset+1);
			switch (schemaType)
			{
				case "tag":
				case "text":
				case "number":
				case "stemmed":
				case "phonetic":
					return aFieldName.substring(0, offset);
			}
		}

		return aFieldName;
	}

	/**
	 * Loads a simple text file into a string array - utility method
	 * available in case you need it.
	 *
	 * @param aPathFileName Path/file name to load.
	 *
	 * @return The text file collapsed into an array of strings.
	 *
	 * @throws IOException I/O error condition triggered.
	 */
	static public ArrayList<String> loadFileList(String aPathFileName)
		throws IOException
	{
		List<String> lineList;

		try (FileReader fileReader = new FileReader(aPathFileName))
		{
			lineList = IOUtils.readLines(fileReader);
		}

		ArrayList<String> fileList = new ArrayList<>();

		for (String followString : lineList)
		{
			if (! StringUtils.startsWith(followString, "#"))
				fileList.add(followString);
		}
		return fileList;
	}
}
