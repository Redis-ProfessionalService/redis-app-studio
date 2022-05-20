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

package com.redis.ds.ds_redis.search;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.ds.ds_redis.json.RedisJson;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterion;
import com.redis.foundation.ds.DSCriterionEntry;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.aggr.*;

import java.util.Optional;

/**
 * This is a helper class for the RediSearch data source and should not
 * be invoked by applications.  Its purpose is to build and execute
 * queries against the search index.
 */
public class SearchCriteria
{
	private int mFacetId;
	private final AppCtx mAppCtx;
	private final RedisDS mRedisDS;
	private final DataDoc mSearchSchema;
	private final RedisSearch mRedisSearch;
	private final UnifiedJedis mCmdConnection;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the search criteria objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 * @param aRedisSearch Redis search instance
	 */
	public SearchCriteria(RedisDS aRedisDS, RedisSearch aRedisSearch)
	{
		mRedisDS = aRedisDS;
		mRedisSearch = aRedisSearch;
		mAppCtx = aRedisDS.getAppCtx();
		mSearchSchema = mRedisSearch.getSearchSchema();
		mCmdConnection = aRedisSearch.getCmdConnection();
	}

	// https://oss.redislabs.com/redisearch/Query_Syntax/#tag_filters
	// However, this does NOT solve for hyphens and commas in tags: Hyphens in
	// tags: "https://forum.redislabs.com/t/query-with-dash-is-treated-as-negation/119"
	private String getTagValue(DataItem aDataItem)
	{
		char ch;
		String itemValue = aDataItem.getValue();
		if (StringUtils.isNotEmpty(itemValue))
		{
			int strLength = itemValue.length();
			StringBuilder stringBuilder = new StringBuilder();
			for (int i = 0; i < strLength; i++)
			{
				ch = itemValue.charAt(i);
				if (Character.isLetterOrDigit(ch))
					stringBuilder.append(ch);
				else
				{
					stringBuilder.append(StrUtl.CHAR_BACKSLASH);
					stringBuilder.append(ch);
				}
			}

			return stringBuilder.toString();
		}
		else
			return  itemValue;
	}

	private void booleanCriterion(StringBuilder aSB, DSCriterionEntry aCE)
	{
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("@%s:{%s}", dataItem.getName(), mRedisDS.getValue(dataItem)));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("-@%s:{%s}", dataItem.getName(), mRedisDS.getValue(dataItem)));
				break;
		}
	}

	private void dateCriterion(StringBuilder aSB, DSCriterionEntry aCE)
	{
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("@%s:%d", Redis.numericFieldName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("-@%s:%d", Redis.numericFieldName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case GREATER_THAN:
				aSB.append(String.format("@%s:[(%d +inf]", Redis.numericFieldName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case GREATER_THAN_EQUAL:
				aSB.append(String.format("@%s:[%d +inf]", Redis.numericFieldName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case LESS_THAN:
				aSB.append(String.format("@%s:[-inf (%d]", Redis.numericFieldName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case LESS_THAN_EQUAL:
				aSB.append(String.format("@%s:[-inf %d]", Redis.numericFieldName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					long timeInMilliseconds1 = Data.createDate(dataItem.getValues().get(0)).getTime();
					long timeInMilliseconds2 = Data.createDate(dataItem.getValues().get(1)).getTime();
					aSB.append(String.format("@%s:[(%d (%d]", Redis.numericFieldName(dataItem.getName()), timeInMilliseconds1, timeInMilliseconds2));
				}
				break;
			case NOT_BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					long timeInMilliseconds1 = Data.createDate(dataItem.getValues().get(0)).getTime();
					long timeInMilliseconds2 = Data.createDate(dataItem.getValues().get(1)).getTime();
					aSB.append(String.format("-@%s:[%s %s]", Redis.numericFieldName(dataItem.getName()), timeInMilliseconds1, timeInMilliseconds2));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.valueCount() == 2)
				{
					long timeInMilliseconds1 = Data.createDate(dataItem.getValues().get(0)).getTime();
					long timeInMilliseconds2 = Data.createDate(dataItem.getValues().get(1)).getTime();
					aSB.append(String.format("@%s:[%s %s]", Redis.numericFieldName(dataItem.getName()), timeInMilliseconds1, timeInMilliseconds2));
				}
				break;
		}
	}

	private void numberCriterion(StringBuilder aSB, DSCriterionEntry aCE)
	{
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("@%s:[%s %s]", Redis.numericFieldName(dataItem.getName()), dataItem.getValue(), dataItem.getValue()));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("-@%s:[%s %s]", Redis.numericFieldName(dataItem.getName()), dataItem.getValue(), dataItem.getValue()));
				break;
			case GREATER_THAN:
				aSB.append(String.format("@%s:[(%s +inf]", Redis.numericFieldName(dataItem.getName()), dataItem.getValue()));
				break;
			case GREATER_THAN_EQUAL:
				aSB.append(String.format("@%s:[%s +inf]", Redis.numericFieldName(dataItem.getName()), dataItem.getValue()));
				break;
			case LESS_THAN:
				aSB.append(String.format("@%s:[-inf (%s]", Redis.numericFieldName(dataItem.getName()), dataItem.getValue()));
				break;
			case LESS_THAN_EQUAL:
				aSB.append(String.format("@%s:[-inf %s]", Redis.numericFieldName(dataItem.getName()), dataItem.getValue()));
				break;
			case BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					String value1 = dataItem.getValues().get(0);
					String value2 = dataItem.getValues().get(1);
					aSB.append(String.format("@%s:[(%s (%s]", Redis.numericFieldName(dataItem.getName()), value1, value2));
				}
				break;
			case NOT_BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					String value1 = dataItem.getValues().get(0);
					String value2 = dataItem.getValues().get(1);
					aSB.append(String.format("-@%s:[%s %s]", Redis.numericFieldName(dataItem.getName()), value1, value2));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.valueCount() == 2)
				{
					String value1 = dataItem.getValues().get(0);
					String value2 = dataItem.getValues().get(1);
					aSB.append(String.format("@%s:[%s %s]", Redis.numericFieldName(dataItem.getName()), value1, value2));
				}
				break;
		}
	}

	private String textItemName(DataItem aDataItem)
	{
		if ((aDataItem.isFeatureTrue(Redis.FEATURE_IS_TAG_FIELD)) || (aDataItem.isFeatureTrue(Redis.FEATURE_IS_FACET_FIELD)))
			return Redis.tagFieldName(aDataItem.getName());
		else if (aDataItem.isFeatureTrue(Redis.FEATURE_IS_STEMMED))
			return Redis.stemmedFieldName(aDataItem.getName());
		else
			return Redis.textFieldName(aDataItem.getName());
	}

	private void stringSensitiveCriterion(StringBuilder aSB, DataItem aHighlightItem,
										  DataItem aSnippetItem, DSCriterionEntry aCE)
	{
		int valueCount;

		DataItem dataItem = aCE.getItem();
		DSCriterion dsCriterion = aCE.getCriterion();
		boolean isTagField = dataItem.isFeatureTrue(Redis.FEATURE_IS_TAG_FIELD) || (dataItem.isFeatureTrue(Redis.FEATURE_IS_FACET_FIELD));
		if ((dsCriterion.isFeatureTrue(Redis.FEATURE_IS_HIGHLIGHTED)) && (aHighlightItem != null))
			aHighlightItem.addValueUnique(dataItem.getName());
		if ((dsCriterion.isFeatureTrue(Redis.FEATURE_IS_HIGHLIGHTED)) && (aSnippetItem != null))
			aSnippetItem.addValueUnique(dataItem.getName());

		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				if (isTagField)
					aSB.append(String.format("@%s:{%s}", Redis.tagFieldName(dataItem.getName()), getTagValue(dataItem)));
				else
					aSB.append(String.format("@%s:%s", textItemName(dataItem), mRedisDS.getValue(dataItem)));
				break;
			case NOT_EQUAL:
				if (isTagField)
					aSB.append(String.format("-@%s:{%s}", Redis.tagFieldName(dataItem.getName()), getTagValue(dataItem)));
				else
					aSB.append(String.format("-@%s:%s", textItemName(dataItem), mRedisDS.getValue(dataItem)));
				break;
			case STARTS_WITH:
				if (isTagField)
					aSB.append(String.format("@%s:{%s*}", Redis.tagFieldName(dataItem.getName()), getTagValue(dataItem)));
				else
					aSB.append(String.format("@%s:%s*", textItemName(dataItem), mRedisDS.getValue(dataItem)));
				break;
			case IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
					{
						if (isTagField)
							aSB.append(String.format("@%s:{", Redis.tagFieldName(dataItem.getName())));
						else
							aSB.append(String.format("@%s:(", textItemName(dataItem)));
					}
					else
						aSB.append(" | ");
					aSB.append(mRedisDS.escapeValue(ceValue));
					valueCount++;
				}
				if (valueCount > 0)
				{
					if (isTagField)
						aSB.append("}");
					else
						aSB.append(")");
				}
				break;
			case NOT_IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
					{
						if (isTagField)
							aSB.append(String.format("-@%s:{", Redis.tagFieldName(dataItem.getName())));
						else
							aSB.append(String.format("-@%s:(", textItemName(dataItem)));
					}
					else
						aSB.append(" | ");
					aSB.append(mRedisDS.escapeValue(ceValue));
					valueCount++;
				}
				if (valueCount > 0)
				{
					if (isTagField)
						aSB.append("}");
					else
						aSB.append(")");
				}
				break;
			case HIGHLIGHT:
				if (aHighlightItem != null)
					aHighlightItem.addValueUnique(textItemName(dataItem));
				break;
			case SNIPPET:
				if (aSnippetItem != null)
					aSnippetItem.addValueUnique(textItemName(dataItem));
				break;
			case GEO_LOCATION:
				if (dataItem.valueCount() == 4)
				{
					String centerLatitude = dataItem.getValues().get(0);
					String centerLongitude = dataItem.getValues().get(1);
					String centerDistance = dataItem.getValues().get(2);
					String unitDistance = dataItem.getValues().get(3);
					aSB.append(String.format("@%s:[%s,%s %s %s]", dataItem.getName(), centerLongitude, centerLatitude, centerDistance, unitDistance));
				}
				break;
		}
	}

	private int offsetFromCriteria(DSCriteria aDSCriteria)
	{
		String itemName;
		DataItem ceDataItem;

		int queryOffset = 0;
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			if (StringUtils.equals(itemName, Redis.RS_QUERY_OFFSET))
			{
				queryOffset = Math.max(0, Data.createInt(ce.getValue()));
				break;
			}
		}

		return queryOffset;
	}

	private int limitFromCriteria(DSCriteria aDSCriteria)
	{
		String itemName;
		DataItem ceDataItem;

		int queryLimit = Redis.QUERY_LIMIT_DEFAULT;
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			if (StringUtils.equals(itemName, Redis.RS_QUERY_LIMIT))
			{
				queryLimit = Data.createInt(ce.getValue());
				break;
			}
		}

		return queryLimit;
	}

	private boolean isPhoneticFeatureEnabledInSchema()
	{
		for (DataItem schemaItem : mSearchSchema.getItems())
		{
			if ((Data.isText(schemaItem.getType()) && (schemaItem.isFeatureTrue(Redis.FEATURE_IS_PHONETIC))))
				return true;
		}

		return false;
	}

	private int filterItemsInCriteria(DSCriteria aDSCriteria)
	{
		DataItem ceDataItem;

		int filterItemCount = 0;
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			if (! StringUtils.startsWith(ceDataItem.getName(), Redis.RS_PREFIX))
			{
				switch (ce.getLogicalOperator())
				{
					case EQUAL:
					case NOT_EQUAL:
					case STARTS_WITH:
					case IN:
					case NOT_IN:
					case GEO_LOCATION:
					case GREATER_THAN:
					case GREATER_THAN_EQUAL:
					case LESS_THAN:
					case LESS_THAN_EQUAL:
					case BETWEEN:
					case NOT_BETWEEN:
					case BETWEEN_INCLUSIVE:
						filterItemCount++;
						break;
				}
			}
		}

		return filterItemCount;
	}

	/**
	 * Converts a data source criteria instance into a RediSearch query string.
	 * The highlight data item identifies if the RediSearch highlighting feature
	 * should be enabled and the snippet data item describes how large a highlighted
	 * snippet should be.
	 *
	 * @param aDSCriteria Data source criteria instance
	 * @param aHighlightItem Highlighting data item instance
	 * @param aSnippetItem Snippet data item instance
	 *
	 * @return RediSearch query string
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public String criteriaToQueryString(DSCriteria aDSCriteria, DataItem aHighlightItem, DataItem aSnippetItem)
		throws RedisDSException
	{
		String itemName;
		Data.Type dataType;
		StringBuilder stringBuilder;
		DataItem ceDataItem, schemaDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "criteriaToQueryString");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Scan the criterion entries to locate special fields (e.g. query string).

		String queryString = StringUtils.EMPTY;
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			if (StringUtils.startsWith(itemName, Redis.RS_PREFIX))
			{
				if (ceDataItem.getName().equals(Redis.RS_QUERY_STRING))
				{
					queryString = ceDataItem.getValue();
					DSCriterion dsCriterion = ce.getCriterion();
					if ((isPhoneticFeatureEnabledInSchema()) && (dsCriterion.isFeatureAssigned(Redis.FEATURE_IS_PHONETIC)) && (dsCriterion.isFeatureFalse(Redis.FEATURE_IS_PHONETIC)))
						queryString += "=>{$phonetic:false}";
					break;
				}
			}
		}

// Build the remainder of the query criteria as a string.

		int filterItemCount = filterItemsInCriteria(aDSCriteria);
		if ((filterItemCount > 0) && (StringUtils.equals(queryString, Redis.QUERY_ALL_DOCUMENTS)))
			stringBuilder = new StringBuilder();
		else
		{
			stringBuilder = new StringBuilder(queryString);
			stringBuilder.append(StrUtl.CHAR_SPACE);
		}
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			if (StringUtils.startsWith(itemName, Redis.RS_PREFIX))
				continue;
			Optional<DataItem> optSchemaDataItem = mSearchSchema.getItemByNameOptional(itemName);
			if (optSchemaDataItem.isPresent())
			{
				schemaDataItem = optSchemaDataItem.get();
				dataType = schemaDataItem.getType();
				ceDataItem.setType(dataType);
				ceDataItem.copyFeatures(schemaDataItem);
				switch (dataType)
				{
					case Boolean:
						booleanCriterion(stringBuilder, ce);
						break;
					case Integer:
					case Long:
					case Float:
					case Double:
						numberCriterion(stringBuilder, ce);
						break;
					case Date:
					case DateTime:
						dateCriterion(stringBuilder, ce);
						break;
					default:
						stringSensitiveCriterion(stringBuilder, aHighlightItem, aSnippetItem, ce);
						break;
				}
			}
			else
				throw new RedisDSException(String.format("[%s] Unable to match criteria field in search schema.", itemName));
		}

// As a convenience, assign our derived query string to the original criteria object.

		queryString = stringBuilder.toString();
		if (StringUtils.isEmpty(queryString))
		{
			stringBuilder.append(StrUtl.CHAR_SPACE);
			queryString = Redis.QUERY_ALL_DOCUMENTS;
		}
		aDSCriteria.addFeature(Redis.RS_QUERY_STRING, queryString);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return queryString;
	}

	/**
	 * Retrieves the highlighting open tag string.
	 *
	 * @return Highlighting open tag string
	 */
	public String getHighlightOpenTag()
	{
		if ((mSearchSchema != null) && (mSearchSchema.isFeatureAssigned(Redis.FEATURE_HL_TAG_OPEN)))
			return mSearchSchema.getFeature(Redis.FEATURE_HL_TAG_OPEN);
		else
			return Redis.HIGHLIGHT_TAG_OPEN;
	}

	/**
	 * Retrieves the highlighting close tag string.
	 *
	 * @return Highlighting close tag string
	 */
	public String getHighlightCloseTag()
	{
		if ((mSearchSchema != null) && (mSearchSchema.isFeatureAssigned(Redis.FEATURE_HL_TAG_CLOSE)))
			return mSearchSchema.getFeature(Redis.FEATURE_HL_TAG_CLOSE);
		else
			return Redis.HIGHLIGHT_TAG_CLOSE;
	}

	private Query prepare(DSCriteria aDSCriteria, int anOffset, int aLimit, StringBuilder aSB)
		throws RedisDSException
	{
		String itemName;
		Query searchQuery;
		DataItem ceDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "prepare");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

// Construct the RediSearch query string based on the criteria.

		DataItem snippetItem = new DataItem.Builder().name("snippet_fields").build();
		DataItem highlightItem = new DataItem.Builder().name("highlight_fields").build();
		String queryString = criteriaToQueryString(aDSCriteria, highlightItem, snippetItem);
		if (StringUtils.isNotEmpty(queryString))
		{
			String patternString = String.format("%s ", Redis.QUERY_ALL_DOCUMENTS);
			if ((StringUtils.startsWith(queryString, patternString)) || (StringUtils.endsWith(queryString, " ")))
				queryString = StringUtils.trim(queryString);
		}
		searchQuery = new Query(queryString);
		if (! StringUtils.endsWith(aSB.toString(), " "))
			aSB.append(String.format(" %s", mRedisDS.escapeValue(queryString)));
		else
			aSB.append(mRedisDS.escapeValue(queryString));

// Apply snippet summaries if it was specified.

		if (snippetItem.isValueNotEmpty())
		{
			aSB.append(String.format(" SUMMARIZE FIELDS %d", snippetItem.valueCount()));
			for (String fieldName : snippetItem.getValues())
				aSB.append(String.format(" %s", mRedisDS.escapeKey(fieldName)));
			searchQuery.highlightFields(snippetItem.getValuesArray());
		}

// Apply highlighting if it was specified.

		if (highlightItem.isValueNotEmpty())
		{
			aSB.append(String.format(" HIGHLIGHT FIELDS %d", highlightItem.valueCount()));
			for (String fieldName : highlightItem.getValues())
				aSB.append(String.format(" %s", mRedisDS.escapeKey(fieldName)));
			String hlOpenTag = getHighlightOpenTag();
			String hlCloseTag = getHighlightCloseTag();
			if ((StringUtils.isNotEmpty(hlOpenTag)) && (StringUtils.isNotEmpty(hlCloseTag)))
			{
				Query.HighlightTags qhTags = new Query.HighlightTags(hlOpenTag, hlCloseTag);
				searchQuery.highlightFields(qhTags, highlightItem.getValuesArray());

				aSB.append(String.format(" TAGS \"%s\" \"%s\"", hlOpenTag, hlCloseTag));
			}
			else
				searchQuery.highlightFields(highlightItem.getValuesArray());
		}

// Apply sorting if it was specified in the criteria.

		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			if (ce.getLogicalOperator() == Data.Operator.SORT)
			{
				ceDataItem = ce.getItem();
				Data.Order sortOrder = Data.Order.valueOf(ceDataItem.getValue());
				if (Data.isDateOrTime(ceDataItem.getType()))
					itemName = Redis.numericFieldName(ceDataItem.getName());
				else if (Data.isNumber(ceDataItem.getType()))
					itemName = Redis.numericFieldName(ceDataItem.getName());
				else if (Data.isText(ceDataItem.getType()))
					itemName = Redis.tagFieldName(ceDataItem.getName());
				else
					itemName = ceDataItem.getName();
				searchQuery.setSortBy(itemName, sortOrder == Data.Order.ASCENDING);
				if (sortOrder == Data.Order.ASCENDING)
					aSB.append(String.format(" SORTBY %s ASC", mRedisDS.escapeKey(itemName)));
				else
					aSB.append(String.format(" SORTBY %s DESC", mRedisDS.escapeKey(itemName)));
			}
		}

// Apply our offset and limit.

		aSB.append(String.format(" LIMIT %d %d", anOffset, aLimit));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return searchQuery.limit(anOffset, aLimit);
	}

	private DataGrid hashResultToDataGrid(SearchResult aSearchResult)
	{
		Logger appLogger = mAppCtx.getLogger(this, "hashResultToDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = new DataGrid(mSearchSchema);
		for (Document rsd : aSearchResult.getDocuments())
		{
			dataGrid.newRow();
			rsd.getProperties().forEach(e -> {
				String itemName = e.getKey();
				String itemValue = StringUtils.EMPTY;
				Object itemObject = e.getValue();
				if (itemObject instanceof String)
					itemValue = (String) e.getValue();
				if (itemObject instanceof Integer)
				{
					Integer iValue = (Integer) itemObject;
					itemValue = iValue.toString();
				}
				else if (itemObject instanceof Long)
				{
					Long lValue = (Long) itemObject;
					itemValue = lValue.toString();
				}
				else if (itemObject instanceof Float)
				{
					Float fValue = (Float) itemObject;
					itemValue = fValue.toString();
				}
				else if (itemObject instanceof Double)
				{
					Double dValue = (Double) itemObject;
					itemValue = dValue.toString();
				}
				dataGrid.setValueByName(itemName, itemValue);
			});
			dataGrid.addRow();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	private DataGrid jsonResultToDataGrid(SearchResult aSearchResult)
	{
		DataDoc dataDoc;
		JSONObject jsonObject;
		Optional<DataDoc> optDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "jsonResultToDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int resultOffset = 0;
		RedisJson redisJson = mRedisDS.createJson();
		DataGrid dataGrid = new DataGrid(mSearchSchema);
		for (Document rsd : aSearchResult.getDocuments())
		{
			jsonObject = new JSONObject((String) rsd.get(Redis.JSON_PATH_ROOT));
			optDataDoc = redisJson.jsonObjectToDataDoc(jsonObject);
			if (optDataDoc.isPresent())
			{
				dataDoc = optDataDoc.get();
				dataGrid.addRow(dataDoc);
			}
			else
				appLogger.error(String.format("[%d Offset]: Unable to convert JSON search result to data document instance.", resultOffset));
			resultOffset++;
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Converts a RediSearch search result instance into a data grid instance.
	 * This methond handles both JSON and Hash document types.
	 *
	 * @param aSearchResult RediSearch result instance
	 *
	 * @return Data grid instance
	 */
	public DataGrid searchResultToDataGrid(SearchResult aSearchResult)
	{
		DataGrid dataGrid;
		Logger appLogger = mAppCtx.getLogger(this, "execute");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mRedisSearch.mDocument == Redis.Document.JSON)
			dataGrid = jsonResultToDataGrid(aSearchResult);
		else
			dataGrid = hashResultToDataGrid(aSearchResult);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Executes a RediSearch query based on the data source criteria instance
	 * using the page offset and total document limits.
	 *
	 * @param aDSCriteria Data source criteria instance
	 * @param anOffset Starting page offset
	 * @param aLimit Total documents returned limit
	 *
	 * @return Data grid instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGrid execute(DSCriteria aDSCriteria, int anOffset, int aLimit)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "execute");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDSCriteria == null)
			throw new RedisDSException("Cannot execute - criteria was not prepared.");

// Create our RediSearch command string.

		String indexKeyName = mRedisDS.createKey().moduleSearch().redisSearchIndex().dataName(mRedisSearch.getIndexName()).name();
		StringBuilder stringBuilder = new StringBuilder(String.format("FT.SEARCH %s", mRedisDS.escapeKey(indexKeyName)));

// Prepare, execute query and transform document results

		Query searchQuery = prepare(aDSCriteria, anOffset, aLimit, stringBuilder);
		SearchResult searchResult = mCmdConnection.ftSearch(indexKeyName, searchQuery);
		DataGrid dataGrid = searchResultToDataGrid(searchResult);
		mRedisDS.saveCommand(appLogger, stringBuilder.toString());

// Assign result set summary features

		dataGrid.addFeature(DS.FEATURE_CUR_LIMIT, aLimit);
		dataGrid.addFeature(DS.FEATURE_CUR_OFFSET, anOffset);
		long nextOffset = anOffset + aLimit;
		dataGrid.addFeature(DS.FEATURE_NEXT_OFFSET, Math.min(searchResult.getTotalResults(), nextOffset));
		dataGrid.addFeature(DS.FEATURE_TOTAL_DOCUMENTS, searchResult.getTotalResults());

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Executes a RediSearch query based on the data source criteria instance.
	 *
	 * @param aDSCriteria Data source criteria instance
	 *
	 * @return Data grid instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGrid execute(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "execute");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDSCriteria == null)
			throw new RedisDSException("Cannot execute - criteria is null.");

// Scan the criterion entries to locate offset and limits.

		int queryOffset = offsetFromCriteria(aDSCriteria);
		int queryLimit = limitFromCriteria(aDSCriteria);
		DataGrid dataGrid = execute(aDSCriteria, queryOffset, queryLimit);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Convenience mthod that creates a facet aggregation data grid.
	 *
	 * @return Data grid instance
	 */
	public DataGrid createAggregateResultGrid()
	{
		DataGrid dataGrid = new DataGrid("RediSearch Aggregate Result Grid");
		dataGrid.addCol(new DataItem.Builder().name("field_name").title("Field Name").build());
		dataGrid.addCol(new DataItem.Builder().name("field_title").title("Field Title").build());
		dataGrid.addCol(new DataItem.Builder().name("facet_name_count").title("Facet Name & Count").build());

		return dataGrid;
	}

	private void addResultToDataGrid(DataGrid aDataGrid, AggregationResult aResult,
									 String anItemName, String anItemNameFC)
	{
		Row aggRow;
		long facetCount;
		String fieldName;
		Logger appLogger = mAppCtx.getLogger(this, "addResultToDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataItem schemaDataItem = mRedisSearch.getSearchSchema().getItemByName(anItemName);
		if ((schemaDataItem != null) && (aResult.totalResults > 0))
		{
			int nullCount = 0;
			String tagItemName = Redis.tagFieldName(anItemName);
			DataItem dataItem = new DataItem(schemaDataItem);
			for (int row = 0; row < aResult.totalResults; row++)
			{
				aggRow = aResult.getRow(row);
				if (aggRow != null)
				{
					if ((aggRow.containsKey(tagItemName)) && (aggRow.containsKey(anItemNameFC)))
					{
						try
						{
							fieldName = aggRow.getString(tagItemName);
						}
						catch (Exception e)
						{
							fieldName = "Unassigned";
						}
						try
						{
							facetCount = aggRow.getLong(anItemNameFC);
						}
						catch (Exception e)
						{
							facetCount = 0L;
						}
						if (facetCount > 0L)
							dataItem.addValue(String.format("%s (%d)", fieldName, facetCount));
					}
				}
				else
					nullCount++;
			}
			if (nullCount > 0)
				appLogger.warn(String.format("aResult.getRow(row) returned a null value %d/%d times in the result set.", nullCount, aResult.totalResults));
			aDataGrid.newRow();
			aDataGrid.setValueByName("field_name", dataItem.getName());
			aDataGrid.setValueByName("field_title", dataItem.getTitle());
			aDataGrid.setValuesByName("facet_name_count", dataItem.getValues());
			aDataGrid.addRow();
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Convenience mthod that creates a facet aggregation data grid for a
	 * UI presentation.
	 *
	 * @return Data grid instance
	 */
	public DataGrid createAggregateUIResultGrid()
	{
		DataGrid facetGrid = new DataGrid("RediSearch Aggregate UI Result Grid");
		facetGrid.addCol(new DataItem.Builder().type(Data.Type.Integer).name("id").title("id").isHidden(true).build());
		facetGrid.addCol(new DataItem.Builder().type(Data.Type.Integer).name("parent_id").title("Parent Id").isHidden(true).build());
		facetGrid.addCol(new DataItem.Builder().name("field_name").title("Field Name").isHidden(true).build());
		facetGrid.addCol(new DataItem.Builder().name("facet_name").title("Facet Name").build());

		mFacetId++;
		facetGrid.newRow();
		facetGrid.setValueByName("id", mFacetId++);
		facetGrid.setValueByName("parent_id", 0);
		facetGrid.setValueByName("field_name", "facet_field");
		facetGrid.setValueByName("facet_name", "Facet List");
		facetGrid.addRow();

		return facetGrid;
	}

	private void addResultToUIDataGrid(DataGrid aDataGrid, AggregationResult aResult,
									   String anItemName, String anItemNameFC)
	{
		Row aggRow;
		long facetCount;
		String fieldName;
		Logger appLogger = mAppCtx.getLogger(this, "addResultToUIDataGrid");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataItem schemaDataItem = mRedisSearch.getSearchSchema().getItemByName(anItemName);
		if ((schemaDataItem != null) && (aResult.totalResults > 0))
		{
			int nullCount = 0;
			DataItem dataItem = new DataItem(schemaDataItem);

			int facetParentId = mFacetId;
			String tagItemName = Redis.tagFieldName(anItemName);

			aDataGrid.newRow();
			aDataGrid.setValueByName("id", mFacetId++);
			aDataGrid.setValueByName("parent_id", 1);
			aDataGrid.setValueByName("field_name", dataItem.getName());
			aDataGrid.setValueByName("facet_name", dataItem.getTitle());
			aDataGrid.addRow();

			for (int row = 0; row < aResult.totalResults; row++)
			{
				aggRow = aResult.getRow(row);
				if (aggRow != null)
				{
					if ((aggRow.containsKey(tagItemName)) && (aggRow.containsKey(anItemNameFC)))
					{
						try
						{
							fieldName = aggRow.getString(tagItemName);
						}
						catch (Exception e)
						{
							fieldName = "Unassigned";
						}
						try
						{
							facetCount = aggRow.getLong(anItemNameFC);
						}
						catch (Exception e)
						{
							facetCount = 0L;
						}
						if (facetCount > 0L)
						{
							aDataGrid.newRow();
							aDataGrid.setValueByName("id", mFacetId++);
							aDataGrid.setValueByName("parent_id", facetParentId);
							aDataGrid.setValueByName("field_name", dataItem.getName());
							aDataGrid.setValueByName("facet_name", String.format("%s (%d)", fieldName, facetCount));
							aDataGrid.addRow();
						}
					}
				}
				else
					nullCount++;
			}
			if (nullCount > 0)
				appLogger.warn(String.format("aResult.getRow(row) returned a null value %d/%d times in the result set.", nullCount, aResult.totalResults));
		}
		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Executes a RediSearch aggregation query based on the data source criteria
	 * instance.  If the output is targeted for a UI presentation, then the data
	 * grid will be formatted accordingly.
	 *
	 * @param aDSCriteria Data source criteria instance
	 * @param aFormatForUIPresentation UI presentation flag
	 *
	 * @return Data grid instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGrid aggregate(DSCriteria aDSCriteria, boolean aFormatForUIPresentation)
		throws RedisDSException
	{
		DataGrid facetGrid;
		StringBuilder stringBuilder;
		DataItem primaryItem, ceDataItem;
		AggregationResult aggregationResult;
		AggregationBuilder aggregationBuilder;
		String itemName, itemNameFC, primaryItemName, tagItemName;
		Logger appLogger = mAppCtx.getLogger(this, "aggregate");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDSCriteria == null)
			throw new RedisDSException("Cannot aggregate - criteria is null.");

// Ensure we have a primary item identified.

		Optional<DataItem> optDataItem = mRedisSearch.getSearchSchema().getPrimaryKeyItemOptional();
		if (optDataItem.isPresent())
		{
			primaryItem = optDataItem.get();
			if ((Data.isNumber(primaryItem.getType())) || (Data.isDateOrTime(primaryItem.getType())))
				primaryItemName = Redis.numericFieldName(primaryItem.getName());
			else
				primaryItemName = Redis.tagFieldName(primaryItem.getName());
		}
		else
			throw new RedisDSException("Cannot aggregate - schema is missing a primary item.");

// Scan the criterion entries to locate offset and limits.

		int queryOffset = offsetFromCriteria(aDSCriteria);
		int queryLimit = limitFromCriteria(aDSCriteria);

// Create our RediSearch command string.

		String indexKeyName = mRedisDS.createKey().moduleSearch().redisSearchIndex().dataName(mRedisSearch.getIndexName()).name();
		String aggregateCommand = String.format("FT.AGGREGATE %s ", mRedisDS.escapeKey(indexKeyName));

// Prepare, execute query and transform document results

		mFacetId = 0;
		if (aFormatForUIPresentation)
			facetGrid = createAggregateUIResultGrid();
		else
			facetGrid = createAggregateResultGrid();
		String queryString = criteriaToQueryString(aDSCriteria, null, null);
		if (StringUtils.isNotEmpty(queryString))
		{
			String patternString = String.format("%s ", Redis.QUERY_ALL_DOCUMENTS);
			if (StringUtils.startsWith(queryString, patternString))
				queryString = StringUtils.trim(queryString);
		}
		for (DSCriterionEntry ce : aDSCriteria.getCriterionEntries())
		{
			ceDataItem = ce.getItem();
			itemName = ceDataItem.getName();
			if (StringUtils.startsWith(itemName, Redis.RS_PREFIX))
				continue;

			if ((ce.getLogicalOperator() == Data.Operator.FACET) && (Data.isText(ceDataItem.getType())))
			{
				if (ceDataItem.isValueTrue())
				{
					tagItemName = Redis.tagFieldName(itemName);
					stringBuilder = new StringBuilder(aggregateCommand);
					stringBuilder.append(StrUtl.CHAR_DBLQUOTE);
					stringBuilder.append(queryString);
					stringBuilder.append(StrUtl.CHAR_DBLQUOTE);
					itemNameFC = String.format("%s_fc", itemName);
					aggregationBuilder = new AggregationBuilder(queryString)
											.groupBy(String.format("@%s", tagItemName), Reducers.count_distinctish(String.format("@%s", primaryItemName)).as(itemNameFC))
											.sortBy(SortedField.desc(String.format("@%s", itemNameFC)))
											.limit(queryOffset, queryLimit);
					stringBuilder.append(String.format(" GROUPBY 1 @%s REDUCE COUNT_DISTINCT 1 @%s as %s SORTBY 2 @%s DESC LIMIT %d %d",
													   tagItemName, primaryItemName, itemNameFC, itemNameFC, queryOffset, queryLimit));
					aggregationResult = mCmdConnection.ftAggregate(indexKeyName, aggregationBuilder);
					if (aFormatForUIPresentation)
						addResultToUIDataGrid(facetGrid, aggregationResult, itemName, itemNameFC);
					else
						addResultToDataGrid(facetGrid, aggregationResult, itemName, itemNameFC);
					if (mFacetId > 0)
						mRedisDS.saveCommand(appLogger, stringBuilder.toString());
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return facetGrid;
	}
}
