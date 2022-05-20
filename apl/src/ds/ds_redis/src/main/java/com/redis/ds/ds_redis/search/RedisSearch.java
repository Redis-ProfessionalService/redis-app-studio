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
import com.redis.ds.ds_redis.core.RedisCore;
import com.redis.ds.ds_redis.core.RedisDoc;
import com.redis.ds.ds_redis.core.RedisGrid;
import com.redis.ds.ds_redis.json.RedisJson;
import com.redis.ds.ds_redis.shared.RedisKey;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.CfgMgr;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.io.DataDocXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.xml.sax.SAXException;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.FieldName;
import redis.clients.jedis.search.IndexDefinition;
import redis.clients.jedis.search.IndexOptions;
import redis.clients.jedis.search.Schema;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * The RedisSearch class is responsible for accessing the RediSearch
 * module commands via the Jedis programmatic library.  It designed to
 * simplify the use of core Foundation class objects like items,
 * documents and grids.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * The goal of class is to showcase most of the features of RediSearch
 * which means that the search index created will be larger because
 * each field could be saved with different options such as stemming,
 * tags, phonetics and sortable.
 *
 * @see <a href="https://oss.redislabs.com/redisearch/index.html">OSS RediSearch</a>
 * @see <a href="https://oss.redislabs.com/redisearch/Aggregations/">OSS RediSearch Aggregations</a>
 * @see <a href="https://github.com/RediSearch/JRediSearch">Java RediSearch</a>
 * @see <a href="https://redislabs.com/redis-enterprise/technology/redis-search/">Redis Enterprise RediSearch</a>
 * @see <a href="https://github.com/RediSearch/RediSearch/blob/master/docs/DESIGN.md">RediSearch Design</a>
 * @see <a href="https://medium.com/@RedisLabs/adding-a-search-engine-to-redis-adventures-in-module-land-6c8b28b600b">RediSearch Module</a>
 * @see <a href="https://redislabs.com/blog/mastering-redisearch-part/">Mastering RediSearch Part 1</a>
 * @see <a href="https://redislabs.com/blog/mastering-redisearch-part-ii/">Mastering RediSearch Part 2</a>
 * @see <a href="https://redislabs.com/blog/mastering-redisearch-part-iii/">Mastering RediSearch Part 3</a>
 * @see <a href="https://redislabs.com/blog/redisearch-1-4-phonetics-spell-check/">Phonetics and Spellcheck</a>
 * @see <a href="https://github.com/redis/jedis">Java Redis</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisSearch
{
	protected String mIndexName;
	protected DataDoc mDataSchema;
	protected final AppCtx mAppCtx;
	protected final CfgMgr mCfgMgr;
	protected DataDoc mSearchSchema;
	protected final RedisDS mRedisDS;
	protected final RedisKey mRedisKey;
	protected final Redis.Document mDocument;
	protected final UnifiedJedis mCmdConnection;

	/**
     * Constructor accepts a Redis data source parameter and initializes
	 * the search objects accordingly.
	 *
	 * @param aDocument Identifies the type of the documents that will be indexed
	 *
	 * @param aRedisDS Redis data source instance
	*/
	public RedisSearch(RedisDS aRedisDS, Redis.Document aDocument)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mCfgMgr = aRedisDS.getCfgMgr();
		mDocument = aDocument;
		mRedisKey = mRedisDS.getRedisKey();
		mRedisDS.setEncryptionOption(Redis.Encryption.None);
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
	}

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the search objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 * @param aDocument Identifies the type of the documents that will be indexed
	 * @param aDataSchemaDoc Data schema data document
	 * @param aSearchSchemaDoc Search schema data document
	 */
	public RedisSearch(RedisDS aRedisDS, Redis.Document aDocument,
					   DataDoc aDataSchemaDoc, DataDoc aSearchSchemaDoc)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		mCfgMgr = aRedisDS.getCfgMgr();
		mDocument = aDocument;
		mRedisKey = mRedisDS.getRedisKey();
		mRedisDS.setEncryptionOption(Redis.Encryption.None);
		mCmdConnection = new UnifiedJedis(aRedisDS.getCmdConnection().getConnection());
		setDataSchema(aDataSchemaDoc);
		setSearchSchema(aSearchSchemaDoc);
	}

	/**
	 * Returns a Redis unified command connection instance.
	 *
	 * @return Unified command connection instance
	 */
	public UnifiedJedis getCmdConnection()
	{
		return mCmdConnection;
	}

	/**
	 * Assigns the search index name.
	 *
	 * @param aName Name of search index
	 */
	public void setIndexName(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
			mIndexName = aName;
	}

	/**
	 * Returns the storage type of indexed documents.
	 *
	 * @return Document store type
	 */
	public Redis.Document getDocumentStoreType()
	{
		return mDocument;
	}

	/**
	 * Assigns the data document instance as the schema definition.
	 * The data schema definition is tracked in this package as a
	 * convenience to the calling application and used when the
	 * parent application needs to add, update or delete documents
	 * in the search index.
	 *
	 * @param aSchemaDoc Data document instance
	 */
	public void setDataSchema(DataDoc aSchemaDoc)
	{
		mDataSchema = aSchemaDoc;
	}

	/**
	 * Retrieves the internal data schema data document definition.
	 *
	 * @return Data schema data document definition
	 */
	public DataDoc getDataSchema()
	{
		return mDataSchema;
	}

	/**
	 * Assigns the data document instance as the schema definition.
	 *
	 * @param aSchemaDoc Data document instance
	 */
	public void setSearchSchema(DataDoc aSchemaDoc)
	{
		mSearchSchema = aSchemaDoc;
	}

	/**
	 * Retrieves the internal search schema data document definition.
	 *
	 * @return Search schema data document definition
	 */
	public DataDoc getSearchSchema()
	{
		return mSearchSchema;
	}

	/**
	 * Retrieves the search index name based on how it was assigned
	 * in the data source.  If an index name was assigned to the
	 * schema, then it used first.  Next, the internal index name
	 * member is selected.  Finally, if the first two do not yield
	 * a name, then the "search_index" application property is used.
	 *
	 * @return Search index name
	 */
	public String getIndexName()
	{
		String indexName;

		if (StringUtils.isNotEmpty(mIndexName))
			indexName = mIndexName;
		else if ((mSearchSchema != null) && (mSearchSchema.isFeatureAssigned(Redis.FEATURE_INDEX_NAME)))
			indexName = mSearchSchema.getFeature(Redis.FEATURE_INDEX_NAME);
		else if ((mSearchSchema != null) && (StringUtils.isNotEmpty(mSearchSchema.getName())))
			indexName = mSearchSchema.getName();
		else
			indexName = mCfgMgr.getString("search_index");

		if (StringUtils.isEmpty(mIndexName))
			mIndexName = indexName;

		return indexName;
	}

	/**
	 * Creates a data document with items suitable for a default search schema.
	 * The logic assumes that all of the data schema fields should be included
	 * in the search schema.
	 *
	 * @param aDataSchemaDoc Data schema document instance
	 * @param anIndexName Name of the search index
	 *
	 * @return Data document representing a search schema
	 */
	public DataDoc createSchemaDoc(DataDoc aDataSchemaDoc, String anIndexName)
	{
		DataItem searchItem;

		DataDoc searchSchemaDoc = new DataDoc(aDataSchemaDoc.getName());
		searchSchemaDoc.setFeatures(aDataSchemaDoc.getFeatures());
		if (StringUtils.isNotEmpty(anIndexName))
			searchSchemaDoc.addFeature(Redis.FEATURE_INDEX_NAME, anIndexName);
		for (DataItem dataItem : aDataSchemaDoc.getItems())
		{
			searchItem = new DataItem(dataItem);
			if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
			{
				if (Data.isText(dataItem.getType()))
					searchItem.enableFeature(Redis.FEATURE_IS_HIGHLIGHTED);
			}
			if (Data.isText(dataItem.getType()))
			{
				searchItem.enableFeature(Redis.FEATURE_IS_STEMMED);
				searchItem.addFeature(Redis.FEATURE_WEIGHT, Redis.FIELD_WEIGHT_DEFAULT);
			}
			else
				searchItem.addFeature(Redis.FEATURE_WEIGHT, "0.0");
			if (dataItem.isFeatureAssigned(Data.FEATURE_IS_SECRET))
				searchItem.disableFeature(Data.FEATURE_IS_SECRET);	// You cannot search against encrypted fields.
			searchSchemaDoc.add(searchItem);
		}

		return searchSchemaDoc;
	}

	/**
	 * Creates a data document with items suitable for a schema editor UI.
	 *
	 * @param aName Name of the schema
	 *
	 * @return Data document representing a schema
	 */
	public DataDoc createSchemaDoc(String aName)
	{
		DataDoc schemaDoc = new DataDoc(aName);
		schemaDoc.add(new DataItem.Builder().name("item_name").title("Item Name").build());
		schemaDoc.add(new DataItem.Builder().name("item_type").title("Item Type").build());
		schemaDoc.add(new DataItem.Builder().name("item_title").title("Item Title").build());
		if (mDocument == Redis.Document.JSON)
			schemaDoc.add(new DataItem.Builder().name("json_path").title("JSON Path").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Float).name("field_weight").title("Field Weight").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_PRIMARY).title("Is Primary").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_REQUIRED).title("Is Required").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_VISIBLE).title("Is Visible").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Data.FEATURE_IS_SUGGEST).title("Is Suggest").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Redis.FEATURE_IS_FACET_FIELD).title("Is Facet").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Redis.FEATURE_IS_HIGHLIGHTED).title("Is Highlighted").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Redis.FEATURE_IS_STEMMED).title("Is Stemmed").build());
		schemaDoc.add(new DataItem.Builder().type(Data.Type.Boolean).name(Redis.FEATURE_IS_GEO_FIELD).title("Is Geo").build());

		return schemaDoc;
	}

	/**
	 * Identifies if the feature name is standard to the search
	 * data source package.
	 *
	 * @param aName Name of the feature
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureStandard(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
		{
			switch (aName)
			{
				case Data.FEATURE_IS_PRIMARY:
				case Data.FEATURE_IS_REQUIRED:
				case Data.FEATURE_IS_VISIBLE:
				case Data.FEATURE_IS_SUGGEST:
				case Redis.FEATURE_IS_FACET_FIELD:
				case Redis.FEATURE_IS_HIGHLIGHTED:
				case Redis.FEATURE_IS_STEMMED:
				case Redis.FEATURE_IS_GEO_FIELD:
					return true;
			}
		}

		return false;
	}

	/**
	 * Convert a data document schema definition into a data grid suitable for
	 * rendering in a schema editor UI.
	 *
	 * @param aSchemaDoc Data document instance (representing the schema)
	 * @param anIsExtended If <i>true</i>, then non standard features will be recognized
	 *
	 * @return Data grid representing the schema definition
	 */
	public DataGrid schemaDocToDataGrid(DataDoc aSchemaDoc, boolean anIsExtended)
	{
		HashMap<String,String> mapFeatures;

// Create our initial data grid schema based on standard item info plus features.

		String schemaName = String.format("%s Schema", aSchemaDoc.getName());
		DataDoc schemaDoc = createSchemaDoc(schemaName);
		DataGrid dataGrid = new DataGrid(schemaDoc);

// Extend the data grid schema for user defined features.

		if (anIsExtended)
		{
			Data.Type featureType;
			String featureKey, featureValue, featureTitle;

			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				mapFeatures = dataItem.getFeatures();
				for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				{
					featureKey = featureEntry.getKey();
					if (! isFeatureStandard(featureKey))
					{
						if (StringUtils.startsWith(featureKey, "is"))
							featureType = Data.Type.Boolean;
						else
						{
							featureValue = featureEntry.getValue();
							if (NumberUtils.isParsable(featureValue))
							{
								int offset = featureValue.indexOf(StrUtl.CHAR_DOT);
								if (offset == -1)
									featureType = Data.Type.Integer;
								else
									featureType = Data.Type.Float;
							}
							else
								featureType = Data.Type.Text;
						}
						featureTitle = Data.nameToTitle(featureKey);
						dataGrid.addCol(new DataItem.Builder().type(featureType).name(featureKey).title(featureTitle).build());
					}
				}
			}
		}

// Populate each row of the data grid based on the schema data document.

		for (DataItem dataItem : aSchemaDoc.getItems())
		{
			dataGrid.newRow();
			dataGrid.setValueByName("item_name", dataItem.getName());
			dataGrid.setValueByName("item_type", Data.typeToString(dataItem.getType()));
			dataGrid.setValueByName("item_title", dataItem.getTitle());
			if (mDocument == Redis.Document.JSON)
				dataGrid.setValueByName("json_path", dataItem.getFeature(Data.FEATURE_JSON_PATH));
			dataGrid.setValueByName("field_weight", dataItem.getFeatureAsFloat(Redis.FEATURE_WEIGHT));
			mapFeatures = dataItem.getFeatures();
			for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				dataGrid.setValueByName(featureEntry.getKey(), featureEntry.getValue());
			dataGrid.addRow();
		}

		return dataGrid;
	}

	/**
	 * Collapses a data grid representing a schema definition back into a
	 * data document schema.  This method assumes that you invoked the
	 * schemaDocToDataGrid() method to build the data grid originally.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Data document schema definition
	 */
	public DataDoc dataGridToSchemaDoc(DataGrid aDataGrid)
	{
		DataDoc dataDoc;
		DataItem schemaItem;
		String itemName, itemType, itemTitle;

		DataDoc schemaDoc = new DataDoc(aDataGrid.getName());
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			dataDoc = aDataGrid.getRowAsDoc(row);
			itemName = dataDoc.getValueByName("item_name");
			itemType = dataDoc.getValueByName("item_type");
			itemTitle = dataDoc.getValueByName("item_title");
			if ((StringUtils.isNotEmpty(itemName)) && (StringUtils.isNotEmpty(itemType)) && (StringUtils.isNotEmpty(itemTitle)))
			{
				schemaItem = new DataItem.Builder().type(Data.stringToType(itemType)).name(itemName).title(itemTitle).build();
				for (DataItem dataItem : dataDoc.getItems())
				{
					if (! StringUtils.startsWith(dataItem.getName(), "item_"))
						dataItem.addFeature(dataItem.getName(), dataItem.getValue());
				}
				schemaDoc.add(schemaItem);
			}
		}

		return schemaDoc;
	}

	private IndexOptions dataDocToIndexOptions(DataDoc aDataDoc, DSCriteria aDSCriteria, StringBuilder aSB)
		throws RedisDSException
	{
		String schemaKeyPrefix;
		IndexDefinition indexDefinition;

		if (mDocument == Redis.Document.JSON)
		{
			indexDefinition = new IndexDefinition(IndexDefinition.Type.JSON);
			schemaKeyPrefix = mRedisKey.searchPrefix(Redis.KEY_MODULE_JSON);
			aSB.append(String.format(" ON JSON PREFIX 1 %s", schemaKeyPrefix));
		}
		else
		{
			indexDefinition = new IndexDefinition(IndexDefinition.Type.HASH);
			schemaKeyPrefix = mRedisKey.searchPrefix(Redis.KEY_MODULE_CORE);
			aSB.append(String.format(" ON HASH PREFIX 1 %s", schemaKeyPrefix));
		}
		String[] keyPrefixes = new String[1];
		keyPrefixes[0] = schemaKeyPrefix;
		indexDefinition.setPrefixes(keyPrefixes);
		if ((aDSCriteria != null) && (aDSCriteria.count() > 0))
		{
			SearchCriteria searchCriteria = new SearchCriteria(mRedisDS, this);
			String queryFilter = searchCriteria.criteriaToQueryString(aDSCriteria, null, null);
			if (StringUtils.isNotEmpty(queryFilter))
			{
				indexDefinition.setFilter(queryFilter);
				aSB.append(String.format(" FILTERS \"%s\"", queryFilter));
			}
		}

// Assign general index options.

		int optionFlags = 0;
		if (aDataDoc.isFeatureTrue(Redis.FEATURE_NO_TERM_OFFSETS))
			aSB.append(" NOOFFSETS");
		else
			optionFlags |= IndexOptions.USE_TERM_OFFSETS;
		if (aDataDoc.isFeatureTrue(Redis.FEATURE_NO_FIELDS))
			aSB.append(" NOFIELDS");
		else
			optionFlags |= IndexOptions.KEEP_FIELD_FLAGS;
		if (aDataDoc.isFeatureTrue(Redis.FEATURE_NO_TERM_FREQUENCIES))
			aSB.append(" NOFREQS");
		else
			optionFlags |= IndexOptions.KEEP_TERM_FREQUENCIES;
		IndexOptions indexOptions = new IndexOptions(optionFlags);
		if (aDataDoc.isFeatureTrue(Redis.FEATURE_NO_STOP_WORDS))
		{
			indexOptions.setNoStopwords();
			aSB.append(" STOPWORDS 0");
		}
		else
		{
			if (aDataDoc.isFeatureAssigned(Redis.FEATURE_STOP_WORDS))
			{
				String swPathFileName = aDataDoc.getFeature(Redis.FEATURE_STOP_WORDS);
				try
				{
					List<String> stopWordsList = Redis.loadFileList(swPathFileName);
					int stopWordCount = stopWordsList.size();
					aSB.append(String.format(" STOPWORDS %d", stopWordCount));
					String[] strValues = new String[stopWordCount];
					strValues = stopWordsList.toArray(strValues);
					indexOptions.setStopwords(strValues);
					for (String stopWord : stopWordsList)
						aSB.append(String.format(" %s", mRedisDS.escapeValue(stopWord)));
				}
				catch (IOException e)
				{
					String msgStr = String.format("Stop words [%s]: %s", swPathFileName, e.getMessage());
					throw new RedisDSException(msgStr);
				}
			}
		}
		if (aDataDoc.isFeatureAssigned(Redis.FEATURE_INDEX_EXPIRATION))
		{
			long indexExpiration = Data.createLong(aDataDoc.getFeature(Redis.FEATURE_INDEX_EXPIRATION));
			indexOptions.setTemporary(indexExpiration);
			aSB.append(String.format(" TEMPORARY %d", indexExpiration));
		}
		indexOptions.setDefinition(indexDefinition);

		return indexOptions;
	}

	/**
	 * Adds one or more schema fields for each data document item.  The goal
	 * of this method is to reduce the need to rebuild the search index if
	 * we want to query on different schema field types, so we will define
	 * all possible field types in the schema at the expense of using more
	 * index space.  The query logic will select the corresponding schema
	 * field name based on the features enabled for each item.
	 *
	 * @param aSchema RediSearch schema instance
	 * @param aDataItem Data item instance
	 * @param aSB String builder instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	private void addDataItemToSchema(Schema aSchema, DataItem aDataItem, StringBuilder aSB)
		throws RedisDSException
	{
		double fieldWeight;
		String phoneticMatcher, tagSeparator, jsonPahtName;
		FieldName textFieldName, stemFieldName, phoneticFieldName, tagFieldName, numericFieldName;
		Logger appLogger = mAppCtx.getLogger(this, "addDataItemToSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String itemName = aDataItem.getName();
		String fieldName = Redis.schemaFieldName(aDataItem);
		Data.Type dataType = aDataItem.getType();
		if (Data.isText(dataType))
		{
			boolean isGeoField = aDataItem.isFeatureTrue(Redis.FEATURE_IS_GEO_FIELD);
			if (isGeoField)
			{
				aSchema.addField(new Schema.Field(fieldName, Schema.FieldType.GEO, true, false));
				aSB.append(" GEO");
			}
			else
			{
				if (aDataItem.isFeatureAssigned(Redis.FEATURE_WEIGHT))
					fieldWeight = aDataItem.getFeatureAsFloat(Redis.FEATURE_WEIGHT);
				else
					fieldWeight = Redis.FIELD_WEIGHT_DEFAULT;
				if (aDataItem.isFeatureAssigned(Redis.FEATURE_PHONETIC_MATCHER))
					phoneticMatcher = aDataItem.getFeature(Redis.FEATURE_PHONETIC_MATCHER);
				else
					phoneticMatcher = Redis.FEATURE_PHONETIC_ENGLISH;
				tagFieldName = new FieldName(fieldName, Redis.tagFieldName(itemName));
				aSB.append(String.format(" %s AS %s TAG",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.tagFieldName(itemName))));
				if (aDataItem.isFeatureAssigned(Redis.FEATURE_TAG_FIELD_SEPARATOR))
				{
					tagSeparator = aDataItem.getFeature(Redis.FEATURE_TAG_FIELD_SEPARATOR);
					aSB.append(String.format(" SEPARATOR %s", mRedisDS.escapeValue(tagSeparator)));
				}
				else
					tagSeparator = null;
				aSchema.addField(new Schema.TagField(tagFieldName, tagSeparator, false));
				textFieldName = new FieldName(fieldName, Redis.textFieldName(itemName));
				aSB.append(String.format(" %s AS %s TEXT NOSTEM WEIGHT %.1f",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.textFieldName(itemName)), fieldWeight));
				aSchema.addField(new Schema.TextField(textFieldName, fieldWeight, false, true, false, null));
				stemFieldName = new FieldName(fieldName, Redis.stemmedFieldName(itemName));
				aSB.append(String.format(" %s AS %s TEXT WEIGHT %.1f",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.stemmedFieldName(itemName)), fieldWeight));
				aSchema.addField(new Schema.TextField(stemFieldName, fieldWeight, false, false, false, null));
				phoneticFieldName = new FieldName(fieldName, Redis.phoneticFieldName(itemName));
				aSB.append(String.format(" %s AS %s TEXT PHONETIC %s WEIGHT %.1f",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.phoneticFieldName(itemName)), mRedisDS.escapeValue(phoneticMatcher), fieldWeight));
				aSchema.addField(new Schema.TextField(phoneticFieldName, fieldWeight, false, false, false, phoneticMatcher));
			}
		}
		else if (Data.isNumber(dataType))
		{
			numericFieldName = new FieldName(fieldName, Redis.numericFieldName(itemName));
			aSchema.addField(new Schema.Field(numericFieldName, Schema.FieldType.NUMERIC, true, false));
			aSB.append(String.format(" %s AS %s NUMERIC SORTABLE",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.numericFieldName(itemName))));
		}
		else if (Data.isDateOrTime(dataType))
		{
			if (mDocument == Redis.Document.Hash)
				numericFieldName = new FieldName(Redis.shadowFieldName(itemName), Redis.numericFieldName(itemName));
			else
			{
				jsonPahtName = String.format("$.%s", Redis.shadowFieldName(itemName));
				numericFieldName = new FieldName(jsonPahtName, Redis.numericFieldName(itemName));
			}
			aSchema.addField(new Schema.Field(numericFieldName, Schema.FieldType.NUMERIC, true, false));
			aSB.append(String.format(" %s AS %s NUMERIC SORTABLE",  mRedisDS.escapeKey(Redis.shadowFieldName(itemName)), mRedisDS.escapeKey(Redis.numericFieldName(itemName))));
			tagFieldName = new FieldName(fieldName, Redis.tagFieldName(itemName));
			aSB.append(String.format(" %s AS %s TAG",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.tagFieldName(itemName))));
			if (aDataItem.isFeatureAssigned(Redis.FEATURE_TAG_FIELD_SEPARATOR))
			{
				tagSeparator = aDataItem.getFeature(Redis.FEATURE_TAG_FIELD_SEPARATOR);
				aSB.append(String.format(" SEPARATOR %s", mRedisDS.escapeValue(tagSeparator)));
			}
			else
				tagSeparator = null;
			if (mDocument == Redis.Document.Hash)
				aSchema.addField(new Schema.TagField(tagFieldName, tagSeparator, true));
			else
				aSchema.addField(new Schema.TagField(tagFieldName, tagSeparator, false));
		}
		else if (Data.isBoolean(dataType))
		{
			tagFieldName = new FieldName(fieldName, Redis.tagFieldName(itemName));
			aSB.append(String.format(" %s AS %s TAG",  mRedisDS.escapeKey(fieldName), mRedisDS.escapeKey(Redis.tagFieldName(itemName))));
			if (aDataItem.isFeatureAssigned(Redis.FEATURE_TAG_FIELD_SEPARATOR))
			{
				tagSeparator = aDataItem.getFeature(Redis.FEATURE_TAG_FIELD_SEPARATOR);
				aSB.append(String.format(" SEPARATOR %s", mRedisDS.escapeValue(tagSeparator)));
			}
			else
				tagSeparator = null;
			aSchema.addField(new Schema.TagField(tagFieldName, tagSeparator, false));
		}
		else
			throw new RedisDSException(String.format("%s: Unknown data type '%s'", fieldName, Data.typeToString(dataType)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private Schema dataDocToSchema(DataDoc aDataDoc, StringBuilder aSB)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "dataDocToSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		aSB.append(" SCHEMA");
		Schema indexSchema = new Schema();
		for (DataItem dataItem : aDataDoc.getItems())
			addDataItemToSchema(indexSchema, dataItem, aSB);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return indexSchema;
	}

	private void ensurePreconditions(boolean aIsSchemaDefined)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "ensurePreconditions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mRedisDS.ensurePreconditions();
		if ((aIsSchemaDefined) && (mSearchSchema == null))
			throw new RedisDSException("Index schema has not been defined.");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private void enforceSchemaLimits(DataDoc aSearchSchemaDoc)
		throws RedisDSException
	{
		int totalFields = aSearchSchemaDoc.count();
		if (totalFields > Redis.SCHEMA_MAXIMUM_ALL_FIELDS)
			throw new RedisDSException(String.format("[%s] Schema has %d fields which exceeds the maximum of %d for RediSearch.",
												  	aSearchSchemaDoc.getName(), totalFields, Redis.SCHEMA_MAXIMUM_ALL_FIELDS));
		int totalTextFields = 0;
		for (DataItem dataItem : aSearchSchemaDoc.getItems())
		{
			if (Data.isText(dataItem.getType()))
				totalTextFields++;
		}
		if (totalTextFields > Redis.SCHEMA_MAXIMUM_TEXT_FIELDS)
			throw new RedisDSException(String.format("[%s] Schema has %d text fields which exceeds the maximum of %d for RediSearch.",
												 	aSearchSchemaDoc.getName(), totalTextFields, Redis.SCHEMA_MAXIMUM_TEXT_FIELDS));
	}

	/**
	 * Stores the search schema definition in the Redis database.  This
	 * schema definition is one that could be used by a parent application
	 * to describe the current search schema.
	 *
	 * @param aSearchSchemaDoc Search schema data document instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void saveSchemaDefinition(DataDoc aSearchSchemaDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveSchemaDefinition");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);

		String schemaKeyName = mRedisKey.moduleSearch().redisSearchSchema().dataName(getIndexName()).name();
		DataDocXML dataDocXML = new DataDocXML(aSearchSchemaDoc);
		dataDocXML.setHeaderSaveFlag(true);
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter);
		try
		{
			dataDocXML.save(printWriter);
		}
		catch (IOException e)
		{
			throw new RedisDSException(e.getMessage());
		}
		String xmlSchemaString = stringWriter.toString().replaceAll("\\n", StringUtils.EMPTY);
		mCmdConnection.set(schemaKeyName, xmlSchemaString);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Stores the search schema definition in the Redis database.  This
	 * schema definition is one that could be used by a parent application
	 * to describe the current search schema.
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void saveSchemaDefinition()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "saveSchemaDefinition");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		saveSchemaDefinition(mSearchSchema);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Create a RediSearch index with a schema based on the data document and
	 * data source criteria instances.
	 *
	 * @param aSearchSchemaDoc Data document for scheme definition
	 * @param aDSCriteria Data source criteria for index definition
	 *
	 * @return <i>true</i> if the operation succeeded or <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public boolean createIndexSaveSchema(DataDoc aSearchSchemaDoc, DSCriteria aDSCriteria)
		throws RedisDSException
	{
		boolean isOK;
		Logger appLogger = mAppCtx.getLogger(this, "createIndexSaveSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(false);
		enforceSchemaLimits(aSearchSchemaDoc);

		String indexKeyName = mRedisKey.moduleSearch().redisSearchIndex().dataName(getIndexName()).name();
		StringBuilder stringBuilder = new StringBuilder(String.format("FT.CREATE %s", mRedisDS.escapeKey(indexKeyName)));
		IndexOptions indexOptions = dataDocToIndexOptions(aSearchSchemaDoc, aDSCriteria, stringBuilder);
		Schema rsSchema = dataDocToSchema(aSearchSchemaDoc, stringBuilder);
		String msgString = mCmdConnection.ftCreate(indexKeyName, indexOptions, rsSchema);
		mRedisDS.saveCommand(appLogger, stringBuilder.toString());
		if (Redis.isResponseOK(msgString))
		{
			isOK = true;
			setSearchSchema(aSearchSchemaDoc);
			setIndexName(getIndexName());
			saveSchemaDefinition(aSearchSchemaDoc);
		}
		else
			isOK = false;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Create a RediSearch index with a schema based on the data document instance.
	 *
	 * @param aSearchSchemaDoc Data document for scheme definition
	 *
	 * @return <i>true</i> if the operation succeeded or <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public boolean createIndexSaveSchema(DataDoc aSearchSchemaDoc)
		throws RedisDSException
	{
		boolean isOK;
		Logger appLogger = mAppCtx.getLogger(this, "createIndexSaveSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (indexExists())
			dropIndex(false);

		ensurePreconditions(false);
		enforceSchemaLimits(aSearchSchemaDoc);
		aSearchSchemaDoc.setName(getIndexName());

		String indexKeyName = mRedisKey.moduleSearch().redisSearchIndex().dataObject(aSearchSchemaDoc).name();
		StringBuilder stringBuilder = new StringBuilder(String.format("FT.CREATE %s", mRedisDS.escapeKey(indexKeyName)));
		IndexOptions indexOptions = dataDocToIndexOptions(aSearchSchemaDoc, null, stringBuilder);
		Schema rsSchema = dataDocToSchema(aSearchSchemaDoc, stringBuilder);
		String msgString = mCmdConnection.ftCreate(indexKeyName, indexOptions, rsSchema);
		mRedisDS.saveCommand(appLogger, stringBuilder.toString());
		if (Redis.isResponseOK(msgString))
		{
			isOK = true;
			setSearchSchema(aSearchSchemaDoc);
			saveSchemaDefinition(aSearchSchemaDoc);
		}
		else
			isOK = false;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Identifies if the schema definition for the search index has been
	 * stored in the Redis DB.
	 *
	 * @return <i>true</i> if it exists and <i>false</i> otherwise
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public boolean schemaExists()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "schemaExists");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);

		String schemaKeyName = mRedisKey.moduleSearch().redisSearchSchema().dataName(getIndexName()).name();
		boolean schemaExists = mCmdConnection.exists(schemaKeyName);
		mRedisDS.saveCommand(appLogger, String.format("EXISTS %s", mRedisDS.escapeKey(schemaKeyName)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return schemaExists;
	}

	/**
	 * Loads the schema definition from the Redis DB into a data document
	 * instance.  If the load fails or the key does not exist, then the
	 * optional data document will not be present.
	 *
	 * @return Optional data document instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public Optional<DataDoc> loadSchema()
		throws RedisDSException
	{
		DataDoc schemaDoc;
		Logger appLogger = mAppCtx.getLogger(this, "loadSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(false);

		String schemaKeyName = mRedisKey.moduleSearch().redisSearchSchema().dataName(getIndexName()).name();
		RedisCore redisCore = mRedisDS.createCore();
		if (redisCore.exists(schemaKeyName))
		{
			String schemaString = redisCore.get(schemaKeyName);
			DataDocXML dataDocXML = new DataDocXML();
			try
			{
				InputStream inputStream = IOUtils.toInputStream(schemaString, StrUtl.CHARSET_UTF_8);
				dataDocXML.load(inputStream);
			}
			catch (ParserConfigurationException | SAXException | IOException e)
			{
				throw new RedisDSException(e.getMessage());
			}
			schemaDoc = dataDocXML.getDataDoc();
		}
		else
			schemaDoc = null;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(schemaDoc);
	}

	private boolean assignFeatureNameValue(DataItem aDataItem, String aName, String aValue)
	{
		boolean isValueDifferent;

		if (StringUtils.isNotEmpty(aValue))
		{
			if (aDataItem.isFeatureAssigned(aName))
			{
				String featureValue = aDataItem.getFeature(aName);
				isValueDifferent = !featureValue.equals(aValue);
			}
			else
				isValueDifferent = true;

			aDataItem.addFeature(aName, aValue);
		}
		else
			isValueDifferent = false;

		return isValueDifferent;
	}

	/**
	 * Updates the data items captured in the schema document parameter
	 * <i>DataDoc</i> against the current search schema.  The data items
	 * must be derived from the schema definition.
	 *
	 * <b>Note:</b> Once the update has been applied, the RediSearch
	 * index will be out-of-sync with it, so you need to rebuild the
	 * search index to reflect these changes.
	 *
	 * @param aSchemaDoc Data document instance
	 *
	 * @return <i>true</i> if the search index should be rebuilt
	 * and <i>false</i> otherwise
	 */
	public boolean updateSchema(DataDoc aSchemaDoc)
	{
		boolean valueChanged;
		String itemName, featureName, featureValue;
		Logger appLogger = mAppCtx.getLogger(this, "updateSchema");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean rebuildIndex = false;
		aSchemaDoc.setName(getIndexName());
		Optional<DataItem> optSchemaDocType = aSchemaDoc.getItemByNameOptional("item_type");
		Optional<DataItem> optSearchSchemaItem = mSearchSchema.getItemByNameOptional(aSchemaDoc.getValueByName("item_name"));
		if ((optSearchSchemaItem.isPresent()) && (optSchemaDocType.isPresent()))
		{
			DataItem searchSchemaItem = optSearchSchemaItem.get();
			DataItem schemaDocTypeItem = optSchemaDocType.get();

			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				itemName = dataItem.getName();
				featureValue = dataItem.getValue();
				if ((itemName.equals("field_weight")) && (Data.isText(Data.stringToType(schemaDocTypeItem.getValue()))))
				{
					valueChanged = assignFeatureNameValue(searchSchemaItem, Redis.FEATURE_WEIGHT, featureValue);
					if ((! rebuildIndex) && (valueChanged))
						rebuildIndex = valueChanged;
				}
				else if (! itemName.startsWith("item_"))
				{
					featureName = dataItem.getName();
					if (Data.isText(dataItem.getType()))
					{
						switch (featureName)
						{
							case Data.FEATURE_IS_SEARCH:
							case Data.FEATURE_IS_SUGGEST:
							case Redis.FEATURE_IS_STEMMED:
							case Redis.FEATURE_IS_PHONETIC:
								valueChanged = assignFeatureNameValue(searchSchemaItem, featureName, featureValue);
								if ((! rebuildIndex) && (valueChanged))
									rebuildIndex = valueChanged;
								break;
						}
					}
					else
					{
						valueChanged = assignFeatureNameValue(searchSchemaItem, featureName, featureValue);
						if (! featureName.equals(Redis.FEATURE_IS_FACET_FIELD))
						{
							if ((! rebuildIndex) && (valueChanged))
								rebuildIndex = valueChanged;
						}
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rebuildIndex;
	}

	private Map<String, Object> dataDocToMap(DataDoc aDataDoc, StringBuilder aSB)
	{
		String itemName;
		Logger appLogger = mAppCtx.getLogger(this, "dataDocToMap");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Map<String, Object> rsMap = new HashMap<>();
		for (DataItem dataItem : aDataDoc.getItems())
		{
			if (dataItem.isFeatureFalse(Data.FEATURE_IS_HIDDEN))
			{
				itemName = dataItem.getName();
				rsMap.put(itemName, dataItem.getValueAsObject());
				aSB.append(String.format(" %s %s", mRedisDS.escapeKey(itemName), mRedisDS.escapeValue(dataItem.getValue())));
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return rsMap;
	}

	private String deriveDocumentId(DataDoc aDataDoc, boolean aIsAddOperation)
		throws RedisDSException
	{
		String docId;

		Optional<DataItem> optDataItem = aDataDoc.getItemByNameOptional(Redis.RS_DOCUMENT_ID);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			docId = dataItem.getValue();
		}
		else
		{
			optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
			if (optDataItem.isPresent())
			{
				DataItem dataItem = optDataItem.get();
				docId = aDataDoc.getValueByName(dataItem.getName());
			}
			else
				throw new RedisDSException("The data document lacks a primary key.");
		}

		if ((StringUtils.isEmpty(docId)) && (aIsAddOperation))
		{
			long keyId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
			docId = Long.toString(keyId);
		}

		return docId;
	}

	private DataItem shadowDataItem(DataItem aDataItem, Date aDate)
	{
		DataItem dataItem = new DataItem(Data.Type.Long, Redis.shadowFieldName(aDataItem.getName()));
		dataItem.setValue(aDate.getTime());

		return dataItem;
	}

	private DataDoc enrichDataDoc(DataDoc aDataDoc)
		throws RedisDSException
	{
		Date itemDate;
		DataItem shadowDataItem;
		DataDoc enrichedDataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "enrichDataDoc");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataDoc == null)
			throw new RedisDSException("Data document is null.");

		enrichedDataDoc = new DataDoc(aDataDoc);
		for (DataItem dataItem : aDataDoc.getItems())
		{
			if (dataItem.isValueAssigned())
			{
				if (Data.isDateOrTime(dataItem.getType()))
				{
					itemDate = dataItem.getValueAsDate();
					if (itemDate == null)
						appLogger.error(String.format("%s: Unable to parse '%s' format of '%s'", dataItem.getName(),
													  dataItem.getValue(), dataItem.getDataFormat()));
					else
					{
						shadowDataItem = shadowDataItem(dataItem, itemDate);
						enrichedDataDoc.add(shadowDataItem);
					}
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return enrichedDataDoc;
	}

	/**
	 * Convenience method that adds a unique document id item to the data document instance.
	 *
	 * @param aDataDoc Data document instance
	 */
	public void addHiddenDocumentId(DataDoc aDataDoc)
	{
		Optional<DataItem> optDataItem = aDataDoc.getItemByNameOptional(Redis.RS_DOCUMENT_ID);
		if (optDataItem.isEmpty())
			aDataDoc.add(new DataItem.Builder().name(Redis.RS_DOCUMENT_ID).title("RS Document Id").isHidden(true).build());
	}

	/**
	 * Add the suggestion item value and its payload item value to the search index.
	 *
	 * @param aSuggestionItem Suggestion item instance
	 * @param aPayloadItem Payload item instance
	 *
	 * @return Entry id of the suggestion
	 */
	public long addSuggestion(DataItem aSuggestionItem, DataItem aPayloadItem)
	{
		long totalSuggestions;
		Logger appLogger = mAppCtx.getLogger(this, "addSuggestion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String payloadString = aPayloadItem.getValue();
		String suggestionString = aSuggestionItem.getValue();
		if ((StringUtils.isNotEmpty(suggestionString)) && (StringUtils.isNotEmpty(payloadString)))
		{
			double searchScore = Redis.getScore(aSuggestionItem);
			// ToDo: Enable when Jedis supports this feature
//			Suggestion rsSuggestion = new Suggestion.Builder().str(suggestionString).score(searchScore).payload(payloadString).build();
//			totalSuggestions = mCmdConnection.addSuggestion(rsSuggestion, false);
			totalSuggestions = 0;
			String suggestKeyName = mRedisKey.moduleSearch().redisSearchSuggest().dataName(getIndexName()).name();
			mRedisDS.saveCommand(appLogger, String.format("FT.SUGADD %s %s %.1f PAYLOAD %s", mRedisDS.escapeKey(suggestKeyName), mRedisDS.escapeValue(suggestionString), searchScore, mRedisDS.escapeValue(payloadString)));
		}
		else
			totalSuggestions = -1;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return totalSuggestions;
	}

	/**
	 * Add the suggestion item value and its payload item value to the search index.
	 *
	 * @param aDataDoc Data document instance
	 */
	public void addSuggestion(DataDoc aDataDoc)
	{
		Logger appLogger = mAppCtx.getLogger(this, "addSuggestions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Optional<DataItem> optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_SUGGEST);
		if (optDataItem.isPresent())
		{
			DataItem suggestItem = optDataItem.get();
			optDataItem = aDataDoc.getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
			if (optDataItem.isPresent())
			{
				DataItem primaryItem = optDataItem.get();
				addSuggestion(suggestItem, primaryItem);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Add the suggestion item value and its payload item value to the search index
	 * for each data document row in the data grid.
	 *
	 * @param aDataGrid Data grid instance
	 */
	public void addSuggestions(DataGrid aDataGrid)
	{
		DataDoc dataDoc;
		Logger appLogger = mAppCtx.getLogger(this, "addSuggestions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			dataDoc = aDataGrid.getRowAsDoc(row);
			addSuggestion(dataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Add the suggestion item value and its payload item value to the search index
	 * for each data document row in the data grid.
	 *
	 * @param aDataDocList List of data document instances
	 */
	public void addSuggestions(List<DataDoc> aDataDocList)
	{
		Logger appLogger = mAppCtx.getLogger(this, "addSuggestions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		for (DataDoc dataDoc : aDataDocList)
			addSuggestion(dataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Add the suggestion item value as a collection of term shingles and
	 * assign each suggestion the payload item value in the search index.
	 *
	 * @param aSuggestionItem Suggestion item instance
	 * @param aPayloadItem Payload item instance
	 *
	 * @return Entry id of the suggestion
	 */
	public long addSuggestionWithShingles(DataItem aSuggestionItem, DataItem aPayloadItem)
	{
		long entryId;
		String suggestionString;
		int tokenOffset, tokenLength;
		Logger appLogger = mAppCtx.getLogger(this, "addSuggestionWithShingles");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataItem suggestionItem = new DataItem(aSuggestionItem);
		suggestionItem.clearValues();

		String delimiterChars = " ,.<>{}[]\"':;!@#$%^&*()-+=~";		// https://oss.redislabs.com/redisearch/Escaping/
		String suggestionValue = aSuggestionItem.getValue();
		if ((suggestionValue.length() > Redis.SUGGESTION_MIN_TOKEN_SIZE) &&
			(StringUtils.containsAny(suggestionValue, delimiterChars)))
		{
			entryId = addSuggestion(aSuggestionItem, aPayloadItem);
			StringTokenizer stringTokenizer = new StringTokenizer(suggestionValue, delimiterChars);
			String curToken = stringTokenizer.nextToken();
			int curOffset = curToken.length();
			while ((stringTokenizer.hasMoreTokens()) && curOffset < suggestionValue.length())
			{
				curToken = stringTokenizer.nextToken();
				tokenLength = curToken.length();
				tokenOffset = suggestionValue.indexOf(curToken, curOffset);
				suggestionString = suggestionValue.substring(tokenOffset);
				curOffset = tokenOffset + tokenLength;
				suggestionItem.setValue(suggestionString);
				if (tokenLength >= Redis.SUGGESTION_MIN_TOKEN_SIZE)
					entryId = addSuggestion(suggestionItem, aPayloadItem);
			}
		}
		else
			entryId = addSuggestion(aSuggestionItem, aPayloadItem);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return entryId;
	}

	/**
	 * Retrieves a data grid of matching suggestions with related payloads and scores.
	 *
	 * @param aPrefix Prefix suggestion string
	 * @param aLimit Limit of suggestions
	 * @param aIsFuzzy If <i>true</i> then include fuzzy matches
	 *
	 * @return Data grid instance
	 */
	public DataGrid getSuggestions(String aPrefix, int aLimit, boolean aIsFuzzy)
	{
		Logger appLogger = mAppCtx.getLogger(this, "getSuggestions");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = new DataGrid("Search Suggestions");
		dataGrid.addCol(new DataItem.Builder().name("_suggest").title("Suggestion").build());
		dataGrid.addCol(new DataItem.Builder().name("payload").title("Payload").build());
		dataGrid.addCol(new DataItem.Builder().type(Data.Type.Double).name("score").title("Score").build());

		// ToDo: Enable when Jedis supports this feature
		/*
		SuggestionOptions suggestionOptions;
		int suggestionLimit = Math.max(aLimit, Redis.SUGGESTION_LIMIT_DEFAULT);
		if (aIsFuzzy)
			suggestionOptions = new SuggestionOptions.Builder().with(SuggestionOptions.With.PAYLOAD_AND_SCORES).fuzzy().max(suggestionLimit).build();
		else
			suggestionOptions = new SuggestionOptions.Builder().with(SuggestionOptions.With.PAYLOAD_AND_SCORES).max(suggestionLimit).build();
		List<Suggestion> suggestionList = mCmdConnection.getSuggestion(aPrefix, suggestionOptions);
		String suggestKeyName = mRedisKey.moduleSearch().redisSearchSuggest().dataName(getIndexName()).name();
		mRedisDS.saveCommand(appLogger, String.format("FT.SUGGET %s %s MAX %d WITHSCORES WITHPAYLOADS", mRedisDS.escapeKey(suggestKeyName), mRedisDS.escapeValue(aPrefix), suggestionLimit));
		for (Suggestion searchSuggestion : suggestionList)
		{
			dataGrid.newRow();
			dataGrid.setValueByName("_suggest", searchSuggestion.getString());
			dataGrid.setValueByName("payload", searchSuggestion.getPayload());
			dataGrid.setValueByName("score", searchSuggestion.getScore());
			dataGrid.addRow();
		}
		*/

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Deletes the suggestion associated with the data item instance.
	 *
	 * @param aSuggestionItem Suggestion data item instance
	 */
	public void deleteSuggestion(DataItem aSuggestionItem)
	{
		Logger appLogger = mAppCtx.getLogger(this, "getSuggestion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		// ToDo: Enable when Jedis supports this feature
		String suggestionString = aSuggestionItem.getValue();
//		mCmdConnection.deleteSuggestion(suggestionString);
		String suggestKeyName = mRedisKey.moduleSearch().redisSearchSuggest().dataName(getIndexName()).name();
		mRedisDS.saveCommand(appLogger, String.format("FT.SUGDEL %s %s", mRedisDS.escapeKey(suggestKeyName), mRedisDS.escapeValue(suggestionString)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Add the synonym item value to the search index.
	 *
	 * @see <a href="https://oss.redislabs.com/redisearch/Synonyms/">OSS RediSearch Synonyms</a>
	 *
	 * @param aSynonymItem Synonym item instance
	 *
	 * @return <i>true</i> successful or <i>false</i> failure
	 */
	public boolean addSynonyms(DataItem aSynonymItem)
	{
		Logger appLogger = mAppCtx.getLogger(this, "addSynonyms");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = false;
		if (aSynonymItem.isValueAssigned())
		{
			/* The group id logic was refactored when there was a breaking 2.0 change with
			the addSynonym() method (e.g. it throws exceptions when used.  You must use the
			updateSynonym() which requires a group id to be assigned.  The random id logic
			below is not ideal, but should work for simple implementations. */
			String[] termValues = aSynonymItem.getValuesArray();
			long groupId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
			String synonymKeyName = mRedisKey.moduleSearch().redisSearchSynonym().dataName(getIndexName()).name();
			String msgResponse = mCmdConnection.ftSynUpdate(synonymKeyName, Long.toString(groupId), termValues);
			if (Redis.isResponseOK(msgResponse))
			{
				isOK = true;
				aSynonymItem.addFeature(Redis.FEATURE_GROUP_ID, groupId);
				StringBuilder stringBuilder = new StringBuilder(String.format("FT.SYNUPDATE %s %d", mRedisDS.escapeKey(synonymKeyName), groupId));
				for (String termValue : termValues)
					stringBuilder.append(String.format(" %s", mRedisDS.escapeValue(termValue)));
				mRedisDS.saveCommand(appLogger, stringBuilder.toString());
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	/**
	 * Updates the synonym item value in the search index.
	 *
	 * @see <a href="https://oss.redislabs.com/redisearch/Synonyms/">OSS RediSearch Synonyms</a>
	 *
	 * @param aSynonymItem Synonym item instance
	 *
	 * @return <i>true</i> successful or <i>false</i> failure
	 */
	public boolean updateSynonyms(DataItem aSynonymItem)
	{
		Logger appLogger = mAppCtx.getLogger(this, "updateSynonyms");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		boolean isOK = false;
		String groupId = aSynonymItem.getFeature(Redis.FEATURE_GROUP_ID);
		if ((StringUtils.isNotEmpty(groupId)) && (aSynonymItem.isValueAssigned()))
		{
			String[] termValues = aSynonymItem.getValuesArray();
			String synonymKeyName = mRedisKey.moduleSearch().redisSearchSynonym().dataName(getIndexName()).name();
			String msgResponse = mCmdConnection.ftSynUpdate(synonymKeyName, groupId, termValues);
			isOK = Redis.isResponseOK(msgResponse);
			if (isOK)
			{
				StringBuilder stringBuilder = new StringBuilder(String.format("FT.SYNUPDATE %s %s", mRedisDS.escapeKey(synonymKeyName), groupId));
				for (String termValue : termValues)
					stringBuilder.append(String.format(" %s", mRedisDS.escapeValue(termValue)));
				mRedisDS.saveCommand(appLogger, stringBuilder.toString());
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return isOK;
	}

	private DataGrid createSynonymGridDefinition()
	{
		DataGrid dataGrid = new DataGrid("RediSearch Synonym Grid");
		dataGrid.addCol(new DataItem.Builder().name("synonym_term").title("Synonym Term").build());
		dataGrid.addCol(new DataItem.Builder().type(Data.Type.Long).name("group_ids").title("Group Ids").build());

		return dataGrid;
	}

	/**
	 * Retrieves a data grid (term, group ids) of all the synonyms stored in the search index.
	 *
	 * @return Data grid instance
	 */
	public DataGrid loadSynonyms()
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadSynonyms");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGrid dataGrid = createSynonymGridDefinition();
		String synonymKeyName = mRedisKey.moduleSearch().redisSearchSynonym().dataName(getIndexName()).name();
		Map<String, List<String>> mapSynonyms = mCmdConnection.ftSynDump(synonymKeyName);
		mapSynonyms.forEach((s, ll) -> {
			dataGrid.newRow();
			dataGrid.setValueByName("synonym_term", s);
			ArrayList<String> longValues = new ArrayList<>();
			longValues.addAll(ll);
			dataGrid.setValuesByName("group_ids", longValues);
			dataGrid.addRow();
		});
		mRedisDS.saveCommand(appLogger, String.format("FT.SYNDUMP %s", mRedisDS.escapeKey(synonymKeyName)));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Adds the data document to the search index.  The data document must designate
	 * a primary field using the <code>Data.FEATURE_IS_PRIMARY</code> feature.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void add(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		if (mDocument == Redis.Document.Hash)
		{
			RedisDoc redisDoc = mRedisDS.createDoc();
			redisDoc.add(enrichDataDoc(aDataDoc));
		}
		else
		{
			RedisJson redisJson = mRedisDS.createJson(mDataSchema);
			redisJson.add(enrichDataDoc(aDataDoc));
		}
		addSuggestion(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the data grid to the search index.  The data document must designate
	 * a primary field using the <code>Data.FEATURE_IS_PRIMARY</code> feature.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @throws RedisDSException Redis data source failure
	 */
	public void add(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int rowCount = aDataGrid.rowCount();
		if (rowCount > 0)
		{
			if (mSearchSchema == null)
			{
				DataDoc searchSchemaDoc = createSchemaDoc(aDataGrid.getColumns(), aDataGrid.getName());
				setSearchSchema(searchSchemaDoc);
			}
			aDataGrid.setColumns(mSearchSchema);
			addSuggestions(aDataGrid);
			if (mDocument == Redis.Document.Hash)
			{
				RedisGrid redisGrid = mRedisDS.createGrid();
				redisGrid.add(aDataGrid);
			}
			else
			{
				RedisJson redisJson = mRedisDS.createJson(mDataSchema);
				redisJson.add(aDataGrid);
			}

			if (! schemaExists())
			{
				saveSchemaDefinition();
				createIndexSaveSchema(mSearchSchema);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the data document as a row to the end of the data grid
	 * in the Redis database and its corresponding search index.
	 * The key name is obtained from the feature
	 * Redis.REDIS_FEATURE_KEY_NAME.
	 *
	 * <b>Note:</b> This method does not add the data document
	 * instance to the end of the data grid instance.  You must
	 * reload the data grid from the Redis database.
	 *
	 * @param aDataGrid Data grid instance
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis operation failure
	 */
	public void add(DataGrid aDataGrid, DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mDocument == Redis.Document.Hash)
		{
			RedisGrid redisGrid = mRedisDS.createGrid();
			redisGrid.add(aDataGrid, aDataDoc);
		}
		else
		{
			RedisJson redisJson = mRedisDS.createJson(mDataSchema);
			redisJson.add(aDataGrid, aDataDoc);
		}
		addSuggestion(aDataDoc);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Adds the data document list to the search index.  The data document must designate
	 * a primary field using the <code>Data.FEATURE_IS_PRIMARY</code> feature.
	 *
	 * @param aDataDocList List of data documents
	 *
	 * @throws RedisDSException Redis data source failure
	 */
	public void add(List<DataDoc> aDataDocList)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "add");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int rowCount = aDataDocList.size();
		if (rowCount > 0)
		{
			if (mSearchSchema == null)
				throw new RedisDSException("Search schema data document has not been assigned.");
			if (mDocument == Redis.Document.Hash)
				throw new RedisDSException("Data document lists can only be stored as JSON documents.");

			RedisJson redisJson = mRedisDS.createJson(mSearchSchema);
			redisJson.add(aDataDocList);

			if (! schemaExists())
			{
				saveSchemaDefinition();
				createIndexSaveSchema(mSearchSchema);
			}
			addSuggestions(aDataDocList);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the data document to the search index.  The data document must designate
	 * a primary field using the <code>Data.FEATURE_IS_PRIMARY</code> feature.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void update(DataDoc aDataDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "update");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		String keyName = aDataDoc.getFeature(Redis.FEATURE_KEY_NAME);
		if (StringUtils.isNotEmpty(keyName))
		{
			if (mDocument == Redis.Document.Hash)
			{
				RedisDoc redisDoc = mRedisDS.createDoc();
				Optional<DataDoc> optDataDoc = redisDoc.getDoc(keyName);
				if (optDataDoc.isPresent())
				{
					DataDoc dbDataDoc = optDataDoc.get();
					redisDoc.update(dbDataDoc, enrichDataDoc(aDataDoc));
				}
			}
			else
			{
				RedisJson redisJson = mRedisDS.createJson(mDataSchema);
				redisJson.update(enrichDataDoc(aDataDoc));
			}
			addSuggestion(aDataDoc);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the documents that
	 * match the <i>DSCriteria</i> in the search index.  In
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
	 * in the search index (based on the offset and limit values).
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGrid query(DSCriteria aDSCriteria, int anOffset, int aLimit)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "query");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		SearchCriteria searchCriteria = new SearchCriteria(mRedisDS,this);
		DataGrid dataGrid = searchCriteria.execute(aDSCriteria, anOffset, aLimit);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the documents that
	 * match the <i>DSCriteria</i> in the search index.
	 *
	 * @param aDSCriteria Data source criteria.
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the search index.
	 *
	 * @throws RedisDSException RediSearch data source exception
	 */
	public DataGrid query(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "query");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		SearchCriteria searchCriteria = new SearchCriteria(mRedisDS,this);
		DataGrid dataGrid = searchCriteria.execute(aDSCriteria);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the field facets that
	 * match the <i>DSCriteria</i> in the search index.
	 *
	 * @param aDSCriteria Data source criteria.
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the search index.
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGrid calculateFacets(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "calculateFacets");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);
		SearchCriteria searchCriteria = new SearchCriteria(mRedisDS,this);
		DataGrid dataGrid = searchCriteria.aggregate(aDSCriteria, false);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Returns a <i>DataGrid</i> representation of the field facets that
	 * match the <i>DSCriteria</i> in the search index that is suitable
	 * for showing in a UI tree grid component.
	 *
	 * @param aDSCriteria Data source criteria.
	 *
	 * @return Data grid representing all rows that match the criteria
	 * in the search index.
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public DataGrid calculateUIFacets(DSCriteria aDSCriteria)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "calculateUIFacets");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);

		SearchCriteria searchCriteria = new SearchCriteria(mRedisDS,this);
		DataGrid dataGrid = searchCriteria.aggregate(aDSCriteria, true);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return dataGrid;
	}

	/**
	 * Captures information regarding the state of the search index into
	 * a data document instance.
	 *
	 * @return Optional data document instance
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public Optional<DataDoc> getInfo()
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "getInfo");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(false);

		DataDoc dataDoc = null;
		String indexName = getIndexName();
		String indexKeyName = mRedisKey.moduleSearch().redisSearchIndex().dataName(indexName).name();
		try
		{
			Map<String, Object> infoMap = mCmdConnection.ftInfo(indexKeyName);
			mRedisDS.saveCommand(appLogger, String.format("FT.INFO %s", mRedisDS.escapeKey(indexKeyName)));
			if ((infoMap != null) && (infoMap.size() > 0))
			{
				dataDoc = new DataDoc(String.format("'%s' Info", indexKeyName));
				for (Map.Entry<String, Object> ie : infoMap.entrySet())
					dataDoc.add(new DataItem.Builder().name(ie.getKey()).title(Data.nameToTitle(ie.getKey())).value(ie.getValue().toString()).build());
			}
		}
		catch (Exception e)
		{
			appLogger.info(String.format("[%s] %s", indexKeyName, e.getMessage()));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return Optional.ofNullable(dataDoc);
	}

	/**
	 * Returns <i>true</i> if the search index exists and <i>false</i> otherwise.
	 *
	 * @return <i>true</i> if the search index exists and <i>false</i> otherwise
	 */
	public boolean indexExists()
	{
		boolean indexExists;

		try
		{
			indexExists = getInfo().isPresent();
		}
		catch (Exception e)
		{
			indexExists = false;
		}

		return indexExists;
	}

	/**
	 * Drops the search index from the Redis database and deletes any
	 * related hash documents (if parameter is true).
	 *
	 * @param aDeleteHashes If true, then document hashes will be deleted too
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void dropIndex(boolean aDeleteHashes)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "dropIndex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ensurePreconditions(true);

		String indexName = getIndexName();
		RedisCore redisCore = mRedisDS.createCore();
		String schemaKeyName = mRedisKey.moduleSearch().redisSearchSchema().dataName(indexName).name();
		if (redisCore.exists(schemaKeyName))
			redisCore.delete(schemaKeyName);
		String indexKeyName = mRedisKey.moduleSearch().redisSearchIndex().dataName(indexName).name();
		if (StringUtils.isEmpty(indexKeyName))
			throw new RedisDSException("search_index: is undefined as a property");

		if (indexExists())
		{
			String msgResponse;

			if (aDeleteHashes)
			{
				msgResponse = mCmdConnection.ftDropIndexDD(indexKeyName);
				mRedisDS.saveCommand(appLogger, String.format("FT.DROP %s DD", mRedisDS.escapeKey(indexKeyName)));
			}
			else
			{
				msgResponse = mCmdConnection.ftDropIndex(indexKeyName);
				mRedisDS.saveCommand(appLogger, String.format("FT.DROP %s", mRedisDS.escapeKey(indexKeyName)));
			}

// Ensure that the RediSearch index name is deleted.

			if (Redis.isResponseOK(msgResponse))
				redisCore.delete(indexKeyName);
		}

// If we created suggestions, then delete them now.

		String suggestKeyName = mRedisKey.moduleSearch().redisSearchSuggest().dataName(indexName).name();
		if (redisCore.exists(suggestKeyName))
			redisCore.delete(suggestKeyName);

// If we created synonyms, then delete them now.

		String synonymKeyName = mRedisKey.moduleSearch().redisSearchSynonym().dataName(indexName).name();
		if (redisCore.exists(synonymKeyName))
			redisCore.delete(synonymKeyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Drops the search index from the Redis database and deletes any
	 * related hash documents (if parameter is true).
	 *
	 * @param aDataGrid Data grid instance (implies hash deletion)
	 *
	 * @throws RedisDSException Redis data source exception
	 */
	public void dropIndex(DataGrid aDataGrid)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "dropIndex");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		dropIndex(true);
		RedisCore redisCore = mRedisDS.createCore();
		RedisKey redisKey = new RedisKey(mRedisDS);
		String gridKeyName = redisKey.moduleCore().redisSortedSet().dataObject(aDataGrid).name();
		if (redisCore.exists(gridKeyName))
			redisCore.delete(gridKeyName);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
