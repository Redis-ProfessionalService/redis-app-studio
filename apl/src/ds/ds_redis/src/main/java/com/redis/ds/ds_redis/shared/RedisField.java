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

package com.redis.ds.ds_redis.shared;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Optional;

/**
 * The RedisField class handles Redis hash field name generation
 * and restoration operations.  The Redis operations executed via the
 * Jedis programmatic library.
 *
 * You can instantiate this class from the parent
 * {@link com.redis.ds.ds_redis.RedisDS} class.
 *
 * Field Name Format - "ItemName:DataType:ValueType:ValueFormat"
 *
 * @see <a href="https://redis.io/commands">OSS Redis Commands</a>
 * @see <a href="https://github.com/redis/jedis">Jedis GitHub site</a>
 * @see <a href="https://www.baeldung.com/jedis-java-redis-client-library">Intro to Jedis – the Java Redis Client Library</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RedisField
{
	private String mFieldName;
	private DataItem mDataItem;
	private final AppCtx mAppCtx;
	protected final RedisDS mRedisDS;

	/**
	 * Constructor accepts a Redis data source parameter and initializes
	 * the field objects accordingly.
	 *
	 * @param aRedisDS Redis data source instance
	 */
	public RedisField(RedisDS aRedisDS)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
	}

	/**
	 * Constructor accepts a Redis data source parameter and field name.
	 *
	 * @param aRedisDS   Redis data source instance
	 * @param aFieldName Field name
	 */
	public RedisField(RedisDS aRedisDS, String aFieldName)
	{
		mRedisDS = aRedisDS;
		mAppCtx = aRedisDS.getAppCtx();
		parseAssign(aFieldName);
	}

	@SuppressWarnings("EnhancedSwitchMigration")
	private String dataTypeToString(Data.Type aType)
	{
		switch (aType)
		{
			case Date:
				return Redis.KEY_DATA_TYPE_DATE;
			case Long:
				return Redis.KEY_DATA_TYPE_LONG;
			case Float:
				return Redis.KEY_DATA_TYPE_FLOAT;
			case Double:
				return Redis.KEY_DATA_TYPE_DOUBLE;
			case Boolean:
				return Redis.KEY_DATA_TYPE_BOOLEAN;
			case Integer:
				return Redis.KEY_DATA_TYPE_INTEGER;
			case DateTime:
				return Redis.KEY_DATA_TYPE_DATETIME;
			default:
				return Redis.KEY_DATA_TYPE_TEXT;
		}
	}

	@SuppressWarnings("EnhancedSwitchMigration")
	private Data.Type stringToDataType(String aDataType)
	{
		switch (aDataType)
		{
			case Redis.KEY_DATA_TYPE_DATE:
				return Data.Type.Date;
			case Redis.KEY_DATA_TYPE_LONG:
				return Data.Type.Long;
			case Redis.KEY_DATA_TYPE_FLOAT:
				return Data.Type.Float;
			case Redis.KEY_DATA_TYPE_DOUBLE:
				return Data.Type.Double;
			case Redis.KEY_DATA_TYPE_BOOLEAN:
				return Data.Type.Boolean;
			case Redis.KEY_DATA_TYPE_INTEGER:
				return Data.Type.Integer;
			case Redis.KEY_DATA_TYPE_DATETIME:
				return Data.Type.DateTime;
			default:
				return Data.Type.Text;
		}
	}

	/**
	 * Generates a field name based on a data item instance.
	 *
	 * Field Name Format - "ItemName:DataType:ValueType:ValueFormat"
	 *
	 * @param aDataItem Data item instance
	 *
	 * @return Field name string
	 */
	public String name(DataItem aDataItem)
	{
		String fieldName = StringUtils.remove(aDataItem.getName(), StrUtl.CHAR_COLON);
		String dataType, valueType, valueFormat;
		if (aDataItem.isFeatureTrue(Redis.FEATURE_IS_KEY))
			dataType = Redis.KEY_DATA_TYPE_KEY;
		else
			dataType = dataTypeToString(aDataItem.getType());
		if (aDataItem.isMultiValue())
			valueType = Redis.KEY_VALUE_MULTI;
		else
			valueType = Redis.KEY_VALUE_SINGLE;
		if (aDataItem.isFeatureTrue(Data.FEATURE_IS_SECRET))
			valueFormat = Redis.KEY_VALUE_ENCRYPTED;
		else
			valueFormat = Redis.KEY_VALUE_PLAIN;
		mFieldName = String.format("%s:%s:%s:%s", fieldName, dataType, valueType, valueFormat);

		return mFieldName;
	}

	/**
	 * Parse the field name into an internally managed data item instance.
	 *
	 * Field Name Format - "ItemName:DataType:ValueType:ValueFormat"
	 *                      0        1        2         3
	 *
	 * @param aFieldName Field name
	 */
	private void parseAssign(String aFieldName)
	{
		mFieldName = aFieldName;
		int separatorCount = StringUtils.countMatches(aFieldName, StrUtl.CHAR_COLON);
		if (separatorCount > 2)
		{
			int offset = aFieldName.indexOf(StrUtl.CHAR_COLON);
			String[] fieldParameters = aFieldName.split(String.valueOf(StrUtl.CHAR_COLON));
			String fieldName = fieldParameters[0].substring(0, offset);;
			if (StringUtils.equals(fieldParameters[1], Redis.KEY_DATA_TYPE_KEY))
			{
				mDataItem = new DataItem.Builder().name(fieldName).title(Data.nameToTitle(aFieldName)).build();
				mDataItem.enableFeature(Redis.FEATURE_IS_KEY);
			}
			else
			{
				mDataItem = new DataItem.Builder().type(stringToDataType(fieldParameters[1])).name(fieldName).title(Data.nameToTitle(fieldName)).build();
				if (StringUtils.equals(fieldParameters[2], Redis.KEY_VALUE_MULTI))
					mDataItem.enableFeature(Data.FEATURE_IS_MULTIVALUE);
				if (StringUtils.equals(fieldParameters[3], Redis.KEY_VALUE_ENCRYPTED))
					mDataItem.enableFeature(Data.FEATURE_IS_SECRET);
			}
		}
		else
			mDataItem = new DataItem.Builder().name(aFieldName).title(Data.nameToTitle(aFieldName)).build();
	}

	/**
	 * Resets the state of the field members prior to new field name assignment.
	 */
	public void reset()
	{
		mDataItem = null;
		mFieldName = StringUtils.EMPTY;
	}

	/**
	 * Resets the state of the field members prior to the assignment of a new
	 * field name.
	 *
	 * @param aFieldName Field name
	 */
	public void resetParseAssign(String aFieldName)
	{
		reset();
		parseAssign(aFieldName);
	}

	/**
	 * Creates a data item instance from the field name.  You must
	 * ensure you provide a field name with the constructor prior
	 * to invoking this method.
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> toDataItem()
	{
		DataItem dataItem = null;
		Logger appLogger = mAppCtx.getLogger(this, "toDataItem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((StringUtils.isNotEmpty(mFieldName)) && (mDataItem != null))
			dataItem = mDataItem;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		return Optional.ofNullable(dataItem);
	}

	/**
	 * Creates a data item instance from the field name.  You must
	 * ensure you provide a field name with the constructor prior
	 * to invoking this method.
	 *
	 * @param aFieldName Field name
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> toDataItem(String aFieldName)
	{
		resetParseAssign(aFieldName);
		return toDataItem();
	}
}