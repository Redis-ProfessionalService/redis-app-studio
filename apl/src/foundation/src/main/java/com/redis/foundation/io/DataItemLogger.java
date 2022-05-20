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

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataItem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Map;

/**
 * This class offers convenience methods for logging data
 * item information.
 */
public class DataItemLogger
{
	protected Logger mLogger;

	public DataItemLogger(Logger aLogger)
	{
		mLogger = aLogger;
	}

	public void writeNV(String aName, String aValue)
	{
		if (aValue == null)
			aValue = StringUtils.EMPTY;
		mLogger.debug(aName + ": " + aValue);
	}

	public void writeNV(String aName, int aValue)
	{
		if (aValue > 0)
			writeNV(aName, Integer.toString(aValue));
	}

	public void writeNV(String aName, long aValue)
	{
		if (aValue > 0)
			writeNV(aName, Long.toString(aValue));
	}

	public void writeFull(DataItem aDataItem)
	{
		if (aDataItem != null)
		{
			writeNV("Name", aDataItem.getName());
			writeNV("Type", Data.typeToString(aDataItem.getType()));
			writeNV("Title", aDataItem.getTitle());
			writeNV("Value", aDataItem.getValuesCollapsed());
			if (aDataItem.getSortOrder() != Data.Order.UNDEFINED)
				writeNV("Sort Order", aDataItem.getSortOrder().name());
			writeNV("Display Size", aDataItem.getDisplaySize());
			writeNV("Default Value", aDataItem.getDefaultValue());

			String nameString;
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aDataItem.getFeatures().entrySet())
			{
				nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
				writeNV(nameString, featureEntry.getValue());
			}
		}
	}

	public void writeSimple(DataItem aDataItem)
	{
		if (aDataItem != null)
			mLogger.debug(aDataItem.toString());
	}

	public void write(DataItem aDataItem)
	{
		writeFull(aDataItem);
	}
}
