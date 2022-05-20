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
import com.redis.foundation.data.DataRange;
import com.redis.foundation.std.IOXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Element;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * The DataRangeXML class provides XML helper methods.
 */
public class DataRangeXML
{
	private final String RANGE_NODE_NAME = "Range";

	/**
	 * Default constructor.
	 */
	public DataRangeXML()
	{
	}

	public boolean isRangeFeature(String aFeatureName)
	{
		return StringUtils.startsWith(aFeatureName, "range");
	}

	public void saveAttr(PrintWriter aPW, DataRange aDataRange)
		throws IOException
	{
		IOXML.writeAttrNameValue(aPW, "rangeType", Data.typeToString(aDataRange.getType()));
		IOXML.writeAttrNameValue(aPW, "rangeDelimiterChar", aDataRange.getDelimiterChar());
		if (aDataRange.getType() == Data.Type.Text)
		{
			String singleString = StrUtl.collapseToSingle(aDataRange.getItems(), aDataRange.getDelimiterChar());
			IOXML.writeAttrNameValue(aPW, "rangeValues", singleString);
		}
		else
		{
			IOXML.writeAttrNameValue(aPW, "rangeMin", aDataRange.getMinString());
			IOXML.writeAttrNameValue(aPW, "rangeMax", aDataRange.getMaxString());
		}
	}

	public DataRange load(Element anElement)
		throws IOException
	{
		DataRange dataRange;

		String attrValue = anElement.getAttribute("rangeType");
		if (StringUtils.isNotEmpty(attrValue))
		{
			Data.Type rangeType = Data.stringToType(attrValue);
			if (rangeType == Data.Type.Text)
			{
				dataRange = new DataRange();
				String delimiterString = anElement.getAttribute("rangeDelimiterChar");
				if (StringUtils.isNotEmpty(delimiterString))
					dataRange.setDelimiterChar(delimiterString);
				String rangeValues = anElement.getAttribute("rangeValues");
				if (StringUtils.isNotEmpty(rangeValues))
					dataRange.setItems(StrUtl.expandToList(rangeValues, dataRange.getDelimiterChar()));
			}
			else
			{
				String minValue = anElement.getAttribute("rangeMin");
				String maxValue = anElement.getAttribute("rangeMax");
				switch (rangeType)
				{
					case Long:
						dataRange = new DataRange(Data.createLong(minValue), Data.createLong(maxValue));
						break;
					case Integer:
						dataRange = new DataRange(Data.createInt(minValue), Data.createInt(maxValue));
						break;
					case Double:
						dataRange = new DataRange(Data.createDouble(minValue), Data.createDouble(maxValue));
						break;
					case Date:
					case DateTime:
						dataRange = new DataRange(Data.createDate(minValue), Data.createDate(maxValue));
						break;
					default:
						dataRange = null;
						break;
				}
			}
		}
		else
			dataRange = null;

		return dataRange;
	}
}
