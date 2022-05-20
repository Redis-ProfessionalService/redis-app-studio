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

import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import org.slf4j.Logger;

import java.util.Map;

/**
 * This class offers convenience methods for logging data
 * document information.
 */
public class DataDocLogger
{
	private Logger mLogger;
	private DataItemLogger mDataItemLogger;

	public DataDocLogger(Logger aLogger)
	{
		mLogger = aLogger;
		mDataItemLogger = new DataItemLogger(aLogger);
	}

	public void writeFull(DataDoc aDataDoc)
	{
		if (aDataDoc != null)
		{
			mDataItemLogger.writeNV("Name", aDataDoc.getName());
			mDataItemLogger.writeNV("Title", aDataDoc.getTitle());

			String nameString;
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aDataDoc.getFeatures().entrySet())
			{
				nameString = String.format(" F[%02d] %s", featureOffset++, featureEntry.getKey());
				mDataItemLogger.writeNV(nameString, featureEntry.getValue());
			}
			for (DataItem dataItem : aDataDoc.getItems())
				mDataItemLogger.writeFull(dataItem);
			PropertyLogger propertyLogger = new PropertyLogger(mLogger);
			propertyLogger.writeFull(aDataDoc.getProperties());
		}
	}

	public void writeSimple(DataDoc aDataDoc)
	{
		if (aDataDoc != null)
		{
			mDataItemLogger.writeNV("Name", aDataDoc.getName());
			mDataItemLogger.writeNV("Title", aDataDoc.getTitle());
			for (DataItem dataItem : aDataDoc.getItems())
				mDataItemLogger.writeSimple(dataItem);
		}
	}

	public void write(DataDoc aDataDoc)
	{
		writeFull(aDataDoc);
	}
}
