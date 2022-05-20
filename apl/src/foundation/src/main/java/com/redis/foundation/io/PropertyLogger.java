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

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * This class offers convenience methods for logging data
 * property information.
 */
public class PropertyLogger
{
	private Logger mLogger;
	private DataItemLogger mDataItemLogger;

	public PropertyLogger(Logger aLogger)
	{
		mLogger = aLogger;
		mDataItemLogger = new DataItemLogger(aLogger);
	}

	public void writeSimple(HashMap<String, Object> aProperties)
	{
		if (aProperties != null)
		{
			for (Map.Entry<String, Object> propertyEntry : aProperties.entrySet())
				mDataItemLogger.writeNV(propertyEntry.getKey(), propertyEntry.getValue().toString());
		}
	}

	public void writeFull(HashMap<String, Object> aProperties)
	{
		writeSimple(aProperties);
	}

	public void write(HashMap<String, Object> aProperties)
	{
		writeSimple(aProperties);
	}
}
