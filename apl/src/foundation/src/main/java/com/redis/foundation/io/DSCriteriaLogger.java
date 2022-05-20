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
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSCriterion;
import com.redis.foundation.ds.DSCriterionEntry;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Map;

/**
 * This class offers convenience methods for logging data
 * source criteria information.
 */
public class DSCriteriaLogger
{
	private String mPrefix;
	private Logger mLogger;
	private DataItemLogger mDataItemLogger;

	public DSCriteriaLogger(Logger aLogger)
	{
		mLogger = aLogger;
		mPrefix = StringUtils.EMPTY;
		mDataItemLogger = new DataItemLogger(aLogger);
	}

	public void setPrefix(String aPrefix)
	{
		if (aPrefix != null)
			mPrefix = String.format("[%s] ", aPrefix);
	}

	public void writeFull(DSCriteria aCriteria)
	{
		if (aCriteria != null)
		{
			int ceIndex = 1;
			DataItem dataItem;
			DSCriterion dsCriterion;
			String nameString, logString;

			mDataItemLogger.writeNV(mPrefix + "Name", aCriteria.getName());
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aCriteria.getFeatures().entrySet())
			{
				nameString = String.format("%s F(%02d) %s", mPrefix, featureOffset++, featureEntry.getKey());
				mDataItemLogger.writeNV(nameString, featureEntry.getValue());
			}
			ArrayList<DSCriterionEntry> dsCriterionEntries = aCriteria.getCriterionEntries();
			int ceCount = dsCriterionEntries.size();
			if (ceCount > 0)
			{
				for (DSCriterionEntry ce : dsCriterionEntries)
				{
					dsCriterion = ce.getCriterion();

					dataItem = dsCriterion.getItem();
					logString = String.format("%s(%d/%d) [%s] %s %s %s", mPrefix, ceIndex++,
											  ceCount, dataItem.getType().name(),dataItem.getName(),
											  Data.operatorToString(ce.getLogicalOperator()),
											  dataItem.getValuesCollapsed());
					mLogger.debug(logString);
				}
			}
			PropertyLogger propertyLogger = new PropertyLogger(mLogger);
			propertyLogger.writeFull(aCriteria.getProperties());
		}
	}

	public void writeSimple(DSCriteria aCriteria)
	{
		if (aCriteria != null)
		{
			int ceIndex = 1;
			String logString;
			DataItem dataItem;
			DSCriterion dsCriterion;

			mDataItemLogger.writeNV(mPrefix + "Name", aCriteria.getName());
			ArrayList<DSCriterionEntry> dsCriterionEntries = aCriteria.getCriterionEntries();
			int ceCount = dsCriterionEntries.size();
			if (ceCount > 0)
			{
				for (DSCriterionEntry ce : dsCriterionEntries)
				{
					dsCriterion = ce.getCriterion();

					dataItem = dsCriterion.getItem();
					logString = String.format("%s(%d/%d) [%s] %s %s %s", mPrefix, ceIndex++,
											  ceCount, dataItem.getType().name(),dataItem.getName(),
											  Data.operatorToString(ce.getLogicalOperator()),
											  dataItem.getValuesCollapsed());
					mLogger.debug(logString);
				}
			}
		}
	}

	public void write(DSCriteria aCriteria)
	{
		writeFull(aCriteria);
	}
}
