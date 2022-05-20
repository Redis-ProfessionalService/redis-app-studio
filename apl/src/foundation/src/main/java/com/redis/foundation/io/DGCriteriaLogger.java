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

import com.redis.foundation.ds.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Map;

/**
 * This class offers convenience methods for logging data
 * graph criteria information.
 */
public class DGCriteriaLogger
{
	private String mPrefix;
	private final Logger mLogger;
	private final DataItemLogger mDataItemLogger;
	private final DSCriteriaLogger mDSCriteriaLogger;

	public DGCriteriaLogger(Logger aLogger)
	{
		mLogger = aLogger;
		mPrefix = StringUtils.EMPTY;
		mDataItemLogger = new DataItemLogger(aLogger);
		mDSCriteriaLogger = new DSCriteriaLogger(aLogger);
	}

	public void setPrefix(String aPrefix)
	{
		if (aPrefix != null)
			mPrefix = String.format("[%s] ", aPrefix);
	}

	public void writeFull(DGCriteria aCriteria)
	{
		if (aCriteria != null)
		{
			int dgcIndex = 1;
			DSCriteria dsCriteria;
			String nameString, logString;

			mDataItemLogger.writeNV(mPrefix + "Name", aCriteria.getName());
			int featureOffset = 0;
			for (Map.Entry<String, String> featureEntry : aCriteria.getFeatures().entrySet())
			{
				nameString = String.format("%s F(%02d) %s", mPrefix, featureOffset++, featureEntry.getKey());
				mDataItemLogger.writeNV(nameString, featureEntry.getValue());
			}
			ArrayList<DGCriterion> dgCriterions = aCriteria.getCriterions();
			int dgCount = dgCriterions.size();
			if (dgCount > 0)
			{
				for (DGCriterion dgc : dgCriterions)
				{
					logString = String.format("%s(%d/%d) %s %s %d %s", mPrefix, dgcIndex++,
											  dgCount, dgc.getObject().name(),
											  dgc.getIdentifier(),
											  dgc.getHops(), dgc.getSchemaName());
					mLogger.debug(logString);
					dsCriteria = dgc.getCriteria();
					mDSCriteriaLogger.writeSimple(dsCriteria);
				}
			}
			PropertyLogger propertyLogger = new PropertyLogger(mLogger);
			propertyLogger.writeFull(aCriteria.getProperties());
		}
	}

	public void writeSimple(DGCriteria aCriteria)
	{
		if (aCriteria != null)
		{
			int dgcIndex = 1;
			String logString;
			DSCriteria dsCriteria;

			mDataItemLogger.writeNV(mPrefix + "Name", aCriteria.getName());
			ArrayList<DGCriterion> dgCriterions = aCriteria.getCriterions();
			int dgCount = dgCriterions.size();
			if (dgCount > 0)
			{
				for (DGCriterion dgc : dgCriterions)
				{
					logString = String.format("%s(%d/%d) %s %s %d %s", mPrefix, dgcIndex++,
											  dgCount, dgc.getObject().name(),
											  dgc.getIdentifier(),
											  dgc.getHops(), dgc.getSchemaName());
					mLogger.debug(logString);
					dsCriteria = dgc.getCriteria();
					mDSCriteriaLogger.writeSimple(dsCriteria);
				}
			}
		}
	}

	public void write(DGCriteria aCriteria)
	{
		writeFull(aCriteria);
	}
}
