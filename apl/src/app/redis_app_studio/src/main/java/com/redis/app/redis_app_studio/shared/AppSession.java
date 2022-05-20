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

package com.redis.app.redis_app_studio.shared;

import com.redis.ds.ds_redis.RedisDSException;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.ds.DGCriteria;
import com.redis.foundation.ds.DS;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The AppSession class manages the creation and recovery of a previously
 * created session.  The logic assumes that the Java EE (servlet) service
 * will manage the creation and tracking of unique session ids.  In addition,
 * this logic will manage the creation and configuration of foundation and
 * Redis data sources for the active application.
 *
 * Each servlet session will have an AppCtx assigned to it.  The AppCtx
 * will have one or more AppResource properties assigned to it using
 * an AppPrefix and dsTitle unique identifier.
 *
 * While this works reasonably well with concurrent applications being
 * created and interacted with, it is possible for application state
 * to spill across different application instances depending on the
 * operations being performed.
 */
public class AppSession
{
	private String mTitle;										// Data source title
	private String mPrefix;										// Application prefix
	private String mStructure;									// Data structure ('flat' or 'hierarchy')
	private String mDataSource;									// Data source (CSV,JSON,Graph)
	private String mDataTarget;									// Memory or Redis database
	private final AppCtx mAppCtx;								// Application context (use properties for AppResources)
	private AppResource mAppResource;							// Active AppResource
	private final SessionContext mSessionContext;				// Servlet session context

	/**
	 * Constructs an application session
	 *
	 * @param aSessionContext Session context instance
	 */
	public AppSession(SessionContext aSessionContext)
	{
		mSessionContext = aSessionContext;
		mAppCtx = mSessionContext.getAppCtx();
		mDataTarget = Constants.APPSES_TARGET_MEMORY;
	}

	protected AppSession(Builder aBuilder)
	{
		mSessionContext = aBuilder.mSessionContext;
		mAppCtx = mSessionContext.getAppCtx();
		mTitle = aBuilder.mTitle;
		mPrefix = aBuilder.mPrefix;
		mDataSource = aBuilder.mDataSource;
		mDataTarget = aBuilder.mDataTarget;
		mStructure = aBuilder.mDataStructure;
	}

	/**
	 * Get the servlet session id.
	 *
	 * @return Session id
	 */
	public String getId()
	{
		return mSessionContext.getId();
	}

	/**
	 * Saves the active application resource for later restoration by an application.
	 *
	 * Note: The save() / restore() methods should be used parts of applications
	 * that utilize the SmartClient grid expansion and grid mass update features.
	 * In both cases, SmartClient limits the contents of the payload being sent
	 * to the App Studio servlet (e.g. missing app prefix, structure and title).
	 * AppViewGrid, Schema and GraphVisualization related classes are impacted.
	 *
	 * @throws DSException Data source exception
	 */
	public void save()
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "save");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (mAppResource == null)
		{
			String msgStr = "The Redis App Studio application resource is null and cannot be saved.";
			appLogger.error(msgStr);
			throw new DSException(msgStr);
		}
		else
			mAppCtx.addProperty(Constants.APPCTX_PROPERTY_DS_RESOURCE, mAppResource);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Returns the last active application resource.
	 *
	 * @return Application resource instance
	 *
	 * @throws DSException Data source exception
	 */
	public AppResource restore()
		throws DSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "restore");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		Object propertyObject = mAppCtx.getProperty(Constants.APPCTX_PROPERTY_DS_RESOURCE);
		if (propertyObject == null)
		{
			String msgStr = "The Redis App Studio session resource has expired - please reload the application";
			appLogger.error(msgStr);
			throw new DSException(msgStr);
		}
		else if (propertyObject instanceof AppResource)
		{
			mAppResource = (AppResource) propertyObject;
			mAppResource.logDebug(appLogger, String.format("%s:Property Restored", getId()));
		}
		else
		{
			String msgStr = String.format("The Redis App Studio session has an unknown property type: %s", propertyObject.toString());
			appLogger.error(msgStr);
			throw new DSException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return mAppResource;
	}

	private String getResourceId()
	{
		return String.format("resource|%s|%s", mPrefix, mTitle);
	}

	/**
	 * Creates and establishes (by loading source and target data sources)
	 * an application resource instance.
	 *
	 * @return Application resource instance
	 *
	 * @throws IOException I/O exception
	 * @throws DSException Data source exception
	 * @throws RedisDSException Redis data source exception
	 */
	public AppResource establish()
		throws IOException, FCException
	{
		AppResource appResource;
		Logger appLogger = mAppCtx.getLogger(this, "establish");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String resourceId = getResourceId();
		Object propertyObject = mAppCtx.getProperty(resourceId);
		if (propertyObject == null)
		{
			appResource = new AppResource(mAppCtx, mPrefix, mStructure, mTitle);
			appResource.create(mDataSource, mDataTarget);
			mAppCtx.addProperty(resourceId, appResource);
			mAppResource = appResource;
			appResource.logDebug(appLogger, String.format("%s:New", getId()));
		}
		else
		{
			appResource = (AppResource) propertyObject;
			appResource.refreshData(mDataTarget);
			mAppResource = appResource;
			appResource.logDebug(appLogger, String.format("%s:Context Restored", getId()));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return appResource;
	}

	/**
	 * Creates and establishes (by loading source and target data sources)
	 * an application resource instance.
	 *
	 * @param aDataTarget Target data source name
	 *
	 * @return Application resource instance
	 *
	 * @throws IOException I/O exception
	 * @throws DSException Data source exception
	 * @throws RedisDSException Redis data source exception
	 */
	public AppResource establish(String aDataTarget)
		throws IOException, FCException
	{
		mDataTarget = aDataTarget;
		return establish();
	}

	/***
	 * The Builder class provides utility methods for constructing application sessions.
	 */
	public static class Builder
	{
		private String mTitle = "Undefined";
		private String mPrefix = "Undefined";
		private String mDataSource = "Undefined";
		private String mDataTarget = "Undefined";
		private String mDataStructure = "Undefined";
		private SessionContext mSessionContext;

		/**
		 * Assigns a session context.
		 *
		 * @param aSessionContext Session context instance
		 *
		 * @return Builder instance
		 */
		public Builder context(SessionContext aSessionContext)
		{
			mSessionContext = aSessionContext;
			return this;
		}

		/**
		 * Assigns a data source.
		 *
		 * @param aDataSource Data source string
		 *
		 * @return Builder instance
		 */
		public Builder dataSource(String aDataSource)
		{
			mDataSource = aDataSource;
			return this;
		}

		/**
		 * Convenience method for data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder gridCSV()
		{
			return dataSource(Constants.APPSES_SOURCE_FLAT_GRID_CSV);
		}

		/**
		 * Convenience method for data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder gridJson()
		{
			return dataSource(Constants.APPSES_SOURCE_HIER_GRID_JSON);
		}

		/**
		 * Convenience method for data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder gridTimeSeries()
		{
			return dataSource(Constants.APPSES_SOURCE_FLAT_TIME_SERIES);
		}

		/**
		 * Convenience method for data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder graphCSV()
		{
			return dataSource(Constants.APPSES_SOURCE_HIER_GRAPH_CSV);
		}

		/**
		 * Assigns a data target.
		 *
		 * @param aDataTarget Data target string
		 *
		 * @return Builder instance
		 */
		public Builder dataTarget(String aDataTarget)
		{
			mDataTarget = aDataTarget;
			return this;
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetMemory()
		{
			return dataTarget(Constants.APPSES_TARGET_MEMORY);
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetRedisCore()
		{
			return dataTarget(Constants.APPSES_TARGET_REDIS_CORE);
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetRedisJson()
		{
			return dataTarget(Constants.APPSES_TARGET_REDIS_JSON);
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetRediSearchHash()
		{
			return dataTarget(Constants.APPRES_TARGET_REDIS_SEARCH_HASH);
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetRediSearchJson()
		{
			return dataTarget(Constants.APPRES_TARGET_REDIS_SEARCH_JSON);
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetRedisGraph()
		{
			return dataTarget(Constants.APPSES_TARGET_REDIS_GRAPH);
		}

		/**
		 * Convenience method for target data source assignment.
		 *
		 * @return Builder instance
		 */
		public Builder targetRedisTimeSeries()
		{
			return dataTarget(Constants.APPSES_TARGET_REDIS_TIMESERIES);
		}

		/**
		 * Assigns a data structure.
		 *
		 * @param aDataStructure Data structure string
		 *
		 * @return Builder instance
		 */
		public Builder dataStructure(String aDataStructure)
		{
			mDataStructure = aDataStructure;
			return this;
		}

		/**
		 * Assigns an application prefix.
		 *
		 * @param aPrefix Application prefix string
		 *
		 * @return Builder instance
		 */
		public Builder prefix(String aPrefix)
		{
			mPrefix = aPrefix;
			return this;
		}

		/**
		 * Assigns a data source title
		 *
		 * @param aTitle Data source title
		 *
		 * @return Builder instance
		 */
		public Builder title(String aTitle)
		{
			mTitle = aTitle;
			return this;
		}

		/**
		 * Assigns a data source criteria.
		 *
		 * @param aCriteria Data source criteria
		 *
		 * @return Builder instance
		 */
		public Builder criteria(DSCriteria aCriteria)
		{
			mTitle = DS.titleFromCriteria(aCriteria);
			mPrefix = DS.appPrefixFromCriteria(aCriteria);
			mDataStructure = DS.structureFromCriteria(aCriteria);
			return this;
		}

		/**
		 * Assigns a data source criteria.
		 *
		 * @param aCriteria Data source criteria
		 *
		 * @return Builder instance
		 */
		public Builder criteria(DGCriteria aCriteria)
		{
			mTitle = DS.titleFromCriteria(aCriteria);
			mPrefix = DS.appPrefixFromCriteria(aCriteria);
			mDataStructure = DS.structureFromCriteria(aCriteria);
			return this;
		}

		/**
		 * Assigns a document map of fields.
		 *
		 * @param aMap Map of document fields
		 *
		 * @return Builder instance
		 */
		@SuppressWarnings({"WhileLoopReplaceableByForEach", "rawtypes"})
		public Builder document(Map aMap)
		{
			String keyName, keyValue;

			if (aMap != null)
			{
				Iterator mapIterator = aMap.entrySet().iterator();
				while (mapIterator.hasNext())
				{
					Map.Entry mapEntry = (Map.Entry) mapIterator.next();
					keyName = mapEntry.getKey().toString();
					if (mapEntry.getValue() != null)
					{
						if (StringUtils.equals(keyName, Constants.RAS_CONTEXT_FIELD_NAME))
						{
							keyValue = mapEntry.getValue().toString();
							// Expecting: appPrefix|dataStructure|dsTitle from "Application.ts"
							List<String> valueList = StrUtl.expandToList(keyValue, StrUtl.CHAR_PIPE);
							if (valueList.size() == 3)
							{
								mPrefix = valueList.get(0);
								mDataStructure = valueList.get(1);
								mTitle = valueList.get(2);
							}
						}
					}
				}
			}
			return this;
		}

		/**
		 * Builds an application session instance.
		 *
		 * @return Application session instance
		 */
		public AppSession build()
		{
			return new AppSession(this);
		}
	}
}
