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

package com.redis.foundation.app;

import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

/**
 * The Configuration Manager provides a collection of convenience
 * methods for managing application properties.
 *
 * @since 1.0
 * @author Al Cole
 */
public class CfgMgr
{
	private AppCtx mAppCtx;
	private String mPrefix = StringUtils.EMPTY;

	/**
	 * Constructor that assigns a property prefix.
	 *
	 * @param anAppCtx Application context instance.
	 * @param aPrefix Configuration property prefix string.
	 */
	public CfgMgr(AppCtx anAppCtx, String aPrefix)
	{
		mAppCtx = anAppCtx;
		mPrefix = aPrefix;
	}

	/**
	 * Returns the configuration property prefix string.
	 *
	 * @return Property prefix string.
	 */
	public String getPrefix()
	{
		return mPrefix;
	}

	/**
	 * Assigns the configuration property prefix to the document Fusion service.
	 *
	 * @param aPrefix Property prefix.
	 */
	public void setCfgPropertyPrefix(String aPrefix)
	{
		mPrefix = aPrefix;
	}

	/**
	 * Convenience method that determines if a property has been
	 * defined in the application context.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return <i>true</i> if it exists and <i>false</i> otherwise
	 */
	public boolean isAssigned(String aSuffix)
	{
		return StringUtils.isNotEmpty(getString(aSuffix));
	}

	/**
	 * Convenience method that returns the value of an application
	 * context configuration property using the concatenation of
	 * the property prefix and suffix values.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return Matching property value.
	 */
	public String getString(String aSuffix)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getString(propertyName);
	}

	/**
	 * Convenience method that returns the value of an application
	 * context configuration property using the concatenation of
	 * the property prefix and suffix values.  If the property is
	 * not found, then the default value parameter will be returned.
	 *
	 * @param aSuffix Property name suffix.
	 * @param aDefaultValue Default value.
	 *
	 * @return Matching property value or the default value.
	 */
	public String getString(String aSuffix, String aDefaultValue)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getString(propertyName, aDefaultValue);
	}

	/**
	 * Returns integer value for the property name identified.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return Value of the property.
	 */
	public int getInteger(String aSuffix)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getInt(propertyName);
	}

	/**
	 * Returns integer value for the property name identified
	 * or the default value (if unmatched).
	 *
	 * @param aSuffix Property name suffix.
	 * @param aDefaultValue Default value to return if property
	 *                      name is not matched.
	 *
	 * @return Value of the property.
	 */
	public int getInteger(String aSuffix, int aDefaultValue)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getInt(propertyName, aDefaultValue);
	}

	/**
	 * Returns long value for the property name identified.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return Value of the property.
	 */
	public long getLong(String aSuffix)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getLong(propertyName);
	}

	/**
	 * Returns long value for the property name identified
	 * or the default value (if unmatched).
	 *
	 * @param aSuffix Property name suffix.
	 * @param aDefaultValue Default value to return if property
	 *                      name is not matched.
	 *
	 * @return Value of the property.
	 */
	public long getLong(String aSuffix, long aDefaultValue)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getLong(propertyName, aDefaultValue);
	}

	/**
	 * Returns a floating point value for the property name identified
	 * or the default value (if unmatched).
	 *
	 * @param aSuffix Property name suffix.
	 * @param aDefaultValue Default value to return if property
	 *                      name is not matched.
	 *
	 * @return Value of the property.
	 */
	public float getFloat(String aSuffix, float aDefaultValue)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getFloat(propertyName, aDefaultValue);
	}

	/**
	 * Returns a double value for the property name identified
	 * or the default value (if unmatched).
	 *
	 * @param aSuffix Property name suffix.
	 * @param aDefaultValue Default value to return if property
	 *                      name is not matched.
	 *
	 * @return Value of the property.
	 */
	public double getDouble(String aSuffix, double aDefaultValue)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getDouble(propertyName, aDefaultValue);
	}

	/**
	 * Convenience method that returns the value of an application
	 * context configuration property using the concatenation of
	 * the property prefix and suffix values.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return Matching property value.
	 */
	public String[] getCfgStringArray(String aSuffix)
	{
		String propertyName;

		if (StringUtils.startsWith(aSuffix, "."))
			propertyName = mPrefix + aSuffix;
		else
			propertyName = mPrefix + "." + aSuffix;

		return mAppCtx.getStringArray(propertyName);
	}

	/**
	 * Returns <i>true</i> if the application context configuration
	 * property value evaluates to <i>true</i>.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isStringTrue(String aSuffix)
	{
		String propertyValue = getString(aSuffix);

		return StrUtl.stringToBoolean(propertyValue);
	}

	/**
	 * Returns <i>true</i> if the application context configuration
	 * property value evaluates to <i>false</i>.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isStringFalse(String aSuffix)
	{
		return !isStringTrue(aSuffix);
	}
}
