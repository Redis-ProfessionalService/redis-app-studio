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

package com.redis.foundation.ds;

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * A DSCriterion is responsible for encapsulating a query item, its operator
 * and one or more values.  It is exclusively referenced from within a
 * {@link DSCriteria} object.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DSCriterion
{
	private DataItem mItem;
	private boolean mIsCaseInsensitive;
	private HashMap<String, String> mFeatures;
	private Data.Operator mOperator = Data.Operator.UNDEFINED;

	/**
	 * Copy constructor
	 *
	 * @param aDSCriterion Data source criterion instance.
	 */
	public DSCriterion(final DSCriterion aDSCriterion)
	{
		if (aDSCriterion != null)
		{
			this.mOperator = aDSCriterion.mOperator;
			this.mItem = new DataItem(aDSCriterion.mItem);
			setCaseInsensitiveFlag(this.mIsCaseInsensitive);
			this.mFeatures = new HashMap<String, String>(aDSCriterion.getFeatures());
		}
	}

	/**
	 * Constructor that accepts a {@link DataItem} and item
	 * operator and initializes the DSCriterion accordingly.
	 *
	 * @param anItem Data item.
	 * @param anOperator Criterion operator.
	 */
	public DSCriterion(DataItem anItem, Data.Operator anOperator)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem(anItem);
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts an item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, String aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts an item name, type, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aType Item type.
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(Data.Type aType, String aName, Data.Operator anOperator, String aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().type(aType).name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts an item name, operator and an array of values
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aType Item type.
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues An array of values.
	 */
	public DSCriterion(Data.Type aType, String aName, Data.Operator anOperator, String... aValues)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().type(aType).name(aName).values(aValues).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts an item name, operator and an array of values
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues An array of values.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, String... aValues)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).values(aValues).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and an array of values
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues An array of values.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, ArrayList<String> aValues)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).values(aValues).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, int aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and an array of values
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues An array of values.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, int... aValues)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).values(aValues).build();
	}

	/**
	 * Constructor accepts a item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, long aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, float aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, double aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, Date aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and a single value
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName      Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue     Item value.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, boolean aValue)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).value(aValue).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor accepts a item name, operator and an array of values
	 * and initializes the DSCriterion accordingly.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues An array of values.
	 */
	public DSCriterion(String aName, Data.Operator anOperator, Date... aValues)
	{
		mOperator = anOperator;
		mIsCaseInsensitive = false;
		mItem = new DataItem.Builder().name(aName).values(aValues).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Constructor dedicated to a geolocation query where the latitude,
	 * longitude, radius and distance unit are used to initialize a
	 * DSCriterion.
	 *
	 * @param aName Item name
	 * @param aLatitude Latitude of center position
	 * @param aLongitude Longitude of center position
	 * @param aRadius Radius distance from center position
	 * @param aUnit Unit of measure
	 */
	public DSCriterion(String aName, double aLatitude, double aLongitude,
					   double aRadius, String aUnit)
	{
		mIsCaseInsensitive = false;
		mOperator = Data.Operator.GEO_LOCATION;
		String[] geoValues = new String[4];
		geoValues[0] = Double.toString(aLatitude);
		geoValues[1] = Double.toString(aLongitude);
		geoValues[2] = Double.toString(aRadius);
		geoValues[3] = aUnit;
		mItem = new DataItem.Builder().name(aName).values(geoValues).build();
		mFeatures = new HashMap<String, String>();
	}

	/**
	 * Returns a string summary representation of a criterion.
	 *
	 * @return String summary representation of a criterion.
	 */
	@Override
	public String toString()
	{
		return String.format("%s %s %s", mItem.getName(), mOperator.name(), mItem.getValue());
	}

	/**
	 * Returns the logical (a.k.a. criterion) operator.
	 *
	 * @return Logical operator.
	 */
	public Data.Operator getLogicalOperator()
	{
		return mOperator;
	}

	/**
	 * Returns the item type.
	 *
	 * @return Item type.
	 */
	public Data.Type getType()
	{
		return mItem.getType();
	}

	/**
	 * Returns the name of the item.
	 *
	 * @return Item name.
	 */
	public String getName()
	{
		return mItem.getName();
	}

	/**
	 * Returns the value of the item.
	 *
	 * @return Item value.
	 */
	public String getValue()
	{
		return mItem.getValue();
	}

	/**
	 * Assigns a data formatting string for a criterian.  The data format
	 * is referenced when data is being parsed or presented during a console
	 * display. The format is based on DecimalFormat (numbers) and
	 * SimpleDateFormat (date/time). Therefore, your assignment is type
	 * dependent.
	 *
	 * @see <a href="https://www.baeldung.com/java-number-formatting">Number Formatting in Java</a>
	 * @see <a href="http://tutorials.jenkov.com/java-internationalization/simpledateformat.html">Java SimpleDateFormat</a>
	 * @see <a href="https://www.baeldung.com/java-simple-date-format">A Guide to SimpleDateFormat</a>
	 *
	 * @param aDataFormat Data format string based on data type
	 */
	public void setDataFormat(String aDataFormat)
	{
		mItem.setDataFormat(aDataFormat);
	}

	/**
	 * Returns the value of the item as a Date.
	 *
	 * @return Item value as a Date.
	 */
	public Date getValueAsDate()
	{
		return mItem.getValueAsDate();
	}

	/**
	 * Returns the name and value of the criterion as a
	 * {@link DataItem}.
	 *
	 * @return Data item.
	 */
	public DataItem getItem()
	{
		return mItem;
	}

	/**
	 * Returns <i>true</i> if the criterion is case sensitive.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isCaseInsensitive()
	{
		return mIsCaseInsensitive;
	}

	/**
	 * Assigns a boolean flag indicating whether the criterion
	 * should be case sensitive.
	 *
	 * @param aFlag Boolean flag.
	 */
	public void setCaseInsensitiveFlag(boolean aFlag)
	{
		mIsCaseInsensitive = aFlag;
	}

	/**
	 * Returns <i>true</i> if the criterion contains multiple values.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isMultiValue()
	{
		return mItem.isMultiValue();
	}

	/**
	 * Returns the array of values associated with this criterion.
	 *
	 * @return Array of values.
	 */
	public final ArrayList<String> getValues()
	{
		return mItem.getValues();
	}

	/**
	 * Adds the value to the collection of criterion values.
	 *
	 * @param aValue Item value.
	 */
	public void addValue(String aValue)
	{
		mItem.addValue(aValue);
	}

	/**
	 * Returns the count of values associated with criterion.
	 *
	 * @return Count of values.
	 */
	public int count()
	{
		return mItem.valueCount();
	}

	/**
	 * Add a unique feature to this criteria.  A feature enhances the core
	 * capability of the criteria.  Standard features are listed below.
	 * <ul>
	 *     <li>Data.FEATURE_OPERATION_NAME</li>
	 * </ul>
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, String aValue)
	{
		mFeatures.put(aName, aValue);
	}

	/**
	 * Add a unique feature to this criterion.  A feature enhances the core
	 * capability of the criteria.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, int aValue)
	{
		addFeature(aName, Integer.toString(aValue));
	}

	/**
	 * Add a unique feature to this criterion.  A feature enhances the core
	 * capability of the criteria.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, boolean aValue)
	{
		addFeature(aName, StrUtl.booleanToString(aValue));
	}

	/**
	 * Enabling the feature will add the name and assign it a
	 * value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of the feature.
	 */
	public void enableFeature(String aName)
	{
		mFeatures.put(aName, StrUtl.STRING_TRUE);
	}

	/**
	 * Disabling a feature will remove its name and value
	 * from the internal list.
	 *
	 * @param aName Name of feature.
	 */
	public void disableFeature(String aName)
	{
		mFeatures.remove(aName);
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and assigned a value.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureAssigned(String aName)
	{
		return (getFeature(aName) != null);
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and assigned a value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureTrue(String aName)
	{
		return StrUtl.stringToBoolean(mFeatures.get(aName));
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and not assigned a value of <i>StrUtl.STRING_TRUE</i>.
	 *
	 * @param aName Name of feature.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureFalse(String aName)
	{
		return !StrUtl.stringToBoolean(mFeatures.get(aName));
	}

	/**
	 * Returns <i>true</i> if the feature was previously
	 * added and its value matches the one provided as a
	 * parameter.
	 *
	 * @param aName Feature name.
	 * @param aValue Feature value to match.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFeatureEqual(String aName, String aValue)
	{
		String featureValue = getFeature(aName);
		return StringUtils.equalsIgnoreCase(featureValue, aValue);
	}

	/**
	 * Count of unique features assigned to this criteria.
	 *
	 * @return Feature count.
	 */
	public int featureCount()
	{
		return mFeatures.size();
	}

	/**
	 * Returns the String associated with the feature name or
	 * <i>null</i> if the name could not be found.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public String getFeature(String aName)
	{
		return mFeatures.get(aName);
	}

	/**
	 * Returns the int associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public int getFeatureAsInt(String aName)
	{
		return Data.createInt(getFeature(aName));
	}

	/**
	 * Returns a read-only copy of the internal map containing
	 * feature list.
	 *
	 * @return Internal feature map instance.
	 */
	public final HashMap<String, String> getFeatures()
	{
		return mFeatures;
	}
}
