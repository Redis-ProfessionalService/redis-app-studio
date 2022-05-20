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
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A DGCriteria can be used to express a search criteria for a graph data source.
 * In its simplest form, it can capture type, labels item names (properties),
 * logical operators (equal, less-than, greater-than), item values and hops.  In
 * addition, it also supports grouping these expressions with boolean operators
 * such as AND and OR.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DGCriteria
{
	private int mNodeCounter = 1;
	private int mRelationshipCounter = 1;
	private String mName = StringUtils.EMPTY;
	private HashMap<String, String> mFeatures;
	private ArrayList<DGCriterion> mCriterions;
	private transient HashMap<String, Object> mProperties;

	/**
	 * Default constructor.
	 */
	public DGCriteria()
	{
		mFeatures = new HashMap<String, String>();
		mCriterions = new ArrayList<DGCriterion>();
	}

	/**
	 * Constructor accepts a name parameter and initializes the
	 * data graph criteria accordingly.
	 *
	 * @param aName Name of the criteria.
	 */
	public DGCriteria(String aName)
	{
		setName(aName);
		mFeatures = new HashMap<String, String>();
		mCriterions = new ArrayList<DGCriterion>();
	}

	/**
	 * Convenience constructor which creates a criteria that will isolate the relationship
	 * between the source and destination nodes.
	 *
	 * @param aName Name of criteria.
	 * @param aSrcNode Source node data document instance.
	 * @param aDstNode Destination node data document instance.
	 * @param anEdgeDoc Edge node data document instance.
	 */
	public DGCriteria(String aName, DataDoc aSrcNode, DataDoc aDstNode, DataDoc anEdgeDoc)
	{
		setName(aName);
		mFeatures = new HashMap<String, String>();
		mCriterions = new ArrayList<DGCriterion>();
		add(new DGCriterion(Data.GraphObject.Vertex, aSrcNode));
		add(new DGCriterion(Data.GraphObject.Edge, anEdgeDoc));
		add(new DGCriterion(Data.GraphObject.Vertex, aDstNode));
	}

	/**
	 * Copy constructor.
	 *
	 * @param aDGCriteria Data graph criteria instance
	 */
	public DGCriteria(DGCriteria aDGCriteria)
	{
		if (aDGCriteria != null)
		{
			setName(aDGCriteria.getName());
			this.mNodeCounter = aDGCriteria.mNodeCounter;
			this.mRelationshipCounter = aDGCriteria.mRelationshipCounter;
			this.mFeatures = new HashMap<String, String>(aDGCriteria.getFeatures());
			mCriterions = new ArrayList<DGCriterion>();
			aDGCriteria.getCriterions().forEach(c -> {
				DGCriterion nc = new DGCriterion(c);
				add(nc);
			});
		}
	}

	/**
	 * Returns a list of criterions currently being managed
	 * by the data graph criteria.
	 *
	 * @return An array list of criterions.
	 */
	public ArrayList<DGCriterion> getCriterions()
	{
		return mCriterions;
	}

	/**
	 * Returns a string summary representation of a data graph criteria.
	 *
	 * @return String summary representation of a data graph criteria.
	 */
	@Override
	public String toString()
	{
		String idName;

		if (StringUtils.isEmpty(mName))
			idName = "Data Graph Criteria";
		else
			idName = mName;

		return String.format("%s [%d criterions]", idName, count());
	}

	/**
	 * Returns the name of the data graph criteria.
	 *
	 * @return Criteria name.
	 */
	public String getName()
	{
		return mName;
	}

	/**
	 * Assigns the name of the data graph criteria.
	 *
	 * @param aName Criteria name.
	 */
	public void setName(String aName)
	{
		mName = aName;
	}

	/**
	 * Adds a criterion to the data graph criteria.
	 *
	 * @param aDGCriterion Criterion instance.
	 */
	public void add(DGCriterion aDGCriterion)
	{
		if (aDGCriterion.getObject() == Data.GraphObject.Vertex)
			aDGCriterion.setSchemaName(String.format("n%d", mNodeCounter++));
		else
			aDGCriterion.setSchemaName(String.format("r%d", mRelationshipCounter++));
		mCriterions.add(aDGCriterion);
	}

	/**
	 * Clears the collection of criterion entries.
	 */
	public void reset()
	{
		mCriterions.clear();
		mFeatures.clear();
	}

	/**
	 * Returns the count of criterion entries in this criteria.
	 *
	 * @return Total criterion entries in this criteria.
	 */
	public int count()
	{
		return mCriterions.size();
	}

	/**
	 * Returns <i>true</i> if any criterions has a number of hops
	 * that is greater than zero.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isHopsAssigned()
	{
		for (DGCriterion dgCriterion : mCriterions)
		{
			if (dgCriterion.isHopsAssigned())
				return true;
		}

		return false;
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
	 * Add a unique feature to this criteria.  A feature enhances the core
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
	 * Add a unique feature to this criteria.  A feature enhances the core
	 * capability of the criteria.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, long aValue)
	{
		addFeature(aName, Long.toString(aValue));
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
	 * Returns the long associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public long getFeatureAsLong(String aName)
	{
		return Data.createLong(getFeature(aName));
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

	/**
	 * Add an application defined property to the criteria.
	 *
	 * <b>Notes:</b>
	 *
	 * <ul>
	 *     <li>The goal of the DGCriteria is to strike a balance between
	 *     providing enough properties to adequately model application
	 *     related data without overloading it.</li>
	 *     <li>This method offers a mechanism to capture additional
	 *     (application specific) properties that may be needed.</li>
	 *     <li>Properties added with this method are transient and
	 *     will not be persisted when saved.</li>
	 * </ul>
	 *
	 * @param aName Property name (duplicates are not supported).
	 * @param anObject Instance of an object.
	 */
	public void addProperty(String aName, Object anObject)
	{
		if (mProperties == null)
			mProperties = new HashMap<String, Object>();
		mProperties.put(aName, anObject);
	}

	/**
	 * Returns the object associated with the property name or
	 * <i>null</i> if the name could not be matched.
	 *
	 * @param aName Name of the property.
	 * @return Instance of an object.
	 */
	public Object getProperty(String aName)
	{
		if (mProperties == null)
			return null;
		else
			return mProperties.get(aName);
	}

	/**
	 * Removes all application defined properties assigned to this bag.
	 */
	public void clearProperties()
	{
		if (mProperties != null)
			mProperties.clear();
	}

	/**
	 * Returns the property map instance managed by the criteria or <i>null</i>
	 * if empty.
	 *
	 * @return Hash map instance.
	 */
	public HashMap<String, Object> getProperties()
	{
		return mProperties;
	}
}
