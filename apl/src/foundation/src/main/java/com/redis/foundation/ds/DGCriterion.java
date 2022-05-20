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
import com.redis.foundation.data.DataItem;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * A DGCriterion is responsible for encapsulating a query item, its operator
 * and one or more values.  It is exclusively referenced from within a
 * {@link DGCriteria} object.
 *
 * Note:
 *  A Data Graph Criterion object supports directed relationship
 *  queries with the Cypher language, so backward and forward refer
 *  to the relationship direction with a Cypher statement.
 *
 *  Per OpenCypher Specification Document:
 *   This manner of describing nodes and relationships can be extended
 *   to cover an arbitrary number of nodes and the relationships between
 *   them, for example: (a)-[]-&gt;(b)&lt;-[]-(c)
 *
 * @since 1.0
 * @author Al Cole
 */
public class DGCriterion
{
	private int mHops;
	private DSCriteria mCriteria;
	private boolean mIsForwardDirected;
	private boolean mIsBackwardDirected;
	private String mIdentifier = StringUtils.EMPTY;
	private String mSchemaName = StringUtils.EMPTY;
	private Data.GraphObject mObject = Data.GraphObject.Undefined;

	/**
	 * Constructor that accepts the graph object.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 */
	public DGCriterion(Data.GraphObject anObject)
	{
		mObject = anObject;
		mIdentifier = StringUtils.EMPTY;
		mCriteria = new DSCriteria("Anonymous Criteria");
	}


	/**
	 * Constructor that accepts the graph object and its identifier.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param anIdentifier Identifier name (vertex label, edge type)
	 */
	public DGCriterion(Data.GraphObject anObject, String anIdentifier)
	{
		mObject = anObject;
		mIdentifier = anIdentifier;
		mCriteria = new DSCriteria(String.format("%s Criteria", anIdentifier));
	}

	/**
	 * Constructor that accepts the graph object, identifier and data source criteria.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param anIdentifier Identifier name (vertex label, edge type)
	 * @param aDSCriteria Data source criteria
	 */
	public DGCriterion(Data.GraphObject anObject, String anIdentifier, DSCriteria aDSCriteria)
	{
		mObject = anObject;
		mCriteria = aDSCriteria;
		mIdentifier = anIdentifier;
	}

	/**
	 * Constructor that accepts the graph object, identifier and number of hops.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param anIdentifier Identifier name (vertex label, edge type)
	 * @param aHops Number of hops
	 */
	public DGCriterion(Data.GraphObject anObject, String anIdentifier, int aHops)
	{
		mHops = aHops;
		mObject = anObject;
		mIdentifier = anIdentifier;
		mCriteria = new DSCriteria(String.format("%s Criteria", anIdentifier));
	}

	/**
	 * Constructor that accepts the graph object, identifier, left-right direction
	 * intent and number of hops.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param anIdentifier Identifier name (vertex label, edge type)
	 * @param anIsBackwardDirected If <i>true</i>, edge points left
	 * @param anIsForwardDirected If <i>true</i>, edge points right
	 * @param aHops Number of hops
	 */
	public DGCriterion(Data.GraphObject anObject, String anIdentifier,
					   boolean anIsBackwardDirected, boolean anIsForwardDirected,
					   int aHops)
	{
		mHops = aHops;
		mObject = anObject;
		mIdentifier = anIdentifier;
		mIsForwardDirected = anIsForwardDirected;
		mIsBackwardDirected = anIsBackwardDirected;
		mCriteria = new DSCriteria(String.format("%s Criteria", anIdentifier));
	}

	/**
	 * Constructor that accepts the graph object, identifier, data source
	 * criteria and number of hops.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param anIdentifier Identifier name (vertex label, edge type)
	 * @param anIsBackwardDirected If <i>true</i>, edge points left
	 * @param anIsForwardDirected If <i>true</i>, edge points right
	 * @param aDSCriteria Data source criteria
	 * @param aHops Number of hops
	 */
	public DGCriterion(Data.GraphObject anObject, String anIdentifier,
					   boolean anIsBackwardDirected, boolean anIsForwardDirected,
					   DSCriteria aDSCriteria, int aHops)
	{
		mHops = aHops;
		mObject = anObject;
		mCriteria = aDSCriteria;
		mIdentifier = anIdentifier;
		mIsForwardDirected = anIsForwardDirected;
		mIsBackwardDirected = anIsBackwardDirected;
	}

	/**
	 * Constructor that accepts the graph object, identifier, data source
	 * criteria and number of hops.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param anIdentifier Identifier name (vertex label, edge type)
	 * @param aDSCriteria Data source criteria
	 * @param aHops Number of hops
	 */
	public DGCriterion(Data.GraphObject anObject, String anIdentifier, DSCriteria aDSCriteria, int aHops)
	{
		mHops = aHops;
		mObject = anObject;
		mCriteria = aDSCriteria;
		mIdentifier = anIdentifier;
	}

	/**
	 * Constructor that accepts the graph object, identifier and data document
	 * instance with the assumption that the criterion should based on matching
	 * the primary item name and value.  An item is consider primary if it's
	 * feature is assigned Data.FEATURE_IS_PRIMARY.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 * @param aDataDoc Data source criteria
	 */
	public DGCriterion(Data.GraphObject anObject, DataDoc aDataDoc)
	{
		mObject = anObject;
		mIdentifier = aDataDoc.getName();
		mCriteria = new DSCriteria(String.format("%s Data Graph Criteria", aDataDoc.getName()));
		Optional<DataItem> optPrimaryItem = aDataDoc.getPrimaryKeyItemOptional();
		if (optPrimaryItem.isPresent())
		{
			DataItem dataItem = optPrimaryItem.get();
			mCriteria.add(dataItem.getName(), Data.Operator.EQUAL, dataItem.getValue());
		}
	}

	/**
	 * Copy constructor.
	 *
	 * @param aDGCriterion Data graph criterion
	 */
	public DGCriterion(DGCriterion aDGCriterion)
	{
		setHops(aDGCriterion.getHops());
		setSchemaName(aDGCriterion.getSchemaName());
		setIdentifier(aDGCriterion.getIdentifier());
		setObject(aDGCriterion.getObject());
		if (aDGCriterion.mCriteria != null)
			setCriteria(aDGCriterion.getCriteria());
	}

	/**
	 * Assigns the graph object type for the criteria.
	 *
	 * @param anObject Graph object (Vertex, Edge)
	 */
	public void setObject(Data.GraphObject anObject)
	{
		mObject = anObject;
	}

	/**
	 * Retrieves the graph object type.
	 *
	 * @return Graph object (Vertex, Edge, Undefined)
	 */
	public Data.GraphObject getObject()
	{
		return mObject;
	}

	/**
	 * Assigns the number of hops (degrees) that should be returned
	 * during a graph query.
	 *
	 * @param aHops Number of hops
	 */
	public void setHops(int aHops)
	{
		mHops = aHops;
	}

	/**
	 * Retrieves the number of hops.
	 *
	 * @return Number of hops
	 */
	public int getHops()
	{
		return mHops;
	}

	/**
	 * Returns <i>true</i> if number of hops is greater than zero.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isHopsAssigned()
	{
		return mHops > 0;
	}

	/**
	 * Assigns an identifier name to the graph object being queried.
	 *
	 * @param anIdentifier Identifier name.
	 */
	public void setIdentifier(String anIdentifier)
	{
		mIdentifier = anIdentifier;
	}

	/**
	 * Retrieve an identifier name.
	 *
	 * @return Identifier name
	 */
	public String getIdentifier()
	{
		return mIdentifier;
	}

	/**
	 * Assigns a schema name to the graph object being queried.  A
	 * schema name is a short-hand variable used in Cypher queries
	 * to classify objects.
	 *
	 * @param aSchemaName Identifier name.
	 */
	public void setSchemaName(String aSchemaName)
	{
		mSchemaName = aSchemaName;
	}

	/**
	 * Retrieve an schema name.  A schema name is a short-hand variable
	 * used in Cypher queries to classify objects.
	 *
	 * @return Schema name
	 */
	public String getSchemaName()
	{
		return mSchemaName;
	}

	/**
	 * Assigns left-directed relationship flag.
	 *
	 * @param aFlag <i>true</i> or <i>false</i>
	 */
	public void setBackwardDirected(boolean aFlag)
	{
		mIsBackwardDirected = true;
	}

	/**
	 * Returns left-directed relationship flag.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isBackwardDirected()
	{
		return mIsBackwardDirected;
	}

	/**
	 * Assigns right-directed relationship flag.
	 *
	 * @param aFlag <i>true</i> or <i>false</i>
	 */
	public void setForwardDirected(boolean aFlag)
	{
		mIsForwardDirected = true;
	}

	/**
	 * Returns right-directed relationship flag.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isForwardDirected()
	{
		return mIsForwardDirected;
	}

	/**
	 * Assign a data source criteria for the data graph criterion.
	 *
	 * @param aDSCriteria Data source criteria instance
	 */
	public void setCriteria(DSCriteria aDSCriteria)
	{
		mCriteria = aDSCriteria;
	}

	/**
	 * Retrieve the data source criteria from the data graph criterion.
	 *
	 * @return Data source criteria
	 */
	public DSCriteria getCriteria()
	{
		return mCriteria;
	}

	/**
	 * Returns <i>true</i> if there is no identifier and the criterion count is zero.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isAnonymous()
	{
		return ((StringUtils.isEmpty(mIdentifier)) && (mCriteria.count() == 0));
	}
}
