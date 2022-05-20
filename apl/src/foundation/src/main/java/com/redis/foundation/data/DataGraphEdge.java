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

package com.redis.foundation.data;

import org.jgrapht.graph.DefaultEdge;

/**
 * The DataGraphEdge captures data item/doc information for a
 * DataGraph edge.
 */
public class DataGraphEdge extends DefaultEdge
{
	static final long serialVersionUID = 111L;

	protected DataDoc mDataDoc;
	protected DataItem mDataItem;

	/**
	 * Constructor that builds a data graph edge with the specified type.
	 *
	 * @param aType Type of relationship
	 */
	public DataGraphEdge(String aType)
	{
		mDataItem = new DataItem.Builder().name(aType).build();
	}

	/**
	 * Constructor that builds a data graph edge based on the data item.
	 * The relationship type is derived from the data item name.
	 *
	 * @param aDataItem Data item instance
	 */
	public DataGraphEdge(DataItem aDataItem)
	{
		mDataItem = aDataItem;
	}

	/**
	 * Constructor that builds a data graph edge based on the data document.
	 * The relationship type is derived from the data document name.
	 *
	 * @param aDataDoc Data document instance
	 */
	public DataGraphEdge(DataDoc aDataDoc)
	{
		mDataDoc = aDataDoc;
	}

	/**
	 * Retrieves the name or type of the relationship.
	 *
	 * @return Name of relationship
	 */
	public String getName()
	{
		String edgeName;

		if (mDataItem != null)
			edgeName = mDataItem.getName();
		else if (mDataDoc != null)
			edgeName = mDataDoc.getName();
		else
			edgeName = "Undefined";

		return edgeName;
	}

	/**
	 * Retrieves the type of the relationship.
	 *
	 * @return Type of relationship
	 */
	public String getType()
	{
		return getName();
	}

	@Override
	public String toString()
	{
		return "[" + getSource() + " : " + getTarget() + " : " + getName() + "]";
	}

	/**
	 * Assigns the data item representing the edge relationship.
	 *
	 * @param aDataItem Data item instance
	 */
	public void setItem(DataItem aDataItem)
	{
		mDataItem = aDataItem;
	}

	/**
	 * Retrieves the data item representing the relationship.
	 *
	 * @return Data item instance
	 */
	public DataItem getItem()
	{
		return mDataItem;
	}

	/**
	 * Assigns the data document representing the edge relationship.
	 *
	 * @param aDataDoc Data document instance
	 */
	public void setDoc(DataDoc aDataDoc)
	{
		mDataDoc = aDataDoc;
	}

	/**
	 * Retrieves the data document representing the relationship.
	 *
	 * @return Data document instance
	 */
	public DataDoc getDoc()
	{
		return mDataDoc;
	}
}
