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

package com.redis.ds.ds_redis.graph;

import com.redis.ds.ds_redis.Redis;
import com.redis.ds.ds_redis.RedisDSException;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.*;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

/**
 * RedisCypher is a helper class responsible for generating
 * Cipher syntax from DataGraph objects.
 */
public class RedisCypher
{
	private AppCtx mAppCtx;
	private DataGraph mDataGraph;

	protected RedisCypher(AppCtx anAppCtx, DataGraph aDataGraph)
	{
		mAppCtx = anAppCtx;
		mDataGraph = aDataGraph;
	}

	protected String escapeValue(String aValue)
	{
		if (StringUtils.containsAny(aValue, StrUtl.CHAR_SPACE))
			return String.format("'%s'", aValue);
		else
			return aValue;
	}

	protected String collapseValue(DataItem aDataItem)
	{
		String itemValue;

		if (aDataItem.isMultiValue())
			itemValue = aDataItem.getValuesCollapsed();
		else
			itemValue = aDataItem.getValue();

// Escape any single quote characters with a backslash character.

		return StringUtils.replace(itemValue, "'", "\\'");
	}

	protected String getValue(DataItem aDataItem)
	{
		switch (aDataItem.getType())
		{
			case Long:
			case Float:
			case Double:
			case Integer:
				return aDataItem.getValue();
			case Boolean:
				if (aDataItem.isValueTrue())
					return "true";
				else
					return "false";
			case DateTime:
				return String.format("'%s'", Data.dateValueFormatted(aDataItem.getValueAsDate(), Data.FORMAT_DATETIME_DEFAULT));
			default:
				return String.format("'%s'", collapseValue(aDataItem));
		}
	}

	protected String shadowItemName(String aName)
	{
		return String.format("%s_shadow", aName);
	}

	private boolean assignDataItem(StringBuilder aSB, DataItem aDataItem)
	{
		Date itemDate;
		boolean valueAssigned;
		Logger appLogger = mAppCtx.getLogger(this, "assignDataItem");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataItem.isValueAssigned())
		{
			aSB.append(String.format("%s:%s", aDataItem.getName(), getValue(aDataItem)));
			if (Data.isDateOrTime(aDataItem.getType()))
			{
				itemDate = aDataItem.getValueAsDate();
				aSB.append(String.format("%s:%d", shadowItemName(aDataItem.getName()), itemDate.getTime()));
			}
			valueAssigned = true;
		}
		else
			valueAssigned = false;

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return valueAssigned;
	}

	private void addProperties(StringBuilder aSB, DataDoc aDataDoc)
	{
		Logger appLogger = mAppCtx.getLogger(this, "addProperties");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDataDoc.count() > 0)
		{
			int itemCount = 0;
			aSB.append(StrUtl.CHAR_SPACE);
			aSB.append(StrUtl.CHAR_CURLYBRACE_OPEN);
			for (DataItem dataItem : aDataDoc.getItems())
			{
				if (dataItem.isValueAssigned())
				{
					if (itemCount > 0)
						aSB.append(StrUtl.CHAR_COMMA);
					if (assignDataItem(aSB, dataItem))
						itemCount++;
				}
			}
			aSB.append(StrUtl.CHAR_CURLYBRACE_CLOSE);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected void addNode(StringBuilder aSB, String anAlias, DataDoc aDataDoc)
	{
		Logger appLogger = mAppCtx.getLogger(this, "addNode");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		aSB.append(StrUtl.CHAR_PAREN_OPEN);
		if (StringUtils.isNotEmpty(anAlias))
			aSB.append(anAlias);
		aSB.append(StrUtl.CHAR_COLON);
		int itemCount = 0;
		ArrayList<String> labelList = StrUtl.expandToList(aDataDoc.getName(), StrUtl.CHAR_PIPE);
		for (String nodeLabel : labelList)
		{
			if (itemCount > 0)
				aSB.append(StrUtl.CHAR_PIPE);
			aSB.append(nodeLabel);
			itemCount++;
		}
		addProperties(aSB, aDataDoc);
		aSB.append(StrUtl.CHAR_PAREN_CLOSE);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected void addNode(StringBuilder aSB, DataDoc aDataDoc)
	{
		addNode(aSB, StringUtils.EMPTY, aDataDoc);
	}

	protected void addRelationship(StringBuilder aSB, DataDoc aSrcNode, DataDoc aDstNode, DataDoc aRelDoc)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "addRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String relationshipType = aRelDoc.getName();
		Optional<DataItem> optPrimaryItem = aSrcNode.getPrimaryKeyItemOptional();
		if (optPrimaryItem.isEmpty())
			throw new RedisDSException(String.format("[%s] Unable to add relationship - source name '%s' is missing a primary item", mDataGraph.getName(), aSrcNode.getName()));
		DataItem srcItem = optPrimaryItem.get();
		optPrimaryItem = aDstNode.getPrimaryKeyItemOptional();
		if (optPrimaryItem.isEmpty())
			throw new RedisDSException(String.format("[%s] Unable to add relationship - source name '%s' is missing a primary item", mDataGraph.getName(), aDstNode.getName()));
		DataItem dstItem = optPrimaryItem.get();
		aSB.append(String.format("MATCH (s:%s), (d:%s) WHERE s.%s = %s AND d.%s = %s CREATE (s)-[r:%s",
								 aSrcNode.getName(), aDstNode.getName(),
								 srcItem.getName(), getValue(srcItem),
								 dstItem.getName(), getValue(dstItem),
								 relationshipType));
		addProperties(aSB, aRelDoc);
		aSB.append("]->(d) RETURN r");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected void addRelationship(StringBuilder aSB, String anAlias, DataGraphEdge aDataGraphEdge)
		throws FCException
	{
		String relationAlias;
		Logger appLogger = mAppCtx.getLogger(this, "addRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(anAlias))
			relationAlias = "r";
		else
			relationAlias = anAlias;
		if (mDataGraph.isEdgeDataItem())
		{
			String relationshipType = aDataGraphEdge.getName();
			DataItem srcNode = mDataGraph.getEdgeItemSource(aDataGraphEdge);
			DataItem dstNode = mDataGraph.getEdgeItemDestination(aDataGraphEdge);
			aSB.append(String.format("MATCH (s:%s), (d:%s) ", srcNode.getName(), dstNode.getName()));
			aSB.append(String.format("CREATE (s)-[r:%s]->(d)", relationshipType));
		}
		else
		{
			DataDoc relNode = aDataGraphEdge.getDoc();
			String relationshipType = aDataGraphEdge.getName();
			DataDoc srcNode = mDataGraph.getEdgeDocSource(aDataGraphEdge);
			Optional<DataItem> optPrimaryItem = srcNode.getPrimaryKeyItemOptional();
			if (optPrimaryItem.isEmpty())
				throw new FCException(String.format("[%s] Unable to add relationship - source name '%s' is missing a primary item", mDataGraph.getName(), srcNode.getName()));
			DataItem srcItem = optPrimaryItem.get();
			DataDoc dstNode = mDataGraph.getEdgeDocDestination(aDataGraphEdge);
			optPrimaryItem = dstNode.getPrimaryKeyItemOptional();
			if (optPrimaryItem.isEmpty())
				throw new FCException(String.format("[%s] Unable to add relationship - source name '%s' is missing a primary item", mDataGraph.getName(), dstNode.getName()));
			DataItem dstItem = optPrimaryItem.get();
			aSB.append(String.format("MATCH (s:%s), (d:%s) WHERE s.%s = %s AND d.%s = %s CREATE (s)-[%s:%s",
									 srcNode.getName(), dstNode.getName(),
									 srcItem.getName(), getValue(srcItem),
									 dstItem.getName(), getValue(dstItem),
									 relationAlias, relationshipType));
			addProperties(aSB, relNode);
			aSB.append(String.format("]->(d) RETURN %s", relationAlias));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected void addRelationship(StringBuilder aSB, DataGraphEdge aDataGraphEdge)
		throws FCException
	{
		addRelationship(aSB, StringUtils.EMPTY, aDataGraphEdge);
	}

	protected void addRelationship(StringBuilder aSB, String aType, String aSrcNode, String aDstNode)
	{
		Logger appLogger = mAppCtx.getLogger(this, "addRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		aSB.append(String.format("MATCH (s:%s), (d:%s) ", aSrcNode, aDstNode));
		aSB.append(String.format("CREATE (s)-[r:%s]->(d) RETURN r", aType));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected boolean updateNode(StringBuilder aSB, String anAlias,
								 DataDocDiff aDataDocDiff, DataDoc aDataDoc)
	{
		DataDoc diffDoc;
		String nodeAlias;
		Logger appLogger = mAppCtx.getLogger(this, "updateNode");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int itemCount = 0;
		if (StringUtils.isEmpty(anAlias))
			nodeAlias = "n1";
		else
			nodeAlias = anAlias;
		if (aDataDoc.count() > 0)
		{
			aSB.append(" SET ");
			Optional<DataDoc> optDiffDoc = aDataDocDiff.changedItems(Data.DIFF_STATUS_DELETED);
			if (optDiffDoc.isPresent())
			{
				diffDoc = optDiffDoc.get();
				for (DataItem dataItem : diffDoc.getItems())
				{
					if (itemCount > 0)
						aSB.append(StrUtl.CHAR_COMMA);
					aSB.append(String.format("%s.%s = NULL", nodeAlias, dataItem.getName()));
					itemCount++;
				}
			}
			optDiffDoc = aDataDocDiff.changedItems(Data.DIFF_STATUS_UPDATED);
			if (optDiffDoc.isPresent())
			{
				diffDoc = optDiffDoc.get();
				for (DataItem dataItem : diffDoc.getItems())
				{
					if (itemCount > 0)
						aSB.append(StrUtl.CHAR_COMMA);
					aSB.append(String.format("%s.%s = %s", nodeAlias, dataItem.getName(), getValue(dataItem)));
					itemCount++;
				}
			}
			optDiffDoc = aDataDocDiff.changedItems(Data.DIFF_STATUS_ADDED);
			if (optDiffDoc.isPresent())
			{
				diffDoc = optDiffDoc.get();
				for (DataItem dataItem : diffDoc.getItems())
				{
					if (itemCount > 0)
						aSB.append(StrUtl.CHAR_COMMA);
					aSB.append(String.format("%s.%s = %s", nodeAlias, dataItem.getName(), getValue(dataItem)));
					itemCount++;
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return itemCount > 0;
	}

	protected boolean updateRelationship(StringBuilder aSB, String anAlias, DataDoc aDataDoc)
	{
		DataDoc diffDoc;
		String relAlias;
		Logger appLogger = mAppCtx.getLogger(this, "updateRelationship");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		int itemCount = 0;
		if (StringUtils.isEmpty(anAlias))
			relAlias = "r1";
		else
			relAlias = anAlias;
		if (aDataDoc.count() > 0)
		{
			aSB.append(" SET ");
			for (DataItem dataItem : aDataDoc.getItems())
			{
				if (itemCount > 0)
					aSB.append(StrUtl.CHAR_COMMA);
				aSB.append(String.format("%s.%s = %s", relAlias, dataItem.getName(), getValue(dataItem)));
				itemCount++;
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return itemCount > 0;
	}

	protected void loadAllNodesRelationships(StringBuilder aSB)
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadAllNodesRelationships");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		aSB.append(String.format("MATCH (%s)-[%s]-() RETURN %s, %s",
								 Redis.RESULT_NODE_SCHEMA, Redis.RESULT_RELATIONSHIP_SCHEMA,
								 Redis.RESULT_NODE_SCHEMA, Redis.RESULT_RELATIONSHIP_SCHEMA));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	private int offsetFromCriteria(DGCriteria aDGCriteria)
	{
		String itemName;
		DataItem ceDataItem;
		DSCriteria dsCriteria;

		int queryOffset = 0;
		for (DGCriterion dgc : aDGCriteria.getCriterions())
		{
			if (dgc.getObject() == Data.GraphObject.Vertex)
			{
				dsCriteria = dgc.getCriteria();
				for (DSCriterionEntry ce : dsCriteria.getCriterionEntries())
				{
					ceDataItem = ce.getItem();
					itemName = ceDataItem.getName();
					if (StringUtils.equals(itemName, Redis.RG_QUERY_OFFSET))
					{
						queryOffset = Math.max(0, Data.createInt(ce.getValue()));
						break;
					}
				}
			}
		}

		return queryOffset;
	}

	private int limitFromCriteria(DGCriteria aDGCriteria)
	{
		String itemName;
		DataItem ceDataItem;
		DSCriteria dsCriteria;

		int queryLimit = Redis.QUERY_LIMIT_DEFAULT;
		for (DGCriterion dgc : aDGCriteria.getCriterions())
		{
			if (dgc.getObject() == Data.GraphObject.Vertex)
			{
				dsCriteria = dgc.getCriteria();
				for (DSCriterionEntry ce : dsCriteria.getCriterionEntries())
				{
					ceDataItem = ce.getItem();
					itemName = ceDataItem.getName();
					if (StringUtils.equals(itemName, Redis.RG_QUERY_LIMIT))
					{
						queryLimit = Data.createInt(ce.getValue());
						break;
					}
				}
			}
		}

		return queryLimit;
	}

	private boolean booleanCriterion(StringBuilder aSB, String aSchemaName, DSCriterionEntry aCE)
	{
		int begLength, endLength;
		Logger appLogger = mAppCtx.getLogger(this, "booleanCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		begLength = aSB.length();
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("%s.%s = %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("%s.%s <> %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
		}
		endLength = aSB.length();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return begLength != endLength;
	}

	private boolean numberCriterion(StringBuilder aSB, String aSchemaName, DSCriterionEntry aCE)
	{
		int valueCount, begLength, endLength;
		Logger appLogger = mAppCtx.getLogger(this, "numberCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		begLength = aSB.length();
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("%s.%s = %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("%s.%s <> %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case GREATER_THAN:
				aSB.append(String.format("%s.%s > %s", aSchemaName, dataItem.getName(), dataItem.getValue()));
				break;
			case GREATER_THAN_EQUAL:
				aSB.append(String.format("%s.%s >= %s", aSchemaName, dataItem.getName(), dataItem.getValue()));
				break;
			case LESS_THAN:
				aSB.append(String.format("%s.%s < %s", aSchemaName, dataItem.getName(), dataItem.getValue()));
				break;
			case LESS_THAN_EQUAL:
				aSB.append(String.format("%s.%s <= %s", aSchemaName, dataItem.getName(), dataItem.getValue()));
				break;
			case BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					String value1 = dataItem.getValues().get(0);
					String value2 = dataItem.getValues().get(1);
					aSB.append(String.format("%s.%s > %s AND %s.%s < %s", aSchemaName, dataItem.getName(),
											 value1, aSchemaName, dataItem.getName(), value2));
				}
				break;
			case NOT_BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					String value1 = dataItem.getValues().get(0);
					String value2 = dataItem.getValues().get(1);
					aSB.append(String.format("NOT %s.%s > %s AND NOT %s.%s < %s", aSchemaName, dataItem.getName(),
											 value1, aSchemaName, dataItem.getName(), value2));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.valueCount() == 2)
				{
					String value1 = dataItem.getValues().get(0);
					String value2 = dataItem.getValues().get(1);
					aSB.append(String.format("%s.%s >= %s AND %s.%s <= %s", aSchemaName, dataItem.getName(),
											 value1, aSchemaName, dataItem.getName(), value2));
				}
				break;
			case IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
						aSB.append(String.format("%s.%s IN [", aSchemaName, dataItem.getName()));
					else
						aSB.append(",");
					aSB.append(escapeValue(ceValue));
					valueCount++;
				}
				if (valueCount > 0)
					aSB.append("]");
				break;
			case NOT_IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
						aSB.append(String.format("%s.%s NOT IN [", aSchemaName, dataItem.getName()));
					else
						aSB.append(",");
					aSB.append(escapeValue(ceValue));
					valueCount++;
				}
				if (valueCount > 0)
					aSB.append("]");
				break;
		}
		endLength = aSB.length();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return begLength != endLength;
	}

	private boolean dateCriterion(StringBuilder aSB, String aSchemaName, DSCriterionEntry aCE)
	{
		int begLength, endLength;
		Logger appLogger = mAppCtx.getLogger(this, "dateCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		begLength = aSB.length();
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("%s.%s = %d", aSchemaName, shadowItemName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("NOT %s.%s = %d", aSchemaName, shadowItemName(dataItem.getName()), dataItem.getValueAsDate().getTime()));
				break;
			case GREATER_THAN:
				aSB.append(String.format("%s.%s > %d", aSchemaName, dataItem.getName(), dataItem.getValueAsDate().getTime()));
				break;
			case GREATER_THAN_EQUAL:
				aSB.append(String.format("%s.%s >= %d", aSchemaName, dataItem.getName(), dataItem.getValueAsDate().getTime()));
				break;
			case LESS_THAN:
				aSB.append(String.format("%s.%s < %d", aSchemaName, dataItem.getName(), dataItem.getValueAsDate().getTime()));
				break;
			case LESS_THAN_EQUAL:
				aSB.append(String.format("%s.%s <= %d", aSchemaName, dataItem.getName(), dataItem.getValueAsDate().getTime()));
				break;
			case BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					long timeInMilliseconds1 = Data.createDate(dataItem.getValues().get(0)).getTime();
					long timeInMilliseconds2 = Data.createDate(dataItem.getValues().get(1)).getTime();
					aSB.append(String.format("%s.%s > %d AND %s.%s < %d", aSchemaName, dataItem.getName(),
											 timeInMilliseconds1, aSchemaName, dataItem.getName(), timeInMilliseconds2));
				}
				break;
			case NOT_BETWEEN:
				if (dataItem.valueCount() == 2)
				{
					long timeInMilliseconds1 = Data.createDate(dataItem.getValues().get(0)).getTime();
					long timeInMilliseconds2 = Data.createDate(dataItem.getValues().get(1)).getTime();
					aSB.append(String.format("NOT %s.%s > %d AND NOT %s.%s < %d", aSchemaName, dataItem.getName(),
											 timeInMilliseconds1, aSchemaName, dataItem.getName(), timeInMilliseconds2));
				}
				break;
			case BETWEEN_INCLUSIVE:
				if (dataItem.valueCount() == 2)
				{
					long timeInMilliseconds1 = Data.createDate(dataItem.getValues().get(0)).getTime();
					long timeInMilliseconds2 = Data.createDate(dataItem.getValues().get(1)).getTime();
					aSB.append(String.format("%s.%s >= %d AND %s.%s <= %d", aSchemaName, dataItem.getName(),
											 timeInMilliseconds1, aSchemaName, dataItem.getName(), timeInMilliseconds2));
				}
				break;
		}
		endLength = aSB.length();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return begLength != endLength;
	}

	private boolean stringSensitiveCriterion(StringBuilder aSB, String aSchemaName, DSCriterionEntry aCE)
	{
		int valueCount, begLength, endLength;
		Logger appLogger = mAppCtx.getLogger(this, "stringSensitiveCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		begLength = aSB.length();
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("%s.%s = %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("%s.%s <> %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case EMPTY:
				aSB.append(String.format("%s.%s = null", aSchemaName, dataItem.getName()));
				break;
			case NOT_EMPTY:
				aSB.append(String.format("%s.%s <> null", aSchemaName, dataItem.getName()));
				break;
			case STARTS_WITH:
				aSB.append(String.format("%s.%s STARTS WITH %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case CONTAINS:
				aSB.append(String.format("%s.%s CONTAINS %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case ENDS_WITH:
				aSB.append(String.format("%s.%s ENDS WITH %s", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
						aSB.append(String.format("%s.%s IN [", aSchemaName, dataItem.getName()));
					else
						aSB.append(",");
					aSB.append(escapeValue(ceValue));
					valueCount++;
				}
				if (valueCount > 0)
					aSB.append("]");
				break;
			case NOT_IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
						aSB.append(String.format("%s.%s NOT IN [", aSchemaName, dataItem.getName()));
					else
						aSB.append(",");
					aSB.append(escapeValue(ceValue));
					valueCount++;
				}
				if (valueCount > 0)
					aSB.append("]");
				break;
		}
		endLength = aSB.length();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return begLength != endLength;
	}

	private boolean stringInsensitiveCriterion(StringBuilder aSB, String aSchemaName, DSCriterionEntry aCE)
	{
		int valueCount, begLength, endLength;
		Logger appLogger = mAppCtx.getLogger(this, "stringInsensitiveCriterion");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		begLength = aSB.length();
		DataItem dataItem = aCE.getItem();
		switch (aCE.getLogicalOperator())
		{
			case EQUAL:
				aSB.append(String.format("%s.%s = toLower(%s)", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case NOT_EQUAL:
				aSB.append(String.format("%s.%s <> toLower(%s)", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case EMPTY:
				aSB.append(String.format("%s.%s = null", aSchemaName, dataItem.getName()));
				break;
			case NOT_EMPTY:
				aSB.append(String.format("%s.%s <> null", aSchemaName, dataItem.getName()));
				break;
			case STARTS_WITH:
				aSB.append(String.format("%s.%s STARTS WITH toLower(%s)", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case CONTAINS:
				aSB.append(String.format("%s.%s CONTAINS toLower(%s)", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case ENDS_WITH:
				aSB.append(String.format("%s.%s ENDS WITH toLower(%s)", aSchemaName, dataItem.getName(), getValue(dataItem)));
				break;
			case IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
						aSB.append(String.format("%s.%s IN [", aSchemaName, dataItem.getName()));
					else
						aSB.append(",");
					aSB.append(String.format("toLower(%s)", escapeValue(ceValue)));
					valueCount++;
				}
				if (valueCount > 0)
					aSB.append("]");
				break;
			case NOT_IN:
				valueCount = 0;
				for (String ceValue : dataItem.getValues())
				{
					if (valueCount == 0)
						aSB.append(String.format("%s.%s NOT IN [", aSchemaName, dataItem.getName()));
					else
						aSB.append(",");
					aSB.append(String.format("toLower(%s)", escapeValue(ceValue)));
					valueCount++;
				}
				if (valueCount > 0)
					aSB.append("]");
				break;
		}
		endLength = aSB.length();

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return begLength != endLength;
	}

	private boolean objectPropertyFilter(StringBuilder aSB, DGCriterion aDGCriterion)
		throws RedisDSException
	{
		String schemaName;
		DataItem ceDataItem;
		Logger appLogger = mAppCtx.getLogger(this, "objectPropertyFilter");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		schemaName = aDGCriterion.getSchemaName();
		DSCriteria dsCriteria = aDGCriterion.getCriteria();

		boolean appendAND = false;
		if ((dsCriteria != null) && (dsCriteria.count() > 0))
		{
			for (DSCriterionEntry ce : dsCriteria.getCriterionEntries())
			{
				if ((ce.getLogicalOperator() != Data.Operator.SORT) && (appendAND))
					aSB.append(" AND ");
				ceDataItem = ce.getItem();
				switch (ceDataItem.getType())
				{
					case Boolean:
						appendAND = booleanCriterion(aSB, schemaName, ce);
						break;
					case Integer:
					case Long:
					case Float:
					case Double:
						appendAND = numberCriterion(aSB, schemaName, ce);
						break;
					case DateTime:
						appendAND = dateCriterion(aSB, schemaName, ce);
						break;
					default:
						if (dsCriteria.isCaseSensitive())
							appendAND = stringSensitiveCriterion(aSB, schemaName, ce);
						else
							appendAND = stringInsensitiveCriterion(aSB, schemaName, ce);
						break;
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return appendAND;
	}

	// Cypher Vertex Format (schemaName:identifierName)
	private void vertexToCypher(StringBuilder aSB, DGCriteria aDGCriteria, DGCriterion aDGCriterion)
		throws RedisDSException
	{
		Logger appLogger = mAppCtx.getLogger(this, "vertexToCypher");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDGCriterion.isAnonymous())
			aSB.append("()");
		else
		{
			String schemaName = aDGCriterion.getSchemaName();
			String identifierName = aDGCriterion.getIdentifier();

			if (StringUtils.isEmpty(schemaName))
				throw new RedisDSException(String.format("%s [%s]: Criterion lacks a schema name.", aDGCriteria.getName(),
													  	 aDGCriterion.getObject()));
			if (StringUtils.isEmpty(identifierName))
				aSB.append(String.format("(%s)", schemaName));
			else
				aSB.append(String.format("(%s:%s)", schemaName, identifierName));
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	// Cypher Relationship Format -[schemaName:identifierName]-
	private void edgeToCypher(StringBuilder aSB, DGCriteria aDGCriteria, DGCriterion aDGCriterion)
		throws RedisDSException
	{
		String leftDirection, rightDirection, hopsRange;
		Logger appLogger = mAppCtx.getLogger(this, "edgeToCypher");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDGCriterion.isBackwardDirected())
			leftDirection = "<-";
		else
			leftDirection = "-";
		if (aDGCriterion.isForwardDirected())
			rightDirection = "->";
		else
			rightDirection = "-";

		if (aDGCriterion.isAnonymous())
			aSB.append(String.format("%s[]%s", leftDirection, rightDirection));
		else
		{
			String schemaName = aDGCriterion.getSchemaName();
			String identifierName = aDGCriterion.getIdentifier();

			if (StringUtils.isEmpty(schemaName))
				throw new RedisDSException(String.format("%s [%s]: Criterion lacks a schema name.", aDGCriteria.getName(),
													  	 aDGCriterion.getObject()));
			if (StringUtils.isEmpty(identifierName))
				aSB.append(String.format("%s[%s]%s", leftDirection, schemaName, rightDirection));
			else
			{
				int hopsCount = aDGCriterion.getHops();
				if (hopsCount > 0)
					hopsRange = String.format("*..%d", hopsCount);
				else
					hopsRange = StringUtils.EMPTY;
				aSB.append(String.format("%s[%s:%s%s]%s", leftDirection, schemaName, identifierName, hopsRange, rightDirection));
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	protected void matchCriteria(StringBuilder aSB, DGCriteria aDGCriteria)
		throws RedisDSException
	{
		DataItem ceDataItem;
		DSCriteria dsCriteria;
		String itemName, schemaName;
		Logger appLogger = mAppCtx.getLogger(this, "matchCriteria");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDGCriteria.count() > 0)
		{

// Step one - identify what needs to be matched (e.g. nodes, relationships).

			aSB.append("MATCH ");
			if (aDGCriteria.isHopsAssigned())
			{
				aSB.append(Redis.RESULT_GRAPH_PATH_SCHEMA);
				aSB.append(" = ");
			}
			for (DGCriterion dgc : aDGCriteria.getCriterions())
			{
				switch (dgc.getObject())
				{
					case Edge:
						edgeToCypher(aSB, aDGCriteria, dgc);
						break;
					case Vertex:
						vertexToCypher(aSB, aDGCriteria, dgc);
						break;
				}
			}
			int statementLength = aSB.length();
			aSB.append(" WHERE ");
			boolean appendAND = false;
			for (DGCriterion dgc : aDGCriteria.getCriterions())
			{
				if ((dgc.getCriteria().count() > 0) && (appendAND))
					aSB.append(" AND ");
				appendAND = objectPropertyFilter(aSB, dgc);
			}

// Apply sorting if it was specified in the criteria.

			for (DGCriterion dgc : aDGCriteria.getCriterions())
			{
				if (dgc.getObject() == Data.GraphObject.Vertex)
				{
					dsCriteria = dgc.getCriteria();
					schemaName = dgc.getSchemaName();
					for (DSCriterionEntry ce : dsCriteria.getCriterionEntries())
					{
						if (ce.getLogicalOperator() == Data.Operator.SORT)
						{
							ceDataItem = ce.getItem();
							Data.Order sortOrder = Data.Order.valueOf(ceDataItem.getValue());
							if (Data.isDateOrTime(ceDataItem.getType()))
								itemName = shadowItemName(ceDataItem.getName());
							else
								itemName = ceDataItem.getName();
							if (sortOrder == Data.Order.ASCENDING)
								aSB.append(String.format(" ORDER BY %s.%s ASC", schemaName, itemName));
							else
								aSB.append(String.format(" ORDER BY %s.%s DESC", schemaName, itemName));
							int queryOffset = offsetFromCriteria(aDGCriteria);
							int queryLimit = limitFromCriteria(aDGCriteria);
							aSB.append(String.format(" SKIP %d LIMIT %d", queryOffset, queryLimit));
							break;
						}
					}
				}
			}

			if (aSB.toString().endsWith(" WHERE "))
				aSB.setLength(statementLength);

			appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
		}
	}

	protected void matchCriteriaWithReturn(StringBuilder aSB, DGCriteria aDGCriteria)
		throws RedisDSException
	{
		String schemaName;
		Logger appLogger = mAppCtx.getLogger(this, "matchCriteriaWithReturn");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aDGCriteria.count() > 0)
		{
			matchCriteria(aSB, aDGCriteria);
			aSB.append(" RETURN ");
			if (aDGCriteria.isHopsAssigned())
				aSB.append(Redis.RESULT_GRAPH_PATH_SCHEMA);
			else
			{
				int returnCount = 0;
				for (DGCriterion dgc : aDGCriteria.getCriterions())
				{
					schemaName = dgc.getSchemaName();
					if (returnCount > 0)
						aSB.append(StrUtl.CHAR_COMMA);
					aSB.append(schemaName);
					returnCount++;
				}
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
