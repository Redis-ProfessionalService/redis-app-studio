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

import com.isomorphic.criteria.AdvancedCriteria;
import com.isomorphic.datasource.DSRequest;
import com.isomorphic.datasource.DataSource;
import com.isomorphic.servlet.RequestContext;
import com.isomorphic.servlet.ServletTools;
import com.redis.ds.ds_content.Content;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.*;
import com.redis.foundation.std.StrUtl;
import com.redis.ds.ds_content.ContentType;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Collection of utility methods that support transformation of SmartClient
 * Data Source Requests into Redis Foundational objects.
 *
 * <b>Note:</b> These classes are utilized before a Servlet method has
 * established an Application Context, so any log messages will be
 * redirected to the containers stdout message stream (e.g. "catalina.out").
 *
 * @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=group..serverDataIntegration">SmartClient Data Sources</a>
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class SCDSRequest
{
	public final String FIELD_CRITERIA_ADVANCED = Data.FEATURE_DS_PREFIX + "advancedCriteria";
	public final String FIELD_CRITERIA_APP_PREFIX = Data.FEATURE_DS_PREFIX + "appPrefix";

	private boolean mIsDebug;
	private final SCLogger mLogger;

	public SCDSRequest()
	{
		mLogger = new SCLogger("SCDSRequest");
	}

	public void setDebugFlag(boolean anIsEnabled)
	{
		mIsDebug = anIsEnabled;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private String extractAdvancedCriteria(Map aSCCriteria)
	{
		String methodName = "extractAdvancedCriteria";
		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		if (aSCCriteria != null)
		{
			String fieldName;

			for (Object eso : aSCCriteria.entrySet())
			{
				Map.Entry mapEntry = (Map.Entry) eso;
				fieldName = mapEntry.getKey().toString();
				if (StringUtils.equals(fieldName, FIELD_CRITERIA_ADVANCED))
					return mapEntry.getValue().toString();
			}
		}

		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return StringUtils.EMPTY;
	}

	private void assignSortFields(DSCriteria aCriteria, List<String> aFieldNameList)
	{
		String methodName = "assignSortFields";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		if ((aCriteria != null) && (aFieldNameList != null))
		{
			for (String sortField : aFieldNameList)
			{
				if (StringUtils.startsWith(sortField, "-"))
					aCriteria.add(sortField.substring(1), Data.Operator.SORT, Data.Order.DESCENDING.name());
				else
					aCriteria.add(sortField, Data.Operator.SORT, Data.Order.ASCENDING.name());
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);
	}

	/**
	 * The SmartClient simple criteria is just a map of name/value
	 * pairs without any operators.  However, this method has logic
	 * that will look for a JSON representation of an AdvancedCriteria
	 * object and (if found) will transform it into a individual
	 * DSCriterion objects.  This method also supports field names
	 * with the convention of "FieldName:Data.Operator" and parsers
	 * into corresponding DSCriterion objects.
	 *
	 * @param aDSRequest SmartClient data source request instance
	 *
	 * @return Data source criteria instance
	 *
	 * @throws Exception Simple criteria parsing error
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public DSCriteria convertDSCriteria(DSRequest aDSRequest)
		throws Exception
	{
		DSCriteria dsCriteria;
		String fieldName, fieldValue;

		String methodName = "convertDSCriteria";
		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		Map scCriteria = aDSRequest.getCriteria();
		DataSource scDS = aDSRequest.getDataSource();
		String dsName = scDS.getName();
		String jsonSCAdvancedCriteria = extractAdvancedCriteria(scCriteria);
		if (StringUtils.isNotEmpty(jsonSCAdvancedCriteria))
			dsCriteria = new DSCriteria(dsName, jsonSCAdvancedCriteria);
		else
		{
			int offset = 0;
			dsCriteria = new DSCriteria(dsName);
			for (Object eso : scCriteria.entrySet())
			{
				Map.Entry mapEntry = (Map.Entry) eso;
				fieldName = mapEntry.getKey().toString();
				if (! StringUtils.startsWith(fieldName, Data.FEATURE_DS_PREFIX))
					dsCriteria.addSpecialEntry(fieldName, "equal", mapEntry.getValue());
				if (mIsDebug)
					mLogger.debug("[" + offset++ + "]" + ": " + mapEntry.getKey() + " = " + mapEntry.getValue());
			}
		}
		dsCriteria.setCaseSensitive(false);

		int offset = 0;
		for (Object eso : scCriteria.entrySet())
		{
			Map.Entry mapEntry = (Map.Entry) eso;
			fieldName = mapEntry.getKey().toString();
			fieldValue = mapEntry.getValue().toString();

			if (StringUtils.startsWith(fieldName, Data.FEATURE_DS_PREFIX))
			{
				dsCriteria.addFeature(fieldName, fieldValue);
				if (mIsDebug)
					mLogger.debug("[" + offset++ + "] Feature" + ": " + fieldName + " = " + fieldValue);
			}
		}

// Assign any sort fields (which are external to the simple criteria).

		List<String> sortByFields = aDSRequest.getSortByFields();
		assignSortFields(dsCriteria, sortByFields);

		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return dsCriteria;
	}

	/**
	 * The SmartClient simple criteria is just a map of name/value
	 * pairs without any operators.  However, this method has logic
	 * that will look for a JSON representation of an AdvancedCriteria
	 * object and (if found) will transform it into a individual
	 * DGCriterion objects.  This method also supports field names
	 * with the convention of "FieldName:Data.Operator" and parsers
	 * into corresponding DSCriterion objects.
	 *
	 * @param aDSRequest SmartClient data source request instance
	 *
	 * @return Data source criteria instance
	 *
	 * @throws Exception Simple criteria parsing error
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public DGCriteria convertDGCriteria(DSRequest aDSRequest)
		throws Exception
	{
		int hopCount;
		Object objectValue;
		DSCriteria dsCriteria;
		Data.GraphObject graphObject;
		String fieldName, fieldValue, objectIdentifier, edgeDirection, criteriaName;

		String methodName = "convertDGCriteria";
		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		Map scCriteria = aDSRequest.getCriteria();
		DataSource scDS = aDSRequest.getDataSource();
		String dsName = scDS.getName();
		DGCriteria dgCriteria = new DGCriteria(dsName);

// First, we will convert non-criteria fields to DGCriteria features.

		int offset = 0;
		for (Object eso : scCriteria.entrySet())
		{
			Map.Entry mapEntry = (Map.Entry) eso;
			fieldName = mapEntry.getKey().toString();
			fieldValue = mapEntry.getValue().toString();

			if (StringUtils.startsWith(fieldName, Data.FEATURE_DS_PREFIX))
			{
				dgCriteria.addFeature(fieldName, fieldValue);
				if (mIsDebug)
					mLogger.debug("[" + offset++ + "] Feature" + ": " + fieldName + " = " + fieldValue);
			}
		}

// Next, we will focus on constructing DGCriterion entries

		int dgCriterionCount = DS.criterionCountFromCriteria(dgCriteria);
		for (int dgcOffset = 0; dgcOffset < dgCriterionCount; dgcOffset++)
		{
			fieldName = String.format("dgc_%d_object_type", dgcOffset);
			objectValue = scCriteria.get(fieldName);
			if (objectValue != null)
			{
				fieldValue = objectValue.toString();
				if (StringUtils.equals(fieldValue, "Node"))
					graphObject = Data.GraphObject.Vertex;
				else
					graphObject = Data.GraphObject.Edge;
				fieldName = String.format("dgc_%d_object_identifier", dgcOffset);
				objectValue = scCriteria.get(fieldName);
				if (objectValue != null)
					objectIdentifier = objectValue.toString();
				else
					objectIdentifier = StringUtils.EMPTY;
				fieldName = String.format("dgc_%d_hop_count", dgcOffset);
				objectValue = scCriteria.get(fieldName);
				if (objectValue != null)
				{
					fieldValue = objectValue.toString();
					hopCount = Integer.parseInt(fieldValue);
				}
				else
					hopCount = 0;
				fieldName = String.format("dgc_%d_edge_direction", dgcOffset);
				objectValue = scCriteria.get(fieldName);
				if (objectValue == null)
					edgeDirection = "None";
				else
					edgeDirection = objectValue.toString();

				fieldName = String.format("dgc_%d_ds_criteria", dgcOffset);
				if (StringUtils.isNotEmpty(objectIdentifier))
					criteriaName = String.format("[%d/%d] %s Criteria", dgcOffset+1, dgCriterionCount, objectIdentifier);
				else
					criteriaName = String.format("[%d/%d] %s Criteria", dgcOffset+1, dgCriterionCount, dsName);
				objectValue = scCriteria.get(fieldName);
				if (objectValue != null)
				{
					fieldValue = objectValue.toString();
					if (StringUtils.isNotEmpty(fieldValue))
						dsCriteria = new DSCriteria(criteriaName, fieldValue);
					else
						dsCriteria = new DSCriteria(criteriaName);
				}
				else
					dsCriteria = new DSCriteria(criteriaName);
				dsCriteria.setCaseSensitive(true);

// Now we can create a new DGCriterion object and add it to the DGCriteria

				if (StringUtils.equals(fieldValue, "Node"))
					dgCriteria.add(new DGCriterion(graphObject, objectIdentifier, dsCriteria));
				else if (StringUtils.equals(edgeDirection, "None"))
					dgCriteria.add(new DGCriterion(graphObject, objectIdentifier, false, false, dsCriteria, hopCount));
				else if (StringUtils.equals(edgeDirection, "Outbound"))
					dgCriteria.add(new DGCriterion(graphObject, objectIdentifier, false, true, dsCriteria, hopCount));
				else
					dgCriteria.add(new DGCriterion(graphObject, objectIdentifier, false, true, dsCriteria, hopCount));
			}
		}

		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return dgCriteria;
	}

	/**
	 * Converts the SmartClient map of field/value pairs into a DataDoc
	 * instance derived from the schema document parameter.
	 *
	 * @param aMap Name/Value map
	 * @param aSchemaDoc Schema data document instance
	 *
	 * @return Data document instance representing the original Map
	 */
	@SuppressWarnings("WhileLoopReplaceableByForEach")
	public DataDoc convertDocument(Map aMap, DataDoc aSchemaDoc)
	{
		Date valueDate;
		DataItem dataItem;
		String keyName, keyValue;
		Optional<DataItem> optDataItem;
		String methodName = "convertDocument";

		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		DataDoc dataDoc = new DataDoc(aSchemaDoc);
		if (aMap != null)
		{
			int offset = 0;
			Iterator mapIterator = aMap.entrySet().iterator();
			while (mapIterator.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) mapIterator.next();
				keyName = mapEntry.getKey().toString();
				if (mapEntry.getValue() != null)
				{
					if (StringUtils.startsWith(keyName, "_"))		// skip details like "_selection_4:true"
						continue;
					else if (StringUtils.equals(keyName, Constants.RAS_CONTEXT_FIELD_NAME))
					{
						keyValue = mapEntry.getValue().toString();
						// Expecting: appPrefix|dataStructure|dsTitle from "Application.ts"
						List<String> valueList = StrUtl.expandToList(keyValue, StrUtl.CHAR_PIPE);
						if (valueList.size() != 3)
							mLogger.error(String.format("[%s] expecting 3, but got %d: %s", Constants.RAS_CONTEXT_FIELD_NAME, valueList.size(), keyValue));
						else
						{
							dataDoc.addFeature(Constants.FEATURE_APP_PREFIX, valueList.get(0));
							dataDoc.addFeature(Constants.FEATURE_DATA_STRUCTURE, valueList.get(1));
							dataDoc.addFeature(Constants.FEATURE_DS_TITLE, valueList.get(2));
						}
					}
					else
					{
						optDataItem = dataDoc.getItemByNameOptional(keyName);
						if (optDataItem.isPresent())
						{
							dataItem = optDataItem.get();
							if (Data.isDateOrTime(dataItem.getType()))
							{
								valueDate = (Date) mapEntry.getValue();
								dataItem.setValue(valueDate);
							}
							else
								dataDoc.setValueByName(keyName, mapEntry.getValue().toString());
							if (mIsDebug)
								mLogger.debug("[" + offset + "]" + ": " + keyName + " = " + mapEntry.getValue());
						}
						else
						{
							if (mIsDebug)
								mLogger.debug("[" + offset + "]" + ":(UNMATCHED) " + keyName + " = " + mapEntry.getValue());
						}
					}
				}
				else if (mIsDebug)
					mLogger.debug("[" + offset + "]" + ": " + keyName + " = null (Skipping)");

				offset++;
			}
		}

		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return dataDoc;
	}

	/**
	 * This SmartClient simple criteria is just a map of name/value
	 * pairs without any operators.  This method creates a DSCriteria
	 * based on the Map by assuming each entry should be converted as
	 * DSCriterion(name, Data.Operator.EQUAL, value)
	 *
	 * @param aCriteria Name/Value map
	 *
	 * @return Data source criteria instance
	 */
	@SuppressWarnings("WhileLoopReplaceableByForEach")
	public DSCriteria convertDSCriteria(Map aCriteria)
	{
		String methodName = "convertDSCriteria";
		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		DSCriteria dsCriteria = new DSCriteria("DS Criteria (Simple Criteria)");
		dsCriteria.setCaseSensitive(false);

		if (aCriteria != null)
		{
			int offset = 0;
			Iterator mapIterator = aCriteria.entrySet().iterator();
			while (mapIterator.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) mapIterator.next();
				dsCriteria.add(mapEntry.getKey().toString(), mapEntry.getValue().toString());
				if (mIsDebug)
				{
					mLogger.debug("Criteria [" + offset + "]" + ": " + mapEntry.getKey() + " = " + mapEntry.getValue());
					if (mapEntry.getKey().equals("criteria"))
						mLogger.debug("Criteria  [" + offset + "]" + "criteria = " + mapEntry.getValue().getClass());
					offset++;
				}
			}
		}

		if (mIsDebug)
			mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return dsCriteria;
	}

	/* NOTE: If you use the following logic within a SmartClient client application:

	   aCriteria.addCriteria(fieldName, OperatorId.IN_SET, multiValues);
	   aCriteria.addCriteria(fieldName, OperatorId.BETWEEN, multiValues);

	   Where multiValues is a String[], then the SmartClient Server will
	   throw a null pointer exception when you call aCriteria.getCriteriaAsMap();

	   The workaround is to assign the values as a single comma separated string
	   and use a special naming convention with the field name as follows:

		singleValue = "Red,Blue,Black";
		aCriteria.addCriteria("text_range:IN", OperatorId.EQUALS, singleValue);
		aCriteria.addCriteria("test_date:BETWEEN", OperatorId.EQUALS, "SEP-20-2012,SEP-30-2012");

	   -------------------------------------------------------------------------------------------

	   This scenario does NOT apply to an AdvancedCriteria used by SmartClient
	   for RDBMS data sources.
	*/
	public DSCriteria convertDSCriteria(AdvancedCriteria aCriteria)
	{
		String methodName = "convertDSCriteria";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		Map<String, Object> scCriteriaMap = aCriteria.getCriteriaAsMap();
		DSCriteria dsCriteria = new DSCriteria("DS Criteria (Advanced Criteria)");

		if (scCriteriaMap != null)
		{
			for (Map.Entry<String, Object> mapEntry : scCriteriaMap.entrySet())
			{
				String entryName = mapEntry.getKey();
				Object entryObject = mapEntry.getValue();

				if (entryName.equals("criteria"))
					addArrayList(dsCriteria, entryObject);
				else
					mLogger.debug("In convertCriteria, entryName = " + entryName);
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return dsCriteria;
	}

	// =============== LEGACY LOGIC BELOW MUST BE VALIDATED BEFORE MOVING UP ===============

	public String getApplicationPrefix(DSCriteria aDSCriteria)
	{
		String appPrefix;
		String methodName = "getApplicationPrefix";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		if (aDSCriteria == null)
		{
			appPrefix = Constants.APPLICATION_PREFIX_DEFAULT;
			mLogger.error("DSCriteria is null - using default default of '" + appPrefix + "'.");
		}
		else
		{
			appPrefix = aDSCriteria.getFeature(Constants.FEATURE_APPLICATION_PREFIX);
			if (StringUtils.isEmpty(appPrefix))
			{
				appPrefix = Constants.APPLICATION_PREFIX_DEFAULT;
				mLogger.error("DSCriteria is missing application prefix - using default of '" + appPrefix + "'.");
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return appPrefix;
	}

	public ArrayList gridToArrayList(DataGrid aDataGrid)
	{
		HashMap rowMap;
		DataDoc dataDoc;
		ArrayList replyList = new ArrayList();
		String methodName = "gridToArrayList";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();
		if ((rowCount > 0) && (colCount > 0))
		{
			for (int row = 0; row < rowCount; row++)
			{
				rowMap = new HashMap();
				dataDoc = aDataGrid.getRowAsDoc(row);
				for (DataItem dataItem : dataDoc.getItems())
					rowMap.put(dataItem.getName(), dataItem.getValueAsObject(StrUtl.CHAR_PIPE));
				replyList.add(rowMap);
				mLogger.debug(String.format("[%d] %s", row, dataDoc.toString()));
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return replyList;
	}

	public void saveCookie(RequestContext aContext, String aName, String aValue)
	{
		if (aContext != null)
		{
			if ((StringUtils.isNotEmpty(aName)) && (StringUtils.isNotEmpty(aValue)))
				ServletTools.setCookie(aContext, aName, aValue, -1);
		}
	}

	@SuppressWarnings("WhileLoopReplaceableByForEach")
	public void printMap(String aKind, Map aMap)
	{
		String methodName = "printMap";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		if (aMap != null)
		{
			int offset = 0;
			Iterator mapIterator = aMap.entrySet().iterator();
			while (mapIterator.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) mapIterator.next();
				mLogger.debug(aKind + " [" + offset + "]" + ": " + mapEntry.getKey() + " = " + mapEntry.getValue());
				if (mapEntry.getKey().equals("criteria"))
					mLogger.debug(aKind + " [" + offset + "]" + "criteria = " + mapEntry.getValue().getClass());
				offset++;
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);
	}

	@SuppressWarnings("IfCanBeSwitch")
	private void addCriterion(DSCriteria aCriteria, String aFieldName,
									 String anOperator, Object anObject)
	{
		String methodName = "addCriterion";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		if (StringUtils.startsWith(aFieldName, Constants.CRITERIA_RAS_PREFIX))
		{
			if (aFieldName.equals(Constants.CRITERIA_RAS_APPLICATION_PREFIX))
				aCriteria.addFeature(Constants.FEATURE_APPLICATION_PREFIX, anObject.toString());
			else if (aFieldName.equals(Constants.CRITERIA_RAS_ACCOUNT_NAME))
				aCriteria.addFeature(Constants.FEATURE_ACCOUNT_NAME, anObject.toString());
			else if (aFieldName.equals(Constants.CRITERIA_RAS_DATABASE_NAME))
				aCriteria.addFeature(Constants.FEATURE_DATABASE_NAME, anObject.toString());
			else if (aFieldName.equals(Constants.CRITERIA_RAS_ACCOUNT_PASSWORD))
				aCriteria.addFeature(Constants.FEATURE_ACCOUNT_PASSWORD, anObject.toString());
			else if (aFieldName.equals(Constants.CRITERIA_RAS_LIMIT_NUMBER))
				aCriteria.addFeature(Constants.FEATURE_LIMIT_NUMBER, anObject.toString());
			else if (aFieldName.equals(Constants.CRITERIA_RAS_OFFSET_NUMBER))
				aCriteria.addFeature(Constants.FEATURE_OFFSET_NUMBER, anObject.toString());
			else if (aFieldName.equals(Constants.CRITERIA_RAS_DETAIL_ID))
				aCriteria.addFeature(Constants.FEATURE_DETAIL_PRIMARY_ID, anObject.toString());
			else
				aCriteria.addFeature(aFieldName, anObject.toString());
		}
		else
			aCriteria.addSpecialEntry(aFieldName, anOperator, anObject);

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);
	}

	private void addArrayList(DSCriteria aCriteria, Object anObject)
	{
		HashMap hashMap;
		LinkedMap linkedMap;
		String mapName, mapOperator;
		Object mapValue, criteriaValue;
		String methodName = "addArrayList";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		if (anObject != null)
		{
			if (anObject instanceof ArrayList)
			{
				ArrayList arrayList = (ArrayList) anObject;
				for (Object arrayObject : arrayList)
				{
					if (arrayObject instanceof HashMap)
					{
						hashMap = (HashMap) arrayObject;
//                        printMap("HashMap", hashMap);
						criteriaValue = hashMap.get("criteria");
						if (criteriaValue != null)
							addArrayList(aCriteria, criteriaValue);
						else
						{
							mapValue = hashMap.get("value");
							mapName = (String) hashMap.get("fieldName");
							mapOperator = (String) hashMap.get("operator");
							addCriterion(aCriteria, mapName, mapOperator, mapValue);
						}
					}
					else if (arrayObject instanceof LinkedMap)
					{
						linkedMap = (LinkedMap) arrayObject;
//                        printMap("LinkedMap", linkedMap);
						criteriaValue = linkedMap.get("criteria");
						if (criteriaValue != null)
							addArrayList(aCriteria, criteriaValue);
						else
						{
							mapValue = linkedMap.get("value");
							mapName = (String) linkedMap.get("fieldName");
							mapOperator = (String) linkedMap.get("operator");
							addCriterion(aCriteria, mapName, mapOperator, mapValue);
						}
					}
					else
						mLogger.error("arrayObject = " + arrayObject.getClass());
				}
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);
	}

	public int getFetchOffset(DSCriteria aDSCriteria, DSRequest aDSRequest)
	{
		int offset;
		String methodName = "getFetchOffset";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		String offsetString = aDSCriteria.getFeature(Constants.FEATURE_OFFSET_NUMBER);
		if (StringUtils.isNotEmpty(offsetString))
			offset = Integer.parseInt(offsetString);
		else
			offset = (int) aDSRequest.getStartRow();

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return offset;
	}

	public int getFetchLimit(DSCriteria aDSCriteria, DSRequest aDSRequest)
	{
		int limit;
		String methodName = "getFetchLimit";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		String limitString = aDSCriteria.getFeature(Constants.FEATURE_LIMIT_NUMBER);
		if (StringUtils.isNotEmpty(limitString))
			limit = Integer.parseInt(limitString);
		else
			limit = (int) (aDSRequest.getEndRow() - aDSRequest.getStartRow());

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return limit;
	}

	public HashMap<String,Object> docToNameObjectMap(DataDoc aDataDoc)
	{
		String fieldName, fieldValue;
		String methodName = "docToNameObjectMap";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		HashMap<String, Object> hashMap = new HashMap<String, Object>();

		for (DataItem dataItem : aDataDoc.getItems())
		{
			fieldName = dataItem.getName();
			fieldValue = dataItem.getValue();

			if ((StringUtils.isNotEmpty(fieldName)) && (StringUtils.isNotEmpty(fieldValue)))
				hashMap.put(fieldName, dataItem.getValueAsObject());
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return hashMap;
	}

	public DataDoc createOptionsDoc()
	{
		String methodName = "createOptionsDoc";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		DataDoc optionsDoc = new DataDoc("Options Doc");
		optionsDoc.add(new DataItem.Builder().name("type_field_name").title("Type Field").build());
		optionsDoc.add(new DataItem.Builder().name("file_field_name").title("File Field").build());
		optionsDoc.add(new DataItem.Builder().name("icon_field_name").title("Icon Field").build());
		optionsDoc.add(new DataItem.Builder().name("icon_extension").title("Icon Extension").build());
		optionsDoc.add(new DataItem.Builder().type(Data.Type.Integer).name("content_length").title("Content Length").defaultValue(Constants.DS_CONTENT_LENGTH_DEFAULT).build());
		optionsDoc.add(new DataItem.Builder().name("delimiter_string").title("Delimiter String").build());

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return optionsDoc;
	}

	public ArrayList docToArrayList(DataDoc anOptionsDoc, DataDoc aDataDoc)
	{
		char delimiterChar;
		HashMap bagMap = new HashMap();
		ArrayList replyList = new ArrayList();
		String methodName = "docToArrayList";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		String delimiterString = anOptionsDoc.getValueByName("delimiter_string");
		if (StringUtils.isEmpty(delimiterString))
			delimiterChar = StrUtl.CHAR_PIPE;
		else
			delimiterChar = delimiterString.charAt(0);

		for (DataItem dataItem : aDataDoc.getItems())
			bagMap.put(dataItem.getName(), dataItem.getValueAsObject(delimiterChar));

		replyList.add(bagMap);

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return replyList;
	}



	public ArrayList gridToArrayList(DataDoc anOptionsDoc, DataGrid aDataGrid)
	{
		HashMap rowMap;
		DataItem dataItem;
		char delimiterChar;
		String methodName = "gridToArrayList";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		ArrayList replyList = new ArrayList();
		String delimiterString = anOptionsDoc.getValueByName("delimiter_string");
		if (StringUtils.isEmpty(delimiterString))
			delimiterChar = StrUtl.CHAR_PIPE;
		else
			delimiterChar = delimiterString.charAt(0);

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();

		if ((rowCount > 0) && (colCount > 0))
		{
			for (int row = 0; row < rowCount; row++)
			{
				rowMap = new HashMap();
				for (int col = 0; col < colCount; col++)
				{
					Optional<DataItem> optDataItem = aDataGrid.getItemByRowColOptional(row, col);
					if (optDataItem.isPresent())
					{
						dataItem = optDataItem.get();
						rowMap.put(dataItem.getName(), dataItem.getValueAsObject(delimiterChar));
					}
				}
				replyList.add(rowMap);
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return replyList;
	}

	public ArrayList gridAddIconToArrayList(ContentType aContentType, DataDoc anOptionDoc, DataGrid aDataGrid)
	{
		HashMap rowMap;
		char delimiterChar;
		DataItem dataItem1, dataItem2;
		String fieldName, iconName, fileName;
		String methodName = "gridAddIconToArrayList";
		mLogger.trace(methodName, Constants.LOG_MSG_ENTER);

		ArrayList replyList = new ArrayList();

		int rowCount = aDataGrid.rowCount();
		int colCount = aDataGrid.colCount();

		if ((rowCount > 0) && (colCount > 0))
		{
			String typeFieldName = anOptionDoc.getValueByName("type_field_name");
			String fileFieldName = anOptionDoc.getValueByName("file_field_name");
			String iconFieldName = anOptionDoc.getValueByName("icon_field_name");
			String iconExtension = anOptionDoc.getValueByName("icon_extension");
			String delimiterString = anOptionDoc.getValueByName("delimiter_string");
			if (StringUtils.isEmpty(delimiterString))
				delimiterChar = StrUtl.CHAR_PIPE;
			else
				delimiterChar = delimiterString.charAt(0);

			for (int row = 0; row < rowCount; row++)
			{
				rowMap = new HashMap();
				for (int col = 0; col < colCount; col++)
				{
					Optional<DataItem> optDataItem = aDataGrid.getItemByRowColOptional(row, col);
					if (optDataItem.isPresent())
					{
						dataItem1 = optDataItem.get();
						fieldName = dataItem1.getName();
						if (aContentType != null)
						{
							if (StringUtils.equals(fieldName, typeFieldName))
							{
								iconName = aContentType.iconByTypeName(dataItem1.getValue());
								if ((StringUtils.equals(iconName, Content.CONTENT_TYPE_UNKNOWN)) &&
									(StringUtils.isNotEmpty(fileFieldName)))
								{
									optDataItem = aDataGrid.getItemByRowNameOptional(row, fileFieldName);
									if (optDataItem.isPresent())
									{
										dataItem2 = optDataItem.get();
										fileName = dataItem2.getValue();
										if (StringUtils.isNotEmpty(fileName))
											iconName = aContentType.iconByFileExtension(fileName);
									}
								}
								if (StringUtils.isNotEmpty(iconExtension))
									iconName += iconExtension;
								rowMap.put(iconFieldName, iconName);
							}
						}
						rowMap.put(fieldName, dataItem1.getValueAsObject(delimiterChar));
					}
				}
				replyList.add(rowMap);
			}
		}

		mLogger.trace(methodName, Constants.LOG_MSG_DEPART);

		return replyList;
	}
}
