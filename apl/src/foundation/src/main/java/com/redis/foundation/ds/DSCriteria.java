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
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import com.redis.foundation.io.DataDocJSON;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * A DSCriteria can be used to express a search criteria for a data source.
 * In its simplest form, it can capture one or more item names, logical operators
 * (equal, less-than, greater-than) and a item values.  In addition, it also
 * supports grouping these expressions with boolean operators such as AND and OR.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DSCriteria
{
	private boolean mIsCaseSensitive = true;
	private String mName = StringUtils.EMPTY;
	private HashMap<String, String> mFeatures;
	private ArrayList<DSCriterionEntry> mCriterionEntries;
	private transient HashMap<String, Object> mProperties;

	/**
	 * Default constructor.
	 */
	public DSCriteria()
	{
		mFeatures = new HashMap<String, String>();
		mCriterionEntries = new ArrayList<DSCriterionEntry>();
	}

	/**
	 * Constructor accepts a name parameter and initializes the
	 * Criteria accordingly.
	 *
	 * @param aName Name of the criteria.
	 */
	public DSCriteria(String aName)
	{
		setName(aName);
		mFeatures = new HashMap<String, String>();
		mCriterionEntries = new ArrayList<DSCriterionEntry>();
	}

	/**
	 * Constructor accepts a criterion parameter and initializes the
	 * Criteria accordingly.
	 *
	 * @param aDSCriterion Criterion instance.
	 */
	public DSCriteria(DSCriterion aDSCriterion)
	{
		mFeatures = new HashMap<String, String>();
		mCriterionEntries = new ArrayList<DSCriterionEntry>();
		add(aDSCriterion);
	}

	/**
	 * Constructor that accepts a name and a SmartClient data item map and initializes
	 * the Criteria accordingly.
	 *
	 * @param aName Name of the criteria.
	 * @param aCriteriaMap Map of data items to construct a criteria from.
	 */
	@SuppressWarnings("unchecked")
	public DSCriteria(String aName, Map<String, Object> aCriteriaMap)
	{
		setName(aName);
		mFeatures = new HashMap<String, String>();
		mCriterionEntries = new ArrayList<DSCriterionEntry>();

		if (aCriteriaMap != null)
		{
			Object mapValue;
			String mapName, mapOperator;
			for (Map.Entry<String, Object> mapEntry : aCriteriaMap.entrySet())
			{
				String entryName = mapEntry.getKey();
				if (! entryName.equals("criteria"))
					continue;

				Object entryObject = mapEntry.getValue();
				if (entryObject instanceof ArrayList)
				{
					ArrayList<Object> arrayList = (ArrayList<Object>) entryObject;
					for (Object arrayObject : arrayList)
					{
						HashMap<String,String> hashMap = (HashMap<String,String>) arrayObject;
						mapValue = hashMap.get("value");
						mapName = hashMap.get("fieldName");
						mapOperator = hashMap.get("operator");
						addSpecialEntry(mapName, mapOperator, mapValue);
					}
				}
			}
		}
	}

	/**
	 * Constructor that accepts a name and a SmartClient Advanced Criteria (collapsed)
	 * in JSON format and initializes the Criteria accordingly.
	 *
	 * @param aName Name of the criteria.
	 * @param aJSONString SmartClient Advanced Criteria (collapsed) in JSON format
	 */
	public DSCriteria(String aName, String aJSONString)
	{
		setName(aName);
		mFeatures = new HashMap<String, String>();
		mCriterionEntries = new ArrayList<DSCriterionEntry>();

		DataDocJSON dataDocJSON = new DataDocJSON();
		Optional<DataDoc> optDataDoc = dataDocJSON.loadFromString(aJSONString);
		if (optDataDoc.isPresent())
		{
			DataItem dataItem;
			DSCriterion dsCriterion;
			ArrayList<String> valueList;
			Data.Operator logicalOperator;
			DataItem diName, diOperator, diValue, diStart, diEnd;
			Optional<DataItem> optName, optOperator, optValue, optStart, optEnd;

			DataDoc dataDoc = optDataDoc.get();
			Collection<DataDoc> dataDocCollection = dataDoc.getChildDocsAsCollection("criteria");
			for (DataDoc ddCriteria : dataDocCollection)
			{
				optName = ddCriteria.getItemByNameOptional("fieldName");
				optOperator = ddCriteria.getItemByNameOptional("operator");
				if ((optName.isPresent()) && (optOperator.isPresent()))
				{
					dsCriterion = null;
					diName = optName.get();
					diOperator = optOperator.get();
					logicalOperator = Data.scOperatorToDataOperator(diOperator.getValue());
					switch (logicalOperator)
					{
						case BETWEEN:
						case BETWEEN_INCLUSIVE:
							optStart = ddCriteria.getItemByNameOptional("start");
							optEnd = ddCriteria.getItemByNameOptional("end");
							if ((optStart.isPresent()) && (optEnd.isPresent()))
							{
								diStart = optStart.get();
								diEnd = optEnd.get();
								valueList = new ArrayList<>();
								valueList.add(diStart.getValue());
								valueList.add(diEnd.getValue());
								dsCriterion = new DSCriterion(diName.getValue(), logicalOperator, valueList);
							}
							break;
						default:
							optValue = ddCriteria.getItemByNameOptional("value");
							if (optValue.isPresent())
							{
								diValue = optValue.get();
								if (diValue.isMultiValue())
									dataItem = new DataItem.Builder().name(diName.getValue()).analyze(diValue.getValues()).build();
								else
									dataItem = new DataItem.Builder().name(diName.getValue()).analyze(diValue.getValue()).build();
								dsCriterion = new DSCriterion(dataItem, logicalOperator);
							}
							break;
					}
					if (dsCriterion != null)
						add(dsCriterion);
				}
			}
		}
	}

	/**
	 * Copy constructor.
	 *
	 * @param aDSCriteria Data source criteria instance.
	 */
	public DSCriteria(final DSCriteria aDSCriteria)
	{
		if (aDSCriteria != null)
		{
			setName(aDSCriteria.getName());
			setCaseSensitive(aDSCriteria.isCaseSensitive());
			this.mCriterionEntries = new ArrayList<DSCriterionEntry>();
			this.mFeatures = new HashMap<String, String>(aDSCriteria.getFeatures());
			aDSCriteria.getCriterionEntries().forEach(ce -> {
				DSCriterionEntry nce = new DSCriterionEntry(ce);
				add(nce);
			});
		}
	}

	/**
	 * Returns a list of criterion entries currently being managed
	 * by the criteria.
	 *
	 * @return An array of criterion entries.
	 */
	public ArrayList<DSCriterionEntry> getCriterionEntries()
	{
		return mCriterionEntries;
	}

	/**
	 * Returns a string summary representation of a criteria.
	 *
	 * @return String summary representation of a criteria.
	 */
	@Override
	public String toString()
	{
		String idName;

		if (StringUtils.isEmpty(mName))
			idName = "Data Source Criteria";
		else
			idName = mName;

		return String.format("%s [%d criterion entries]", idName, count());
	}

	/**
	 * Adds a SmartClient representation of an advanced criterion object to
	 * this criteria.  This method can handle both Simple and Advanced Criteria
	 * objects.  In the case of a Simple one (e.g. name/value pairs), you
	 * should follow the convention of "FieldName:Data.Operator" and the
	 * logic will create a DSCriterion object with the matching entries.
	 *
	 * @param aFieldName Field name.
	 * @param anOperator Logical field operator.
	 * @param anObject Generic representation of the value.
	 */
	@SuppressWarnings("unchecked")
	public void addSpecialEntry(String aFieldName, String anOperator, Object anObject)
	{
		if ((StringUtils.isNotEmpty(aFieldName)) && (StringUtils.isNotEmpty(anOperator)) && (anObject != null))
		{
			String textValue;
			DSCriterion dsCriterion;

			int offset = aFieldName.indexOf(StrUtl.CHAR_COLON);
			if (offset > 0)
			{
				textValue = anObject.toString();
				String fieldName = aFieldName.substring(0, offset);
				Data.Operator fieldOperator = Data.Operator.valueOf(aFieldName.substring(offset + 1));
				switch (fieldOperator)
				{
					case IN:
					case NOT_IN:
					case BETWEEN:
					case NOT_BETWEEN:
					case BETWEEN_INCLUSIVE:
						ArrayList<String> fieldValues = StrUtl.expandToList(textValue, StrUtl.CHAR_PIPE);
						dsCriterion = new DSCriterion(fieldName, fieldOperator, fieldValues);
						break;
					default:
						dsCriterion = new DSCriterion(fieldName, fieldOperator, textValue);
						break;
				}
			}
			else
			{
				if (anObject instanceof Integer)
				{
					Integer integerValue = (Integer) anObject;
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), integerValue);
				}
				else if (anObject instanceof Long)
				{
					Long longValue = (Long) anObject;
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), longValue);
				}
				else if (anObject instanceof Float)
				{
					Float floatValue = (Float) anObject;
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), floatValue);
				}
				else if (anObject instanceof Double)
				{
					Double doubleValue = (Double) anObject;
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), doubleValue);
				}
				else if (anObject instanceof Date)
				{
					Date dateValue = (Date) anObject;
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), dateValue);
				}
				else if (anObject instanceof ArrayList)
				{
					ArrayList<String> arrayList = (ArrayList<String>) anObject;
					String[] fieldValues = StrUtl.convertToMulti(arrayList);
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), fieldValues);
				}
				else
				{
					textValue = anObject.toString();
					dsCriterion = new DSCriterion(aFieldName, Data.scOperatorToDataOperator(anOperator), textValue);
				}
			}

			add(dsCriterion);
		}
	}

	private String criterionFieldValueName(int aValue)
	{
		return String.format("%s_%d", DS.CRITERIA_VALUE_ITEM_NAME, aValue);
	}

	/**
	 * Returns a {@link DataGrid} representation of this criteria.
	 * Use this method as a convenient way to flatten the hierarchy
	 * of criterion objects.
	 *
	 * @return Simple grid representation of the criteria.
	 */
	public DataGrid toGrid()
	{
		DSCriterion dsCriterion;
		int maxValueCount = maxCountOfValues();

		DataGrid dataGrid = new DataGrid(mName);
		dataGrid.addCol(new DataItem.Builder().name(DS.CRITERIA_BOOLEAN_ITEM_NAME).build());
		dataGrid.addCol(new DataItem.Builder().name(DS.CRITERIA_ENTRY_TYPE_NAME).build());
		dataGrid.addCol(new DataItem.Builder().name(DS.CRITERIA_ENTRY_ITEM_NAME).build());
		dataGrid.addCol(new DataItem.Builder().name(DS.CRITERIA_OPERATOR_ITEM_NAME).build());
		for (int val = 0; val < maxValueCount; val++)
			dataGrid.addCol(new DataItem.Builder().name(criterionFieldValueName(val+1)).build());

		for (DSCriterionEntry ce : mCriterionEntries)
		{
			dsCriterion = ce.getCriterion();

			dataGrid.newRow();
			dataGrid.setValueByName(DS.CRITERIA_BOOLEAN_ITEM_NAME, ce.getBooleanOperator().name());
			dataGrid.setValueByName(DS.CRITERIA_ENTRY_TYPE_NAME, Data.typeToString(dsCriterion.getType()));
			dataGrid.setValueByName(DS.CRITERIA_ENTRY_ITEM_NAME, dsCriterion.getName());
			dataGrid.setValueByName(DS.CRITERIA_OPERATOR_ITEM_NAME, dsCriterion.getLogicalOperator().name());
			int valueNumber = 1;
			if (dsCriterion.isMultiValue())
			{
				for (String fieldValue : dsCriterion.getValues())
					dataGrid.setValueByName(criterionFieldValueName(valueNumber++), fieldValue);
			}
			else
				dataGrid.setValueByName(criterionFieldValueName(valueNumber), dsCriterion.getValue());
			dataGrid.addRow();
		}

		return dataGrid;
	}

	/**
	 * Returns the name of the criteria.
	 *
	 * @return Criteria name.
	 */
	public String getName()
	{
		return mName;
	}

	/**
	 * Assigns the name of the criteria.
	 *
	 * @param aName Criteria name.
	 */
	public void setName(String aName)
	{
		mName = aName;
	}

	/**
	 * Assigns case sensitive flag - used for string comparison operators.
	 * The default is <i>true</i>.
	 *
	 * @param anIsCaseSensitive <i>true</i> or <i>false</i>
	 */
	public void setCaseSensitive(boolean anIsCaseSensitive)
	{
		mIsCaseSensitive = anIsCaseSensitive;
	}

	/**
	 * Returns the case sensitivity flag.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isCaseSensitive()
	{
		return mIsCaseSensitive;
	}

	/**
	 * Adds a criterion entry to the criteria.
	 *
	 * @param aDSCriterion Criterion instance.
	 */
	public void add(DSCriterion aDSCriterion)
	{
		mCriterionEntries.add(new DSCriterionEntry(aDSCriterion));
	}

	/**
	 * Adds the criterion instance to the criteria using the logical
	 * operator parameter.
	 *
	 * @param anOperator Logical operator.
	 * @param aDSCriterion Criterion instance.
	 */
	public void add(Data.Operator anOperator, DSCriterion aDSCriterion)
	{
		mCriterionEntries.add(new DSCriterionEntry(anOperator, aDSCriterion));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the {@link DataItem} parameter and then adds it to the
	 * criteria.
	 *
	 * <b>Note:</b>The default criterion operator for the criterion
	 * entry will be <i>EQUAL</i>.
	 *
	 * @param anItem Simple data item.
	 */
	public void add(DataItem anItem)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(anItem, Data.Operator.EQUAL)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * <b>Note:</b>The default criterion operator for the criterion
	 * entry will be <i>EQUAL</i>.
	 *
	 * @param aName Item name.
	 * @param aValue Item value.
	 */
	public void add(String aName, String aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, Data.Operator.EQUAL, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the {@link DataItem} parameter and then adds it to the
	 * criteria.
	 *
	 * @param anItem Simple item.
	 * @param anOperator Criterion operator.
	 */
	public void add(DataItem anItem, Data.Operator anOperator)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(anItem, anOperator)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public void add(String aName, Data.Operator anOperator, String aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues An array of values.
	 */
	public void add(String aName, Data.Operator anOperator, String... aValues)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValues)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public void add(String aName, Data.Operator anOperator, int aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.  This
	 * method is useful for defining an array of numbers with
	 * the <i>IN</i> operator.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValues Item values.
	 */
	public void add(String aName, Data.Operator anOperator, int... aValues)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValues)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public void add(String aName, Data.Operator anOperator, long aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public void add(String aName, Data.Operator anOperator, float aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public void add(String aName, Data.Operator anOperator, double aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue Item value.
	 */
	public void add(String aName, Data.Operator anOperator, Date aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria. This
	 * method is useful for defining an array of numbers with
	 * the <i>BETWEEN</i> operator.
	 *
	 * @param aName Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue1 First Date value.
	 * @param aValue2 Second Date value.
	 */
	public void add(String aName, Data.Operator anOperator, Date aValue1, Date aValue2)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND,
												   new DSCriterion(aName, anOperator, aValue1, aValue2)));
	}

	/**
	 * Convenience method that first constructs a criterion from
	 * the parameters and then adds it to the criteria.
	 *
	 * @param aName      Item name.
	 * @param anOperator Criterion operator.
	 * @param aValue     Item value.
	 */
	public void add(String aName, Data.Operator anOperator, boolean aValue)
	{
		mCriterionEntries.add(new DSCriterionEntry(Data.Operator.AND, new DSCriterion(aName, anOperator, aValue)));
	}

	/**
	 * Adds the criterion entry to the criteria using the boolean
	 * operator parameter.
	 *
	 * @param anOperator Boolean operator.
	 * @param aCriterionEntry Criterion entry.
	 */
	public void add(Data.Operator anOperator, DSCriterionEntry aCriterionEntry)
	{
		aCriterionEntry.setBooleanOperator(anOperator);
		mCriterionEntries.add(aCriterionEntry);
	}

	/**
	 * Adds the criterion entry to the criteria using the boolean
	 * operator parameter.
	 *
	 * <b>Note:</b>The default boolean operator for the criterion
	 * entry will be <i>AND</i>.
	 *
	 * @param aCriterionEntry Criterion entry.
	 */
	public void add(DSCriterionEntry aCriterionEntry)
	{
		add(Data.Operator.AND, aCriterionEntry);
	}

	/**
	 * Remove the criterion entry by its item name and logical operator.
	 *
	 * @param anItemName Item name
	 * @param anOperator Logical operator
	 */
	public void deleteByNameOperator(String anItemName, Data.Operator anOperator)
	{
		if (StringUtils.isNotEmpty(anItemName))
		{
			int offset = 0;
			DSCriterion dsCriterion;
			boolean isMatched = false;

			for (DSCriterionEntry ce : mCriterionEntries)
			{
				dsCriterion = ce.getCriterion();
				if ((dsCriterion.getName().equals(anItemName)) && (dsCriterion.getLogicalOperator() == anOperator))
				{
					isMatched = true;
					break;
				}
				offset++;
			}

			if (isMatched)
				mCriterionEntries.remove(offset);
		}
	}

	/**
	 * Scans the criteria for the first criterion entry that
	 * matches the item name.  If matched, then the criterion
	 * instance is returned.
	 *
	 * @param anItemName Item name to match.
	 *
	 * @return Data item instance or <i>null</i>.
	 */
	public DSCriterion getFirstCriterionByName(String anItemName)
	{
		if (StringUtils.isNotEmpty(anItemName))
		{
			for (DSCriterionEntry ce : mCriterionEntries)
			{
				if (ce.getName().equals(anItemName))
					return ce.getCriterion();
			}
		}

		return null;
	}

	/**
	 * Scans the criteria for the first criterion entry that
	 * matches the item name.  If matched, then the data item
	 * instance is returned.
	 *
	 * @param anItemName Item name to match.
	 *
	 * @return Data item instance or <i>null</i>.
	 */
	public DataItem getFirstItemByName(String anItemName)
	{
		if (StringUtils.isNotEmpty(anItemName))
		{
			for (DSCriterionEntry ce : mCriterionEntries)
			{
				if (ce.getName().equals(anItemName))
					return ce.getItem();
			}
		}

		return null;
	}

	/**
	 * Returns <i>true</i> if the complete criteria is simple in
	 * its nature.  A simple criteria is one where all of the
	 * boolean operators are <i>AND</i> and all values are
	 * single.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isSimple()
	{
		boolean isSimple = true;

		for (DSCriterionEntry ce : mCriterionEntries)
		{
			if (! ce.isSimple())
			{
				isSimple = false;
				break;
			}
		}
		return isSimple;
	}

	/**
	 * Returns the maximum number of values any criterion might
	 * have in the criteria.  If the criterion does not represent
	 * a multi-value, then the count for that entry would be one.
	 *
	 * @return Maximum count of values.
	 */
	public int maxCountOfValues()
	{
		int maxCount = 0;
		DSCriterion dsCriterion;

		for (DSCriterionEntry ce : mCriterionEntries)
		{
			dsCriterion = ce.getCriterion();
			maxCount = Math.max(maxCount, dsCriterion.count());
		}

		return maxCount;
	}

	/**
	 * Returns <i>true</i> if the complete criteria is advanced
	 * in its nature.  An advanced criteria is one where any of the
	 * boolean operators are <i>OR</i> or that there are entries
	 * with multi-values.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isAdvanced()
	{
		return !isSimple();
	}

	/**
	 * Clears the collection of criterion entries.
	 */
	public void reset()
	{
		mIsCaseSensitive = true;
		mCriterionEntries.clear();
		mFeatures.clear();
	}

	/**
	 * Returns the count of criterion entries in this criteria.
	 *
	 * @return Total criterion entries in this criteria.
	 */
	public int count()
	{
		return mCriterionEntries.size();
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
	 *     <li>The goal of the DSCriteria is to strike a balance between
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
