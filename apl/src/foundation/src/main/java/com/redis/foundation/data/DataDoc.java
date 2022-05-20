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

import com.redis.foundation.std.DigitalHash;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

/**
 * A DataDoc manages a collection of data items representing a document.
 */
public class DataDoc
{
	private String mName = StringUtils.EMPTY;
	private String mTitle = StringUtils.EMPTY;
	private String mAction = StringUtils.EMPTY;
	private transient HashMap<String, Object> mProperties;
	private HashMap<String, String> mFeatures = new HashMap<String, String>();
	private LinkedHashMap<String, DataItem> mItems = new LinkedHashMap<>();
	private LinkedHashMap<String, ArrayList<DataDoc>> mChildDocs = new LinkedHashMap<>();

	/**
	 * Constructs a data document based on the name.
	 *
	 * @param aName Name of document
	 */
	public DataDoc(String aName)
	{
		setName(aName);
	}

	/**
	 * Constructs a data document based on the name and title.
	 *
	 * @param aName Name of document
	 * @param aTitle Title of document
	 */
	public DataDoc(String aName, String aTitle)
	{
		setName(aName);
		setTitle(aTitle);
	}

	/**
	 * Clones and existing data document instance.
	 *
	 * @param aDataDoc Data document instance
	 */
	public DataDoc(final DataDoc aDataDoc)
	{
		if (aDataDoc != null)
		{
			setName(aDataDoc.getName());
			setTitle(aDataDoc.getTitle());
			aDataDoc.getItems().forEach(di -> {
				DataItem ndi = new DataItem(di);
				add(ndi);
			});
			aDataDoc.getChildDocsAsCollection().forEach(dd -> {
				DataDoc ndd = new DataDoc(dd);
				addChild(ndd);
			 });
			setFeatures(aDataDoc.getFeatures());
		}
	}

	/**
	 * Returns a string representation of a data item.
	 *
	 * @return String summary representation of this data item.
	 */
	@Override
	public String toString()
	{
		String ddString = String.format("%s [%d items]", mName, mItems.size());
		if (mChildDocs.size() > 0)
			ddString += String.format("[%d children]", mChildDocs.size());
		if (StringUtils.isNotEmpty(mTitle))
			ddString += String.format(", t = %s", mTitle);

		return ddString;
	}

	/**
	 * Assigns a name to the data document instance.
	 *
	 * @param aName Name of document
	 */
	public void setName(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
			mName = aName;
	}

	/**
	 * Returns the name of the data document.
	 *
	 * @return Name of document
	 */
	public String getName()
	{
		return mName;
	}

	/**
	 * Assigns a title to the data document instance.
	 *
	 * @param aTitle Title of the document
	 */
	public void setTitle(String aTitle)
	{
		if (StringUtils.isNotEmpty(aTitle))
			mTitle = aTitle;
	}

	/**
	 * Returns a title of the data document.
	 *
	 * @return Title of the document
	 */
	public String getTitle()
	{
		return mTitle;
	}

	/**
	 * Returns the action previously assigned to the data document instance.
	 *
	 * @return Action string
	 */
	public String getAction()
	{
		return mAction;
	}

	/**
	 * Assigns an action to the document.
	 *
	 * @param anAction Action string (e.g. "add", "update", "delete")
	 */
	public void setAction(String anAction)
	{
		if (StringUtils.isNotEmpty(anAction))
			mAction = anAction;
	}

	/**
	 * Returns a data item matching the name parameter.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item instance
	 *
	 * @throws NoSuchElementException If the name cannot be matched in the document
	 */
	public DataItem getItemByName(String aName)
		throws NoSuchElementException
	{
		DataItem dataItem = null;

		if (StringUtils.isNotEmpty(aName))
			dataItem = mItems.get(aName);

		if (dataItem == null)
			throw new NoSuchElementException(String.format("[%s]Unable to locate item by name: %s", mName, aName));
		else
			return dataItem;
	}

	/**
	 * Returns an optional data item matching the name parameter.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> getItemByNameOptional(String aName)
	{
		DataItem dataItem = null;

		if (StringUtils.isNotEmpty(aName))
			dataItem = mItems.get(aName);

		return Optional.ofNullable(dataItem);
	}

	/**
	 * Returns an optional data item matching the feature name parameter.
	 *
	 * @param aFeature Feature name to base enable check on
	 *
	 * @return Optional data item instance
	 */
	public Optional<DataItem> getItemByFeatureEnabledOptional(String aFeature)
	{
		DataItem featureDataItem = null;

		if (StringUtils.isNotEmpty(aFeature))
		{
			for (DataItem dataItem : mItems.values())
			{
				if (dataItem.isFeatureTrue(aFeature))
				{
					featureDataItem = dataItem;
					break;
				}
			}
		}

		return Optional.ofNullable(featureDataItem);
	}

	/**
	 * Returns a data item at the offset specified by the parameter.
	 *
	 * @param anOffset Offset of the data item
	 *
	 * @return Data item instance
	 *
	 * @throws ArrayIndexOutOfBoundsException Offset is out of bounds
	 */
	public DataItem getItemByOffset(int anOffset)
		throws ArrayIndexOutOfBoundsException
	{
		DataItem dataItem = null;

		if ((anOffset >= 0) && (anOffset < mItems.size()))
		{
			int offset = 0;
			for (DataItem di : mItems.values())
			{
				if (offset == anOffset)
				{
					dataItem = di;
					break;
				}
				else
					offset++;
			}
		}

		if (dataItem == null)
			throw new ArrayIndexOutOfBoundsException(Integer.toString(anOffset));
		else
			return dataItem;
	}

	/**
	 * Determines if there has been one or more values assigned to the data item
	 * identified by name.
	 *
	 * @param aName Name of item
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isValueAssigned(String aName)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		return optDataItem.map(DataItem::isValueAssigned).orElse(false);
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, String aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValue(aValue));
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValues List of values to assign to the data item
	 */
	public void setValuesByName(String aName, ArrayList<String> aValues)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValues(aValues));
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, char aValue)
	{
		setValueByName(aName, String.valueOf(aValue));
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, String aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(aValue));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public String getValueByName(String aName)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		if (optDataItem.isPresent())
			return optDataItem.get().getValue();
		else
			return StringUtils.EMPTY;
	}

	/**
	 * Returns a data item value list identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value list
	 *
	 */
	public ArrayList<String> getValuesByName(String aName)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		if (optDataItem.isPresent())
			return optDataItem.get().getValues();
		else
			return new ArrayList<String>();
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, Boolean aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValue(aValue));
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, char aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(String.valueOf(aValue)));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public char getValueAsChar(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		if (dataItem != null)
			return dataItem.getValueAsChar();
		else
			return StrUtl.CHAR_SPACE;
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, Boolean aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(aValue));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public Boolean getValueAsBoolean(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsBoolean();
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, Integer aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValue(aValue));
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, Integer aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(aValue));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public Integer getValueAsInteger(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsInteger();
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, Long aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValue(aValue));
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, Long aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(aValue));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public Long getValueAsLong(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsLong();
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, Float aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValue(aValue));
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, Float aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(aValue));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public Float getValueAsFloat(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsFloat();
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, Double aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.setValue(aValue));
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, Double aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		optDataItem.ifPresent(di -> di.addValue(aValue));
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public Double getValueAsDouble(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsDouble();
	}

	/**
	 * Assigns a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void setValueByName(String aName, Date aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			if (dataItem.getType() == Data.Type.Date)
				dataItem.setValue(Data.dateValueFormatted(aValue, Data.FORMAT_DATE_DEFAULT));
			else
				dataItem.setValue(Data.dateValueFormatted(aValue, Data.FORMAT_DATETIME_DEFAULT));
		}
	}

	/**
	 * Adds a value to the data item identified by the name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to assign to the data item
	 */
	public void addValueByName(String aName, Date aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			if (dataItem.getType() == Data.Type.Date)
				dataItem.addValue(Data.dateValueFormatted(aValue, Data.FORMAT_DATE_DEFAULT));
			else
				dataItem.addValue(Data.dateValueFormatted(aValue, Data.FORMAT_DATETIME_DEFAULT));
		}
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public Date getValueAsDate(String aName)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsDate();
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 * @param aDateFormat Simple date format
	 *
	 * @return Data item value
	 */
	public Date getValueAsDate(String aName, String aDateFormat)
	{
		DataItem dataItem = getItemByName(aName);
		return dataItem.getValueAsDate(aDateFormat);
	}

	/**
	 * Returns a data item value identified by a name.
	 *
	 * @param aName Name of the data item
	 *
	 * @return Data item value
	 */
	public long getValueAsTime(String aName)
	{
		return getValueAsDate(aName).getTime();
	}

	/**
	 * Determines if the parameter value is contained in the list of values
	 * for the data item identified by name.
	 *
	 * @param aName Name of the data item
	 * @param aValue Value to match
	 *
	 * @return <i>true</i> if the value matches the list of values
	 */
	public boolean isValueInByName(String aName, String aValue)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		if (optDataItem.isPresent())
		{
			DataItem dataItem = optDataItem.get();
			ArrayList<String> valueList = dataItem.getValues();
			return valueList.contains(aValue);
		}

		return false;
	}

	/**
	 * Determines if the parameter value is contained in the list of values
	 * for the data item identified by name.
	 *
	 * @param aName Name of the data item
	 * @param aValues List of values to match
	 *
	 * @return <i>true</i> if the value matches the list of values
	 */
	public boolean isValueInByName(String aName, ArrayList<String> aValues)
	{
		Optional<DataItem> optDataItem = getItemByNameOptional(aName);
		if ((optDataItem.isPresent()) && (aValues != null))
		{
			DataItem dataItem = optDataItem.get();
			ArrayList<String> valueList = dataItem.getValues();
			for (String value : aValues)
			{
				if (valueList.contains(value))
					return true;
			}
		}

		return false;
	}

	/**
	 * Convenience method that disables a feature among all items.
	 *
	 * @param aName Name of feature to disable.
	 */
	public void disableItemFeature(String aName)
	{
		String featureValue;

		for (DataItem dataItem : getItems())
		{
			featureValue = dataItem.getFeature(aName);
			if (StringUtils.isNotEmpty(featureValue))
				dataItem.disableFeature(aName);
		}
	}

	/**
	 * Convenience method that resets the value of all items to an empty string.
	 */
	public void resetValues()
	{
		for (DataItem dataItem : getItems())
			dataItem.clearValues();
		mChildDocs = new LinkedHashMap<>();
	}

	/**
	 * Convenience method that resets the value of all fields to either an
	 * empty string or a default value.
	 */
	public void resetValuesWithDefaults()
	{
		for (DataItem dataItem : getItems())
		{
			dataItem.clearValues();
			dataItem.assignValueFromDefault();
		}
	}

	/**
	 * Add an item to the data document.  If the item name already
	 * exists in the data document, it will be replaced.
	 *
	 * @param anItem Data item instance
	 */
	public void add(DataItem anItem)
	{
		mItems.put(anItem.getName(), anItem);
	}

	/**
	 * Update an item in the data document.
	 *
	 * @param anItem Data item instance
	 */
	public void update(DataItem anItem)
	{
		mItems.put(anItem.getName(), anItem);
	}

	/**
	 * Remove the data item identified by the name.
	 *
	 * @param aName Name of data item
	 */
	public void remove(String aName)
	{
		mItems.remove(aName);
	}

	/**
	 * Count of items being managed by the data document instance.
	 *
	 * @return Count of data items
	 */
	public int count()
	{
		return mItems.size();
	}

	/**
	 * Returns the list of data items being managed by the data document instance.
	 *
	 * @return Collection of data items.
	 */
	public Collection<DataItem> getItems()
	{
		return mItems.values();
	}

	/**
	 * Adds the data document instance as a child document identified by the name.
	 *
	 * @param aName Name of the child document
	 * @param aDoc Data document instance
	 */
	public void addChild(String aName, DataDoc aDoc)
	{
		if ((StringUtils.isNotEmpty(aName)) && (aDoc != null))
		{
			ArrayList<DataDoc> childDocs = mChildDocs.get(aName);
			if (childDocs == null)
				childDocs = new ArrayList<>();
			childDocs.add(aDoc);
			mChildDocs.put(aName, childDocs);
		}
	}

	/**
	 * Adds the data document instance as a child document.
	 *
	 * @param aDoc Data document instance
	 */
	public void addChild(DataDoc aDoc)
	{
		if (aDoc != null)
			addChild(aDoc.getName(), aDoc);
	}

	/**
	 * Delete the child document identified by the name parameter.
	 *
	 * @param aName Name of child document
	 */
	public void deleteChild(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
			mChildDocs.remove(aName);
	}

	/**
	 * Returns the count of child documents.
	 *
	 * @return Count of child documents
	 */
	public int childrenCount()
	{
		return mChildDocs.size();
	}

	/**
	 * Returns the list of child documents as a collection.
	 *
	 * @return Collection of data document instances
	 */
	public Collection<DataDoc> getChildDocsAsCollection()
	{
		Collection<DataDoc> dataDocList = new ArrayList<>();
		mChildDocs.entrySet().stream()
				  .forEach(e -> {
				  	for (DataDoc dataDoc : e.getValue())
				  		dataDocList.add(dataDoc);
				  });

		return dataDocList;
	}

	/**
	 * Returns the list of child documents that match the parameter name as a collection.
	 *
	 * @param aName Relationship name
	 * @return Collection of data document instances
	 */
	public Collection<DataDoc> getChildDocsAsCollection(String aName)
	{
		Collection<DataDoc> dataDocList = new ArrayList<>();
		if (StringUtils.isNotEmpty(aName))
		{
			mChildDocs.entrySet().stream()
					  .filter(e -> e.getKey().equals(aName))
					  .forEach(e -> {
						  for (DataDoc dataDoc : e.getValue())
							  dataDocList.add(dataDoc);
					  });
		}

		return dataDocList;
	}

	/**
	 * Returns the list of child documents as a list.
	 *
	 * @return List of data document instances
	 */
	public ArrayList<DataDoc> getChildDocsAsList()
	{
		ArrayList<DataDoc> dataDocList = new ArrayList<>();
		mChildDocs.entrySet().forEach(e -> {
			dataDocList.addAll(e.getValue());
		});

		return dataDocList;
	}

	/**
	 * Returns the list of child documents that match the parameter name as a list.
	 *
	 * @param aName Relationship name
	 * @return List of data document instances
	 */
	public ArrayList<DataDoc> getChildDocsAsList(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
			return mChildDocs.get(aName);

		return null;
	}

	/**
	 * Returns the list of child documents that match the parameter name as a collection.
	 *
	 * @param aName Relationship name
	 * @return Collection of data document instances
	 */
	public DataDoc getFirstChildDoc(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
		{
			ArrayList<DataDoc> childDocs = mChildDocs.get(aName);
			if (childDocs != null)
				return childDocs.get(0);
		}

		return null;
	}

	/**
	 * Returns a linked hash map of child documents.
	 *
	 * @return a linked hash map of child documents
	 */
	public LinkedHashMap<String, ArrayList<DataDoc>> getChildDocs()
	{
		return mChildDocs;
	}

	/**
	 * Retuns the data items as a stream.
	 *
	 * @return Data item stream
	 */
	public Stream<DataItem> stream()
	{
		List<DataItem> dataItemList = new ArrayList<DataItem>();
		mItems.forEach((s, dataItem) -> dataItemList.add(dataItem));

		return dataItemList.stream();
	}

	/**
	 * Process the data document information (name, fields, features) through
	 * the digital hash algorithm.
	 *
	 * @param aHash Digital hash instance.
	 * @param anIsFeatureIncluded Should features be included.
	 *
	 * @throws IOException Triggered by hash algorithm.
	 */
	public void processHash(DigitalHash aHash, boolean anIsFeatureIncluded)
		throws IOException
	{
		for (DataItem dataItem : getItems())
		{
			aHash.processBuffer(dataItem.getName());
			aHash.processBuffer(Data.typeToString(dataItem.getType()));
			aHash.processBuffer(dataItem.getTitle());
			if (anIsFeatureIncluded)
			{
				for (Map.Entry<String, String> featureEntry : getFeatures().entrySet())
				{
					aHash.processBuffer(featureEntry.getKey());
					aHash.processBuffer(featureEntry.getValue());
				}
			}
			if (dataItem.isMultiValue())
				aHash.processBuffer(dataItem.getValuesCollapsed());
			else
				aHash.processBuffer(dataItem.getValue());
		}
	}

	/**
	 * Generates a unique hash string using the MD5 algorithm using
	 * the data document item information.
	 *
	 * @param anIsFeatureIncluded Should feature name/values be included?
	 *
	 * @return Unique hash string.
	 */
	public String generateUniqueHash(boolean anIsFeatureIncluded)
	{
		String hashId;

		DigitalHash digitalHash = new DigitalHash();
		try
		{
			processHash(digitalHash, anIsFeatureIncluded);
			hashId = digitalHash.getHashSequence();
		}
		catch (IOException e)
		{
			UUID uniqueId = UUID.randomUUID();
			hashId = uniqueId.toString();
		}

		return hashId;
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, String aValue)
	{
		mFeatures.put(aName, aValue);
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, int aValue)
	{
		addFeature(aName, Integer.toString(aValue));
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, long aValue)
	{
		addFeature(aName, Long.toString(aValue));
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, float aValue)
	{
		addFeature(aName, Float.toString(aValue));
	}

	/**
	 * Add a unique feature to this document.  A feature enhances the core
	 * capability of the data document.
	 *
	 * @param aName Name of the feature.
	 * @param aValue Value to associate with the feature.
	 */
	public void addFeature(String aName, double aValue)
	{
		addFeature(aName, Double.toString(aValue));
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
	 * Count of unique features assigned to this document.
	 *
	 * @return Feature count.
	 */
	public int featureCount()
	{
		return mFeatures.size();
	}

	/**
	 * Returns the number of fields that match the feature name
	 * parameter.
	 *
	 * @param aName Feature name.
	 *
	 * @return Matching count.
	 */
	public int featureNameCount(String aName)
	{
		int nameCount = 0;

		for (DataItem dataItem : getItems())
		{
			if (StringUtils.isNotEmpty(dataItem.getFeature(aName)))
				nameCount++;
		}

		return nameCount;
	}

	/**
	 * Returns the number of items that match the feature name
	 * and value parameters.  The value is matched in a case
	 * insensitive manner.
	 *
	 * @param aName Feature name.
	 * @param aValue Feature value.
	 *
	 * @return Matching count.
	 */
	public int featureNameValueCount(String aName, String aValue)
	{
		String featureValue;
		int nameValueCount = 0;

		for (DataItem dataItem : getItems())
		{
			featureValue = dataItem.getFeature(aName);
			if (StringUtils.equalsIgnoreCase(featureValue, aValue))
				nameValueCount++;
		}

		return nameValueCount;
	}

	/**
	 * Returns the first feature that matches the name and has a
	 * non-null/empty item value. This can be useful for unique
	 * features like <i>Data.FEATURE_IS_PRIMARY</i>.
	 *
	 * @param aName Name of the unique feature
	 *
	 * @return Feature value or an empty string if it cannot be matched.
	 */
	public String featureFirstItemValue(String aName)
	{
		String itemValue = StringUtils.EMPTY;
		for (DataItem dataItem : getItems())
		{
			if (dataItem.isFeatureAssigned(aName))
			{
				itemValue = dataItem.getValue();
				if (StringUtils.isNotEmpty(itemValue))
					break;
			}
		}

		return itemValue;
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
	 * Returns the float associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public float getFeatureAsFloat(String aName)
	{
		return Data.createFloat(getFeature(aName));
	}

	/**
	 * Returns the double associated with the feature name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Feature value or <i>null</i>
	 */
	public double getFeatureAsDouble(String aName)
	{
		return Data.createDouble(getFeature(aName));
	}

	/**
	 * Removes all features assigned to this object instance.
	 */
	public void clearFeatures()
	{
		mFeatures.clear();
	}

	/**
	 * Assigns the hash map of features to the list.
	 *
	 * @param aFeatures Feature list.
	 */
	public void setFeatures(HashMap<String, String> aFeatures)
	{
		if (aFeatures != null)
			mFeatures = new HashMap<String, String>(aFeatures);
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
	 * Convenience method will identify a list of items that have a feature
	 * matching the parameter name.  If none are found, then list will be
	 * empty.
	 *
	 * @param aName Feature name.
	 *
	 * @return List of data items.
	 */
	public ArrayList<DataItem> getItemByFeatureName(String aName)
	{
		ArrayList<DataItem> dataItemList = new ArrayList<>();

		for (DataItem dataItem : getItems())
		{
			if (StringUtils.isNotEmpty(dataItem.getFeature(aName)))
				dataItemList.add(dataItem);
		}

		return dataItemList;
	}

	/**
	 * Convenience method will identify the first item that has a feature
	 * matching the parameter name.
	 *
	 * @param aName Feature name.
	 *
	 * @return Optional of data item.
	 */
	public Optional<DataItem> getFirstItemByFeatureNameOptional(String aName)
	{
		DataItem dataItem = null;

		for (DataItem di : getItems())
		{
			if (StringUtils.isNotEmpty(di.getFeature(aName)))
			{
				dataItem = di;
				break;
			}
		}

		return Optional.ofNullable(dataItem);
	}

	/**
	 * Convenience method will identify the first item that has a feature
	 * matching the parameter name and a value assigned.
	 *
	 * @param aName Feature name.
	 *
	 * @return Optional of data item.
	 */
	public Optional<DataItem> getFirstItemByFeatureNameWithValueOptional(String aName)
	{
		DataItem dataItem = null;

		for (DataItem di : getItems())
		{
			if (StringUtils.isNotEmpty(di.getFeature(aName)))
			{
				if (di.isValueAssigned())
				{
					dataItem = di;
					break;
				}
			}
		}

		return Optional.ofNullable(dataItem);
	}

	/**
	 * Convenience method will identify which field represents the primary
	 * key in the data document.  If non are found, then <i>null</i> is
	 * returned.
	 *
	 * @return Optional of primary key item.
	 */
	public Optional<DataItem> getPrimaryKeyItemOptional()
	{
		return getFirstItemByFeatureNameOptional(Data.FEATURE_IS_PRIMARY);
	}

	/**
	 * Add an application defined property to the data document.
	 * <p>
	 * <b>Notes:</b>
	 * </p>
	 * <ul>
	 *     <li>The goal of the DataDoc is to strike a balance between
	 *     providing enough properties to adequately model application
	 *     related data without overloading it.</li>
	 *     <li>This method offers a mechanism to capture additional
	 *     (application specific) properties that may be needed.</li>
	 *     <li>Properties added with this method are transient and
	 *     will not be stored when saved or cloned.</li>
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
	 * Updates the property by name with the object instance.
	 *
	 * @param aName Name of the property
	 * @param anObject Instance of an object
	 */
	public void updateProperty(String aName, Object anObject)
	{
		if (mProperties == null)
			mProperties = new HashMap<String, Object>();
		mProperties.put(aName, anObject);
	}

	/**
	 * Removes a property from the data item.
	 *
	 * @param aName Name of the property
	 */
	public void deleteProperty(String aName)
	{
		if (mProperties != null)
			mProperties.remove(aName);
	}

	/**
	 * Returns an Optional for an object associated with the property name.
	 *
	 * @param aName Name of the property.
	 * @return Optional instance of an object.
	 */
	public Optional<Object> getProperty(String aName)
	{
		if (mProperties == null)
			return Optional.empty();
		else
			return Optional.ofNullable(mProperties.get(aName));
	}

	/**
	 * Removes all application defined properties assigned to this data document.
	 */
	public void clearDocProperties()
	{
		if (mProperties != null)
			mProperties.clear();
	}

	/**
	 * Convenience method that removes all properties for each data item
	 * in the document.
	 */
	public void clearItemProperties()
	{
		for (DataItem dataItem : getItems())
			dataItem.clearProperties();
	}

	/**
	 * Returns the property map instance managed by the data document or <i>null</i>
	 * if empty.
	 *
	 * @return Hash map instance.
	 */
	public HashMap<String, Object> getProperties()
	{
		return mProperties;
	}

	/**
	 * Convenience method examines all of the items in the document to determine if
	 * they are valid. A validation check ensures values are assigned when required
	 * and do not exceed range limits (if assigned).
	 * <p>
	 * <b>Note:</b> If a item fails the validation check, then a property called
	 * <i>Field.VALIDATION_PROPERTY_NAME</i> will be assigned a relevant message.
	 * </p>
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isValid()
	{
		boolean isValid = true;

		clearItemProperties();
		for (DataItem dataItem : getItems())
		{
			if (! dataItem.isValid())
				isValid = false;
		}

		return isValid;
	}

	/**
	 * Creates a list of validation messages for fields that failed
	 * the <code>isValid()</code> check.
	 *
	 * @return List of failed validation messages.
	 */
	public ArrayList<String> getValidationMessages()
	{
		Optional<Object> propertyMessage;

		ArrayList<String> messageList = new ArrayList<String>();
		for (DataItem dataItem : getItems())
		{
			propertyMessage = dataItem.getProperty(Data.VALIDATION_PROPERTY_NAME);
			if (propertyMessage.isPresent())
				messageList.add(String.format("%s: %s", dataItem.getName(), propertyMessage.toString()));
		}

		return messageList;
	}

	/**
	 * Will compare each item of the current document against the items within the
	 * document provided as a parameter.
	 * <p>
	 * <b>Note:</b> If an item is found to differ, then a property called
	 * <i>Data.VALIDATION_FIELD_CHANGED</i> will be assigned a relevant message.
	 * </p>
	 *
	 * @param aDataDoc Document of items to compare.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isItemValuesEqual(DataDoc aDataDoc)
	{
		DataItem dataItem;

		clearItemProperties();
		boolean isEqual = true;
		for (DataItem docItem : getItems())
		{
			try
			{
				dataItem = aDataDoc.getItemByName(docItem.getName());
				if (! dataItem.isEqual(docItem))
				{
					isEqual = false;
					addProperty(Data.VALIDATION_ITEM_CHANGED, String.format("%s: %s", dataItem.getName(), Data.VALIDATION_MESSAGE_ITEM_CHANGED));
				}
			}
			catch (NoSuchElementException e)
			{
				isEqual = false;
				addProperty(Data.VALIDATION_ITEM_CHANGED, String.format("%s: Name does not exist", docItem.getName()));
			}
		}

		return isEqual;
	}

	/**
	 * Indicates whether some other object is "equal to" this one.
	 *
	 * @param anObject Reference object with which to compare.
	 *
	 * @return  {@code true} if this object is the same as the anObject
	 *          argument; {@code false} otherwise.
	 */
	@Override
	public boolean equals(Object anObject)
	{
		if (! (anObject instanceof DataDoc))
			return false;
		DataDoc dataDoc = (DataDoc) anObject;
		String objMD5 = dataDoc.generateUniqueHash(true);
		String thisMD5 = this.generateUniqueHash(true);

		return thisMD5.equals(objMD5);
	}

	/**
	 * Returns a hash code value for the object. This method is
	 * supported for the benefit of hash tables such as those provided by
	 * {@link java.util.HashMap}.
	 *
	 * @return A hash code value for this object.
	 */
	@Override
	public int hashCode()
	{
		return new HashCodeBuilder().append(mName).append(mTitle).append(mItems).append(mFeatures).toHashCode();
	}
}
