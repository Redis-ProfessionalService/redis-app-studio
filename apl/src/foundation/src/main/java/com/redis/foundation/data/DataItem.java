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

import com.redis.foundation.std.DatUtl;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.math.NumberUtils;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * A data item captures a type, name, title, features, values and transient properties.
 * Data items can be used to describe schema meta data and manage the serialization of
 * data values.  The Builder class can be used to quickly construct new data item instances.
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataItem
{
    private int mStoredSize;
    private int mDisplaySize;
    private DataRange mRange;
    private String mName = StringUtils.EMPTY;
    private String mTitle = StringUtils.EMPTY;
    private String mUIFormat = StringUtils.EMPTY;                   // SC UI output format
    private Data.Type mType = Data.Type.Undefined;
    private String mDataFormat = StringUtils.EMPTY;                 // Parsing and console output format
    private String mDefaultValue = StringUtils.EMPTY;
    private Data.Order mSortOrder = Data.Order.UNDEFINED;
    private transient HashMap<String, Object> mProperties;
    private ArrayList<String> mValues = new ArrayList<String>();
    private HashMap<String, String> mFeatures = new HashMap<String, String>();

    /**
     * Constructs a data item based on the type and name.
     *
     * @param aType Data type.
     * @param aName Item name.
     */
    public DataItem(Data.Type aType, String aName)
    {
        setType(aType);
        setName(aName);
        enableFeature(Data.FEATURE_IS_VISIBLE);
    }

    /**
     * Constructs a Text data item based on the item name and title.
     *
     * @param aName Item name.
     * @param aTitle Item title.
     */
    public DataItem(String aName, String aTitle)
    {
        setName(aName);
        setTitle(aTitle);
        enableFeature(Data.FEATURE_IS_VISIBLE);
    }

    /**
     * Constructs a data item based on the item name and generic object value.
     * The logic will recognize a text value as being multi-valued if it contains
     * pipe '|' delimiter character within it and extract the values accordingly.
     *
     * @param aName Item name.
     * @param aValue Object value.
     */
    public DataItem(String aName, Object aValue)
    {
        if (aValue != null)
        {
            setName(aName);
            setTitle(Data.nameToTitle(aName));
            setType(Data.getTypeByObject(aValue));
            if (mType == Data.Type.Text)
            {
                String strValue = aValue.toString();
                if (StringUtils.containsAny(strValue, StrUtl.CHAR_PIPE))
                    expandAndSetValues(strValue);
                else
                    setValue(strValue);
            }
            else
                setValue(aValue.toString());
            enableFeature(Data.FEATURE_IS_VISIBLE);
        }
    }

    /**
     * Clones an existing data item instance.
     *
     * @param aDataItem Data item instance.
     */
    public DataItem(final DataItem aDataItem)
    {
        mValues = new ArrayList<String>();

        if (aDataItem != null)
        {
            setType(aDataItem.getType());
            setName(aDataItem.getName());
            setTitle(aDataItem.getTitle());
            setUIFormat(aDataItem.getUIFormat());
            setDataFormat(aDataItem.getDataFormat());
            setSortOrder(aDataItem.getSortOrder());
            setStoredSize(aDataItem.getStoredSize());
            setDisplaySize(aDataItem.getDisplaySize());
            setDefaultValue(aDataItem.getDefaultValue());
            if (aDataItem.isRangeAssigned())
                setRange(new DataRange(aDataItem.getRange()));
            setValues(aDataItem.getValues());

            this.mFeatures = new HashMap<String, String>(aDataItem.getFeatures());
            if (aDataItem.mProperties != null)
            {
                mProperties = new HashMap<>();
                aDataItem.mProperties.forEach(this::addProperty);
            }
        }
    }

    private DataItem(Builder aBuilder)
    {

// Assign our features before we assign our data type because they influence option assignments.

        if (aBuilder.mIsStored)
            enableFeature(Data.FEATURE_IS_STORED);
        if (aBuilder.mIsVisible)
            enableFeature(Data.FEATURE_IS_VISIBLE);
        if (aBuilder.mIsHidden)
            enableFeature(Data.FEATURE_IS_HIDDEN);
        if (aBuilder.mIsRequired)
            enableFeature(Data.FEATURE_IS_REQUIRED);
        if (aBuilder.mIsPrimary)
            enableFeature(Data.FEATURE_IS_PRIMARY);
        if (aBuilder.mIsSecret)
            enableFeature(Data.FEATURE_IS_SECRET);
        if (aBuilder.mIsCurrency)
            enableFeature(Data.FEATURE_IS_CURRENCY);
        if (aBuilder.mIsSuggest)
            enableFeature(Data.FEATURE_IS_SUGGEST);

        setName(aBuilder.mName);
        if ((aBuilder.mType == Data.Type.Undefined) && (aBuilder.mValues.size() > 0))
            setType(Data.Type.Text);
        else
            setType(aBuilder.mType);
        setTitle(aBuilder.mTitle);
        setStoredSize(aBuilder.mStoredSize);
        setDisplaySize(aBuilder.mDisplaySize);
        if (StringUtils.isNotEmpty(aBuilder.mUIFormat))
            setUIFormat(aBuilder.mUIFormat);
        if (StringUtils.isNotEmpty(aBuilder.mDataFormat))
            setDataFormat(aBuilder.mDataFormat);
        setDefaultValue(aBuilder.mDefaultValue);
        if (aBuilder.mRange != null)
            setRange(aBuilder.mRange);
        if (aBuilder.mValues.size() > 0)
            setValues(aBuilder.mValues);
    }

    /**
     * Returns a string representation of a data item.
     *
     * @return String summary representation of this data item.
     */
    @Override
    public String toString()
    {
        String diString = String.format("[%s] n = %s", Data.typeToString(mType), mName);
        if (isValueAssigned())
            diString += String.format(", v = %s", StrUtl.collapseToSingle(mValues, StrUtl.CHAR_PIPE));
        if (StringUtils.isNotEmpty(mDefaultValue))
            diString += String.format(", dv = %s", mDefaultValue);
        if (StringUtils.isNotEmpty(mTitle))
            diString += String.format(", t = %s", mTitle);

        return diString;
    }

    /**
     * Returns the stored size for the data item.
     *
     * @return Stored size.
     */
    public int getStoredSize()
    {
        return mStoredSize;
    }

    /**
     * Assigns the stored size for the data item.
     *
     * @param aStoredSize Stored size.
     */
    public void setStoredSize(int aStoredSize)
    {
        mStoredSize = aStoredSize;
    }

    /**
     * Returns the display size for the data item.
     *
     * @return Display size.
     */
    public int getDisplaySize()
    {
        return mDisplaySize;
    }

    /**
     * Assigns the display size for the data item.
     *
     * @param aDisplaySize Display size.
     */
    public void setDisplaySize(int aDisplaySize)
    {
        mDisplaySize = aDisplaySize;
    }

    /**
     * Returns the sort order preference of the item.
     *
     * @return Sort order preference.
     */
    public Data.Order getSortOrder()
    {
        return mSortOrder;
    }

    /**
     * Assigns a sort order preference to the item.
     *
     * @param aSortOrder Sort order.
     */
    public void setSortOrder(Data.Order aSortOrder)
    {
        mSortOrder = aSortOrder;
    }

    /**
     * Returns <i>true</i> if the item is sorted or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isSorted()
    {
        return mSortOrder != Data.Order.UNDEFINED;
    }

    /**
     * Returns the title of the data item.
     *
     * @return  Data item title.
     */
    public String getTitle()
    {
        return mTitle;
    }

    /**
     * Assigns a data item title.
     *
     * @param aTitle Data item title.
     */
    public void setTitle(String aTitle)
    {
        if (StringUtils.isNotEmpty(aTitle))
            mTitle = aTitle;
    }

    /**
     * Returns the name of the data item.
     *
     * @return  Data item name.
     */
    public String getName()
    {
        return this.mName;
    }

    /**
     * Assigns a data item name.
     *
     * @param aName Name of the data item.
     */
    public void setName(String aName)
    {
        if (StringUtils.isNotEmpty(aName))
            mName = aName;
    }

    /**
     * Returns the data type of the item.
     *
     * @return Data type enumerated value.
     */
    public Data.Type getType()
    {
        return mType;
    }

    /**
     * Assigns the data type for the item.
     *
     * @param aType Data type.
     */
    public void setType(Data.Type aType)
    {
        mType = aType;
        if (StringUtils.isEmpty(mDataFormat))
        {
            switch (aType)
            {
                case Integer:
                case Long:
                    if (isFeatureTrue(Data.FEATURE_IS_CURRENCY))
                    {
                        mUIFormat = "&#x00A4;,0";
                        mDataFormat = Data.FORMAT_INTEGER_CURRENCY_COMMA;
                    }
                    else
                    {
                        if (StringUtils.containsIgnoreCase(mName, "year"))
                        {
                            mUIFormat = "#";
                            mDataFormat = "#";
                        }
                        else
                        {
                            mUIFormat = ",0";
                            mDataFormat = Data.FORMAT_INTEGER_COMMA;
                        }
                    }
                    break;
                case Float:
                case Double:
                    if (isFeatureTrue(Data.FEATURE_IS_CURRENCY))
                    {
                        mUIFormat = "&#x00A4;,0.00";
                        mDataFormat = Data.FORMAT_DOUBLE_CURRENCY_COMMA_POINT;
                    }
                    else
                    {
                        mUIFormat = ",0.00";
                        mDataFormat = Data.FORMAT_DOUBLE_COMMA_POINT;
                    }
                    break;
                case Date:
                    mDataFormat = Data.FORMAT_DATE_DEFAULT;
                    break;
                case DateTime:
                    mDataFormat = Data.FORMAT_DATETIME_DEFAULT;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Returns the data format string for the item.  The data format is
     * referenced when data is being parsed or presented during a console
     * display.
     *
     * @return Data ormat string
     */
    public String getDataFormat()
    {
        return mDataFormat;
    }

    /**
     * Assigns a data formatting string for item.  The data format is
     * referenced when data is being parsed or presented during a console
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
        mDataFormat = aDataFormat;
    }

    /**
     * Returns the UI format string for the item.  The UI format is
     * referenced when data is being parsed or presented in a UI
     * framework.
     *
     * @return Data ormat string
     */
    public String getUIFormat()
    {
        return mUIFormat;
    }

    /**
     * Assigns a data formatting string for item.  The UI format is
     * referenced when data is being parsed or presented in a UI
     * framework. The format is based on DecimalFormat (numbers) and
     * SimpleDateFormat (date/time). Therefore, your assignment is type
     * dependent.
     *
     * @see <a href="https://www.smartclient.com/smartclient-12.0/isomorphic/system/reference/?id=type..FormatString">SmartClient Formatting</a>
     *
     * @param aUIFormat UI format string based on UI presentation
     */
    public void setUIFormat(String aUIFormat)
    {
        mUIFormat = aUIFormat;
    }

    /**
     * If a {@link DataRange} instance was previously assigned,
     * then this method will return a reference to its instance.
     *
     * @return DataRange if previously assigned or <i>null</i> otherwise.
     */
    public DataRange getRange()
    {
        return mRange;
    }

    /**
     * Assign a {@link DataRange} to the data itme.  A data range
     * is used by the validation methods to determine if a data value
     * falls within a min and max range or a enumerated list of options.
     *
     * @param aRange DataRange instance reference.
     */
    public void setRange(DataRange aRange)
    {
        mRange = new DataRange(aRange);
    }

    /**
     * Creates a {@link DataRange} instance and assigns the <i>String</i>
     * array to it.
     *
     * @param aStrArgs Array of string values.
     */
    public void setRange(String... aStrArgs)
    {
        if (Data.isText(mType))
            mRange = new DataRange(aStrArgs);
    }

    /**
     * Creates a {@link DataRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(int aMin, int aMax)
    {
        if (Data.isNumber(mType))
            mRange = new DataRange(aMin, aMax);
    }

    /**
     * Creates a {@link DataRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(long aMin, long aMax)
    {
        if (Data.isNumber(mType))
            mRange = new DataRange(aMin, aMax);
    }

    /**
     * Creates a {@link DataRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(double aMin, double aMax)
    {
        if (Data.isNumber(mType))
            mRange = new DataRange(aMin, aMax);
    }

    /**
     * Creates a {@link DataRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(Date aMin, Date aMax)
    {
        if (Data.isDateOrTime(mType))
            mRange = new DataRange(aMin, aMax);
    }

    /**
     * Creates a {@link DataRange} instance and assigns the minimum
     * and maximum parameters to it.
     *
     * @param aMin Minimum value of the range.
     * @param aMax Maximum value of the range.
     */
    public void setRange(Calendar aMin, Calendar aMax)
    {
        if (Data.isDateOrTime(mType))
            mRange = new DataRange(aMin, aMax);
    }

    /**
     * Removes the {@link DataRange} assignment from the item.
     */
    public void clearRange()
    {
        mRange = null;
    }

    /**
     * Returns <i>true</i> if the item has a {@link DataRange}
     * assigned to it or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isRangeAssigned()
    {
        return mRange != null;
    }

    /**
     * Returns <i>true</i> if the data item represents a multi-value or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isMultiValue()
    {
        return mValues.size() > 1;
    }

    /**
     * Returns <i>true</i> if the data uten is empty or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueEmpty()
    {
        return StringUtils.isEmpty(getValue());
    }

    /**
     * Returns <i>true</i> if the data uten is not empty or <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueNotEmpty()
    {
        return StringUtils.isNotEmpty(getValue());
    }

    /**
     * Assigns the value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(String aValue)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            mValues.clear();
            mValues.add(aValue);
            if (mType == Data.Type.Undefined)
                setType(Data.Type.Text);
            enableFeature(Data.FEATURE_IS_UPDATED);
        }
    }

    /**
     * Returns a <i>String</i> representation of the data item value.
     *
     * @return <i>String</i> representation of the data item value.
     */
    public String getValue()
    {
        if (isValueAssigned())
            return mValues.get(0);

        return StringUtils.EMPTY;
    }

    /**
     * Returns a value as a character.
     *
     * @return Data item value
     */
    public char getValueAsChar()
    {
        if (isValueAssigned())
            return getValue().charAt(0);
        else
            return StrUtl.CHAR_SPACE;
    }

    /**
     * Returns a string representation of the value with a format
     * applied.
     *
     * @see <a href="https://www.baeldung.com/java-number-formatting">Number Formatting in Java</a>
     * @see <a href="http://tutorials.jenkov.com/java-internationalization/simpledateformat.html">Java SimpleDateFormat</a>
     * @see <a href="https://www.baeldung.com/java-simple-date-format">A Guide to SimpleDateFormat</a>
     *
     * @param aFormat Format string based on data type
     *
     * @return Formatted value
     */
    public String getValueFormatted(String aFormat)
    {
        String formattedValue;
        String rawValue = getValue();

        if ((StringUtils.isEmpty(rawValue)) || (StringUtils.isEmpty(aFormat)))
            formattedValue = rawValue;
        else
        {
            if (Data.isNumber(mType))
            {
                DecimalFormat decimalFormat = new DecimalFormat(aFormat);
                formattedValue = decimalFormat.format(Data.createDouble(rawValue));
            }
            else
                formattedValue = rawValue;
        }

        return formattedValue;
    }

    /**
     * Returns a string representation of the value with a format
     * applied.
     *
     * @return Formatted value
     */
    public String getValueFormatted()
    {
        return getValueFormatted(mDataFormat);
    }

    /**
     * Adds the value parameter to the data item.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValue(String aValue)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            mValues.add(aValue);
            if (mType == Data.Type.Undefined)
                setType(Data.Type.Text);
            enableFeature(Data.FEATURE_IS_UPDATED);
        }
    }

    /**
     * Adds the value parameter to the data item if it is multi-value
     * and ensure that it is unique.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValueUnique(String aValue)
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            if (! mValues.contains(aValue))
            {
                mValues.add(aValue);
                enableFeature(Data.FEATURE_IS_UPDATED);
            }
            if (mType == Data.Type.Undefined)
                setType(Data.Type.Text);
        }
    }

    /**
     * Assigns the value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Boolean aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Boolean);
        setValue(StrUtl.booleanToString(aValue));
    }

    /**
     * Adds the value parameter to the data item.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValue(Boolean aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Boolean);
        addValue(aValue.toString());
    }

    public Boolean getValueAsBoolean()
    {
        return StrUtl.stringToBoolean(getValue());
    }

    /**
     * Returns the list of data values as a <i>Boolean</i> type.
     *
     * @return Values of the data item.
     */
    public ArrayList<Boolean> getValuesAsBoolean()
    {
        ArrayList<Boolean> booleanList = new ArrayList<>();
        getValues().forEach(v -> booleanList.add(StrUtl.stringToBoolean(v)));

        return booleanList;
    }

    /**
     * Returns <i>true</i> if the data value evaluates as true or
     * <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueTrue()
    {
        return Data.isValueTrue(getValue());
    }

    /**
     * Returns <i>false</i> if the data value evaluates as false or
     * <i>true</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueFalse()
    {
        return !Data.isValueTrue(getValue());
    }

    /**
     * Assigns the value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Integer aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Integer);
        setValue(aValue.toString());
    }

    /**
     * Adds the value parameter to the data item.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValue(Integer aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Integer);
        addValue(aValue.toString());
    }

    /**
     * Returns the data value as an <i>int</i> type.
     *
     * @return Value of the data item.
     */
    public Integer getValueAsInteger()
    {
        String strValue = getValue();
        if (StringUtils.containsIgnoreCase(strValue, "E"))
            return Double.valueOf(strValue).intValue();
        else
            return Data.createIntegerObject(getValue());
    }

    /**
     * Returns the list of data values as a <i>Integer</i> type.
     *
     * @return Values of the data item.
     */
    public ArrayList<Integer> getValuesAsInteger()
    {
        ArrayList<Integer> integerList = new ArrayList<>();
        getValues().forEach(v -> integerList.add(Data.createIntegerObject(v)));

        return integerList;
    }

    /**
     * Assigns the value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Long aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Long);
        setValue(aValue.toString());
    }

    /**
     * Adds the value parameter to the data item.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValue(Long aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Long);
        addValue(aValue.toString());
    }

    /**
     * Returns the item value as a <i>Long</i> type.
     *
     * @return Value of the item.
     */
    public Long getValueAsLong()
    {
        String strValue = getValue();
        if (StringUtils.containsIgnoreCase(strValue, "E"))
            return Double.valueOf(strValue).longValue();
        else
            return Data.createLongObject(strValue);
    }

    /**
     * Returns the list of data values as a <i>Long</i> type.
     *
     * @return Values of the data item.
     */
    public ArrayList<Long> getValuesAsLong()
    {
        ArrayList<Long> longList = new ArrayList<>();
        getValues().forEach(v -> longList.add(Data.createLongObject(v)));

        return longList;
    }

    /**
     * Assigns the value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Double aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Double);
        setValue(aValue.toString());
    }

    /**
     * Adds the value parameter to the data item.
     *
     * @param aValue A value that is formatted appropriately for
     *               the data type it represents.
     */
    public void addValue(Double aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Double);
        addValue(aValue.toString());
    }

    /**
     * Returns the data value as a <i>Double</i> type.
     *
     * @return Value of the data item.
     */
    public Double getValueAsDouble()
    {
        return Data.createDoubleObject(getValue());
    }

    /**
     * Returns the list of data values as a <i>Double</i> type.
     *
     * @return Values of the data item.
     */
    public ArrayList<Double> getValuesAsDouble()
    {
        ArrayList<Double> doubleList = new ArrayList<>();
        getValues().forEach(v -> doubleList.add(Data.createDoubleObject(v)));

        return doubleList;
    }

    /**
     * Assigns the value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Float aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Float);
        setValue(aValue.toString());
    }

    /**
     * Adds the value parameter to the list of data item values.
     *
     * @param aValue Value to assign.
     */
    public void addValue(Float aValue)
    {
        if (mType == Data.Type.Undefined)
            setType(Data.Type.Float);
        addValue(aValue.toString());
    }

    /**
     * Returns the data value as a <i>Float</i> type.
     *
     * @return Value of the data item.
     */
    public Float getValueAsFloat()
    {
        return Data.createFloatObject(getValue());
    }

    /**
     * Returns the list of data values as a <i>Float</i> type.
     *
     * @return Values of the data item.
     */
    public ArrayList<Float> getValuesAsFloat()
    {
        ArrayList<Float> floatList = new ArrayList<>();
        getValues().forEach(v -> floatList.add(Data.createFloatObject(v)));

        return floatList;
    }

    /**
     * Assigns the value parameter to the item.
     *
     * @param aValue Value to assign.
     */
    public void setValue(Date aValue)
    {
        if (aValue != null)
        {
            if (StringUtils.isNotEmpty(mDataFormat))
            {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(mDataFormat);
                setValue(simpleDateFormat.format(aValue.getTime()));
            }
            else
                setValue(aValue.toString());
        }
    }

    /**
     * Returns the item value as a <i>Date</i> type.
     *
     * @return Value of the field.
     */
    public Date getValueAsDate()
    {
        return Data.createDate(getValue(), mDataFormat);
    }

    /**
     * Returns the item value as a <i>Date</i> type.
     *
     * @param aDateFormat Simple data format string
     *
     * @return Value of the field.
     */
    public Date getValueAsDate(String aDateFormat)
    {
        return Data.createDate(getValue(), aDateFormat);
    }

    /**
     * Returns the list of data values as a <i>Date</i> type.
     *
     * @return Values of the data item.
     */
    public ArrayList<Date> getValuesAsDate()
    {
        ArrayList<Date> dateList = new ArrayList<>();
        getValues().forEach(v -> dateList.add(Data.createDate(v, mDataFormat)));

        return dateList;
    }

    /**
     * Assigns a list of values to the data item.
     *
     * @param aValues List of string values.
     */
    public void setValues(ArrayList<String> aValues)
    {
        if (aValues != null)
        {
            mValues = new ArrayList<String>(aValues);
            if (mType == Data.Type.Undefined)
                setType(Data.Type.Text);
            enableFeature(Data.FEATURE_IS_UPDATED);
        }
    }

    /**
     * Returns a list of data item values.
     *
     * @return List of data item values.
     */
    public ArrayList<String> getValues()
    {
        return mValues;
    }

    /**
     * Returns an array of data item values.
     *
     * @return Array of data item values.
     */
    public String[] getValuesArray()
    {
        String[] strValues = new String[mValues.size()];
        strValues = mValues.toArray(strValues);

        return strValues;
    }

    /**
     * Returns the list of values as a collapsed string with the separator character
     * being used as a delimiter.
     *
     * @param aSeparator Separator character
     *
     * @return String of collapsed values
     */
    public String getValuesCollapsed(char aSeparator)
    {
        return StrUtl.collapseToSingle(mValues, aSeparator);
    }

    /**
     * Returns the list of values as a collapsed string with a default separator character
     * being used as a delimiter.
     *
     * @return String of collapsed values
     */
    public String getValuesCollapsed()
    {
        return getValuesCollapsed(StrUtl.CHAR_PIPE);
    }

    /**
     * Expand the value string into a list of individual values
     * using the delimiter character to identify each one.
     *
     * @param aValue One or more values separated by a
     *               delimiter character.
     * @param aSeparator Separator character.
     */
    public void expandAndSetValues(String aValue, char aSeparator)
    {
        setValues(StrUtl.expandToList(aValue, aSeparator));
    }

    /**
     * Expand the value string into a list of individual values
     * using the default delimiter character to identify each one.
     *
     * @param aValue One or more values separated by a
     *               delimiter character.
     */
    public void expandAndSetValues(String aValue)
    {
        setValues(StrUtl.expandToList(aValue, StrUtl.CHAR_PIPE));
    }

    /**
     * Identifies if there has been one or more values assigned to the data item.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueAssigned()
    {
        return mValues.size() > 0;
    }

    /**
     * Convenience method that returns the item value as an <i>Object</i> type.
     * For example, if your field represents an <i>int</i> value, then the method
     * will return an object representing an <i>Integer</i>.
     *
     * @return Object instance representation of the field.
     */
    public Object getValueAsObject()
    {
        switch (mType)
        {
            case Integer:
                return Data.createIntegerObject(getValue());
            case Long:
                return Data.createLongObject(getValue());
            case Float:
                return Data.createFloatObject(getValue());
            case Double:
                return Data.createDoubleObject(getValue());
            case Boolean:
                return Data.isValueTrue(getValue());
            case Date:
            case DateTime:
                return Data.createDate(getValue(), mDataFormat);
            default:
                if (isMultiValue())
                    return getValuesCollapsed();
                else
                    return getValue();
        }
    }

    /**
     * Convenience method that returns the item value as an <i>Object</i> type.
     * For example, if your field represents an <i>int</i> value, then the method
     * will return an object representing an <i>Integer</i>.
     *
     * @param aDelimiterChar Delimiter character.
     *
     * @return Object instance representation of the field.
     */
    public Object getValueAsObject(char aDelimiterChar)
    {
        switch (mType)
        {
            case Integer:
                return Data.createIntegerObject(getValue());
            case Long:
                return Data.createLongObject(getValue());
            case Float:
                return Data.createFloatObject(getValue());
            case Double:
                return Data.createDoubleObject(getValue());
            case Boolean:
                return Data.isValueTrue(getValue());
            case Date:
            case DateTime:
                return Data.createDate(getValue(), mDataFormat);
            default:
                if (isMultiValue())
                    return getValuesCollapsed(aDelimiterChar);
                else
                    return getValue();
        }
    }

    /**
     * Clears all values from the data item.
     */
    public void clearValues()
    {
        mValues.clear();
        if (isFeatureTrue(Data.FEATURE_IS_UPDATED))
            disableFeature(Data.FEATURE_IS_UPDATED);
    }

    /**
     * Assigns the default value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setDefaultValue(String aValue)
    {
        mDefaultValue = aValue;
    }

    public String getDefaultValue()
    {
        return mDefaultValue;
    }

    /**
     * Assigns the default value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setDefaultValue(Boolean aValue)
    {
        setDefaultValue(StrUtl.booleanToString(aValue));
    }

    /**
     * Assigns the default value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setDefaultValue(Integer aValue)
    {
        setDefaultValue(aValue.toString());
    }

    /**
     * Assigns the default value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setDefaultValue(Long aValue)
    {
        setDefaultValue(aValue.toString());
    }

    /**
     * Assigns the default value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setDefaultValue(Double aValue)
    {
        setDefaultValue(aValue.toString());
    }

    /**
     * Assigns the default value parameter to the data item.
     *
     * @param aValue Value to assign.
     */
    public void setDefaultValue(Float aValue)
    {
        setDefaultValue(aValue.toString());
    }

    /**
     * Copies the default value (if one was provided) to the field
     * value property.
     */
    public void assignValueFromDefault()
    {
        if (StringUtils.isNotEmpty(mDefaultValue))
        {
            if (mDefaultValue.equals(Data.VALUE_DATETIME_TODAY))
                setValue(new Date());
            else
                setValue(mDefaultValue);
        }
    }

    /**
     * Returns the count of values in the list.
     *
     * @return Count of values in the list.
     */
    public int valueCount()
    {
        return mValues.size();
    }

    /**
     * Returns <i>true</i> if the data value is valid or <i>false</i> otherwise.
     * A validation check ensures values are assigned when required and do not
     * exceed range limits (if assigned).
     * <p>
     * <b>Note:</b> If a item fails the validation check, then a property called
     * <i>Field.VALIDATION_PROPERTY_NAME</i> will be assigned a relevant message.
     * </p>
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid()
    {
        if (! isFeatureTrue(Data.FEATURE_IS_HIDDEN))
        {
            if ((isFeatureTrue(Data.FEATURE_IS_REQUIRED)) && (StringUtils.isEmpty(getValue())))
            {
                addProperty(Data.VALIDATION_PROPERTY_NAME, Data.VALIDATION_MESSAGE_IS_REQUIRED);
                return false;
            }
            else if (isRangeAssigned())
            {
                if (! mRange.isValid(getValue()))
                {
                    addProperty(Data.VALIDATION_PROPERTY_NAME, Data.VALIDATION_MESSAGE_OUT_OF_RANGE);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Add a unique feature to this item.  A feature enhances the core
     * capability of the item.  Standard features are listed below.
     *
     * <ul>
     *     <li>Data.FEATURE_IS_PRIMARY_KEY</li>
     *     <li>Data.FEATURE_IS_VISIBLE</li>
     *     <li>Data.FEATURE_IS_UPDATED</li>
     *     <li>Data.FEATURE_IS_REQUIRED</li>
     *     <li>Data.FEATURE_IS_UNIQUE</li>
     *     <li>Data.FEATURE_IS_INDEXED</li>
     *     <li>Data.FEATURE_IS_STORED</li>
     *     <li>Data.FEATURE_IS_SECRET</li>
     *     <li>Data.FEATURE_TYPE_ID</li>
     *     <li>Data.FEATURE_INDEX_TYPE</li>
     *     <li>Data.FEATURE_STORED_SIZE</li>
     *     <li>Data.FEATURE_INDEX_POLICY</li>
     *     <li>Data.FEATURE_FUNCTION_NAME</li>
     *     <li>Data.FEATURE_SEQUENCE_SEED</li>
     *     <li>Data.FEATURE_SEQUENCE_INCREMENT</li>
     *     <li>Data.FEATURE_SEQUENCE_MANAGEMENT</li>
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
     * Add a unique feature to this item.  A feature enhances the core
     * capability of the item.
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, int aValue)
    {
        addFeature(aName, Integer.toString(aValue));
    }

    /**
     * Add a unique feature to this item.  A feature enhances the core
     * capability of the item.
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, long aValue)
    {
        addFeature(aName, Long.toString(aValue));
    }

    /**
     * Add a unique feature to this item.  A feature enhances the core
     * capability of the item.
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, float aValue)
    {
        addFeature(aName, Float.toString(aValue));
    }

    /**
     * Add a unique feature to this item.  A feature enhances the core
     * capability of the item.
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
     * Count of unique features assigned to this item.
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
     * Copies the features from the data item parameter to the current
     * data item instance.
     *
     * @param aDataItem Data item instance
     */
    public void copyFeatures(DataItem aDataItem)
    {
        HashMap<String, String> diFeatures = aDataItem.getFeatures();
        diFeatures.forEach(this::addFeature);
    }

    /**
     * Add an application defined property to the data item.
     * <p>
     * <b>Notes:</b>
     * </p>
     * <ul>
     *     <li>The goal of the Dataitem is to strike a balance between
     *     providing enough properties to adequately model application
     *     related data without overloading it.</li>
     *     <li>This method offers a mechanism to capture additional
     *     (application specific) properties that may be needed.</li>
     *     <li>Properties added with this method are transient and
     *     will not be stored when saved.</li>
     * </ul>
     *
     * @param aName Property name (duplicates are not supported)
     * @param anObject Instance of an object
     */
    public void addProperty(String aName, Object anObject)
    {
        if (mProperties == null)
            mProperties = new HashMap<String, Object>();
        mProperties.put(aName, anObject);
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
     * Clears all property entries.
     */
    public void clearProperties()
    {
        if (mProperties != null)
            mProperties.clear();
    }

    /**
     * Returns <i>true</i> if the value of the data item
     * parameter matches the current value of this item.  The
     * comparison is done via {@link StringUtils}.equals() method.
     * If the comparison fails, the it returns <i>false</i>.
     *
     * @param aDataItem Data item instance
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValueEqual(DataItem aDataItem)
    {
        if (aDataItem != null)
        {
            String value1 = getValuesCollapsed();
            String value2 = aDataItem.getValuesCollapsed();
            if (StringUtils.equals(value1, value2))
                return true;
        }

        return false;
    }

    /**
     * Returns <i>true</i> if the name and value of the data item
     * parameter matches the current value of this data item.  The
     * comparison is done via {@link StringUtils}.equals() method.
     * If the comparison fails, the it returns <i>false</i>.
     *
     * @param aDataItem Data item instance
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isEqual(DataItem aDataItem)
    {
        return ((aDataItem.getType() == getType()) &&
                (StringUtils.equals(aDataItem.getName(), getName())) &&
                (isValueEqual(aDataItem)));
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param anObject Object instance to compare
     *
     * @return <i>true</i> or <i>false</i>
     */
    @Override
    public boolean equals(Object anObject)
    {
        DataItem dataItem;

        if (! (anObject instanceof DataItem))
            return false;
        dataItem = (DataItem) anObject;

        return isEqual(dataItem);
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
        return new HashCodeBuilder().append(mType).append(mName).append(mTitle)
                                    .append(mValues).append(mFeatures).toHashCode();
    }

    /***
     * The Builder class provides utility methods for constructing data items.
     */
    public static class Builder
    {
        private int mStoredSize;
        private DataRange mRange;
        private int mDisplaySize;
        private boolean mIsStored;
        private boolean mIsSecret;
        private boolean mIsPrimary;
        private boolean mIsCurrency;
        private boolean mIsRequired;
        private boolean mIsHidden = false;
        private boolean mIsVisible = true;
        private boolean mIsSuggest = false;
        private String mName = StringUtils.EMPTY;
        private String mTitle = StringUtils.EMPTY;
        private String mUIFormat = StringUtils.EMPTY;
        private Data.Type mType = Data.Type.Undefined;
        private String mDataFormat = StringUtils.EMPTY;
        private String mDefaultValue = StringUtils.EMPTY;
        private ArrayList<String> mValues = new ArrayList<>();

        /**
         * Assigns a data type to a data item.
         *
         * @param aType Data type
         *
         * @return Builder instance
         */
        public Builder type(Data.Type aType)
        {
            mType = aType;
            return this;
        }

        /**
         * Assigns a name to a data item.
         *
         * @param aName Item name
         *
         * @return Builder instance
         */
        public Builder name(String aName)
        {
            mName = aName;
            return this;
        }

        /**
         * Assigns a title to a data item.
         *
         * @param aTitle Item title
         *
         * @return Builder instance
         */
        public Builder title(String aTitle)
        {
            mTitle = aTitle;
            return this;
        }

        /**
         * Assigns a data format to a data item.
         *
         * @param aFormat Numeric or date format
         *
         * @return Builder instance
         */
        public Builder dataFormat(String aFormat)
        {
            mDataFormat = aFormat;
            return this;
        }

        /**
         * Assigns a UI format to a data item.
         *
         * @param aFormat Based on UI presentation framework
         *
         * @return Builder instance
         */
        public Builder uiFormat(String aFormat)
        {
            mUIFormat = aFormat;
            return this;
        }

        /**
         * Assigns a stored size to a data item.
         *
         * @param aSize Item stored size
         *
         * @return Builder instance
         */
        public Builder storedSize(int aSize)
        {
            mStoredSize = aSize;
            return this;
        }

        /**
         * Assigns a display size to a data item.
         *
         * @param aSize Item display size
         *
         * @return Builder instance
         */
        public Builder displaySize(int aSize)
        {
            mDisplaySize = aSize;
            return this;
        }

        /**
         * Identifies if the data item should be stored.
         *
         * @param anIsStored Item stored flag
         *
         * @return Builder instance
         */
        public Builder isStored(boolean anIsStored)
        {
            mIsStored = anIsStored;
            return this;
        }

        /**
         * Identifies if the data item should be visible.
         *
         * @param anIsVisible Item visible flag
         *
         * @return Builder instance
         */
        public Builder isVisible(boolean anIsVisible)
        {
            mIsVisible = anIsVisible;
            return this;
        }

        /**
         * Identifies if the data item should be hidden.
         *
         * @param anIsHidden Item hidden flag
         *
         * @return Builder instance
         */
        public Builder isHidden(boolean anIsHidden)
        {
            mIsHidden = anIsHidden;
            return this;
        }

        /**
         * Identifies if the data item should be required.
         *
         * @param anIsRequired Item required flag
         *
         * @return Builder instance
         */
        public Builder isRequired(boolean anIsRequired)
        {
            mIsRequired = anIsRequired;
            return this;
        }

        /**
         * Identifies if the data item should be a primary identifier.
         *
         * @param anIsPrimary Item primary flag
         *
         * @return Builder instance
         */
        public Builder isPrimary(boolean anIsPrimary)
        {
            mIsPrimary = anIsPrimary;
            return this;
        }

        /**
         * Identifies if the data item should be used for suggestions.
         *
         * @param anIsSuggest Use for suggestions flag
         *
         * @return Builder instance
         */
        public Builder isSuggest(boolean anIsSuggest)
        {
            mIsSuggest = anIsSuggest;
            return this;
        }

        /**
         * Identifies if the numeric data item should be formatted for currency.
         *
         * @param anIsCurrency Use for currency flag
         *
         * @return Builder instance
         */
        public Builder isCurrency(boolean anIsCurrency)
        {
            mIsCurrency = anIsCurrency;
            return this;
        }

        /**
         * Identifies if the data item should be managed as a secret.
         *
         * @param anIsSecret Item secret flag
         *
         * @return Builder instance
         */
        public Builder isSecret(boolean anIsSecret)
        {
            mIsSecret = anIsSecret;
            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aRange Data range instance
         *
         * @return Builder instance
         */
        public Builder range(DataRange aRange)
        {
            mRange = aRange;

            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aStrArgs Array of string values.
         *
         * @return Builder instance
         */
        public Builder range(String... aStrArgs)
        {
            if (Data.isText(mType))
                mRange = new DataRange(aStrArgs);
            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aMin Minimum value of the range.
         * @param aMax Maximum value of the range.
         *
         * @return Builder instance
         */
        public Builder range(int aMin, int aMax)
        {
            if (Data.isNumber(mType))
                mRange = new DataRange(aMin, aMax);
            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aMin Minimum value of the range.
         * @param aMax Maximum value of the range.
         *
         * @return Builder instance
         */
        public Builder range(long aMin, long aMax)
        {
            if (Data.isNumber(mType))
                mRange = new DataRange(aMin, aMax);
            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aMin Minimum value of the range.
         * @param aMax Maximum value of the range.
         *
         * @return Builder instance
         */
        public Builder range(double aMin, double aMax)
        {
            if (Data.isNumber(mType))
                mRange = new DataRange(aMin, aMax);
            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aMin Minimum value of the range.
         * @param aMax Maximum value of the range.
         *
         * @return Builder instance
         */
        public Builder range(Date aMin, Date aMax)
        {
            if (Data.isDateOrTime(mType))
                mRange = new DataRange(aMin, aMax);
            return this;
        }

        /**
         * Assigns a range to a data item.
         *
         * @param aMin Minimum value of the range.
         * @param aMax Maximum value of the range.
         *
         * @return Builder instance
         */
        public Builder range(Calendar aMin, Calendar aMax)
        {
            if (Data.isDateOrTime(mType))
                mRange = new DataRange(aMin, aMax);
            return this;
        }

        /**
         * Assigns a default value to a data item.
         *
         * @param aValue Item default value
         *
         * @return Builder instance
         */
        public Builder defaultValue(String aValue)
        {
            if (StringUtils.isNotEmpty(aValue))
                mDefaultValue = aValue;
            return this;
        }

        /**
         * Assigns a default value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder defaultValue(Integer aValue)
        {
            if (aValue != null)
                mValues.add(aValue.toString());
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Integer;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(String aValue)
        {
            if (StringUtils.isNotEmpty(aValue))
                mValues.add(aValue);
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Text;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(char aValue)
        {
            return value(String.valueOf(aValue));
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(Date aValue)
        {
            if (aValue != null)
            {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
                mValues.add(simpleDateFormat.format(aValue.getTime()));
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.DateTime;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(Boolean aValue)
        {
            if (aValue != null)
                mValues.add(StrUtl.booleanToString(aValue));
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Boolean;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(Integer aValue)
        {
            if (aValue != null)
                mValues.add(aValue.toString());
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Integer;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(Long aValue)
        {
            if (aValue != null)
                mValues.add(aValue.toString());
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Long;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(Float aValue)
        {
            if (aValue != null)
                mValues.add(aValue.toString());
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Float;
            return this;
        }

        /**
         * Assigns a value to a data item.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder value(Double aValue)
        {
            if (aValue != null)
                mValues.add(aValue.toString());
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Double;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(String... aValues)
        {
            if (aValues != null)
            {
                for (String value : aValues)
                    if (StringUtils.isNotEmpty(value))
                        mValues.add(value);
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Text;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(ArrayList<String> aValues)
        {
            if (aValues != null)
            {
                for (String value : aValues)
                    if (StringUtils.isNotEmpty(value))
                        mValues.add(value);
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Text;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(Date... aValues)
        {
            if (aValues != null)
            {
                for (Date value : aValues)
                {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
                    mValues.add(simpleDateFormat.format(value.getTime()));
                }
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.DateTime;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(int... aValues)
        {
            if (aValues != null)
            {
                for (Integer value : aValues)
                    mValues.add(value.toString());
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Integer;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(long... aValues)
        {
            if (aValues != null)
            {
                for (Long value : aValues)
                    mValues.add(value.toString());
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Long;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(float... aValues)
        {
            if (aValues != null)
            {
                for (Float value : aValues)
                    mValues.add(value.toString());
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Float;
            return this;
        }

        /**
         * Assigns an array of values to a data item.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder values(double... aValues)
        {
            if (aValues != null)
            {
                for (Double value : aValues)
                    mValues.add(value.toString());
            }
            if (mType == Data.Type.Undefined)
                mType = Data.Type.Double;
            return this;
        }

        private void assignType(String aValue)
        {
            if (aValue != null)
            {
                if (NumberUtils.isParsable(aValue))
                {
                    int offset = aValue.indexOf(StrUtl.CHAR_DOT);
                    if (offset == -1)
                        mType = Data.Type.Integer;
                    else
                        mType = Data.Type.Double;
                }
                else
                {
                    Date fieldDate = DatUtl.detectCreateDate(aValue);
                    if (fieldDate == null)
                        mType = Data.Type.Text;
                    else
                        mType = Data.Type.DateTime;
                }
            }
            else
                mType = Data.Type.Undefined;
        }

        /**
         * Assigns a value to a data item while analyzing
         * the content of the value to derive its data type.
         *
         * @param aValue Item value
         *
         * @return Builder instance
         */
        public Builder analyze(String aValue)
        {
            if (aValue != null)
            {
                mValues.add(aValue);
                assignType(aValue);
            }
            return this;
        }

        /**
         * Assigns an array of values to a data item while analyzing
         * the content of the value to derive its data type.
         *
         * @param aValues Item values
         *
         * @return Builder instance
         */
        public Builder analyze(ArrayList<String> aValues)
        {
            boolean isFirst = true;
            for (String value : aValues)
            {
                if (StringUtils.isNotEmpty(value))
                {
                    mValues.add(value);
                    if (isFirst)
                    {
                        isFirst = false;
                        assignType(value);
                    }
                }
            }
            return this;
        }

        /**
         * Builds a data item instance.
         *
         * @return Data item instance
         */
        public DataItem build()
        {
            return new DataItem(this);
        }
    }
}
