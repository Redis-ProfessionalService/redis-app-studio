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

import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

/**
 * A DataRange is used by {@link DataItem} to ensure that their
 * values constrained to an enumerated list or range of values.
 *
 * @since 1.0
 * @author Al Cole
 */
public class DataRange
{
    private Long mMinLong;
    private Long mMaxLong;
    private Double mMinDouble;
    private Double mMaxDouble;
    private Integer mMinInteger;
    private Integer mMaxInteger;
    private Calendar mMinCalendar;
    private Calendar mMaxCalendar;
    private ArrayList<String> mItems;
    private Data.Type mType = Data.Type.Undefined;
    private char mDelimiterChar = StrUtl.CHAR_COMMA;

    /**
     * Constructor accepts an array of string values.
     *
     * @param aStrArgs Array of string values.
     */
    public DataRange(String... aStrArgs)
    {
        mType = Data.Type.Text;
        mItems = new ArrayList<String>();
        if (aStrArgs != null)
            mItems.addAll(Arrays.asList(aStrArgs));
    }

    public void setItems(ArrayList<String> aStrArgs)
    {
        mItems = new ArrayList<String>();
        if (aStrArgs != null)
            mItems.addAll(aStrArgs);
    }

    /**
     * Constructor accepts a minimum and maximum value.
     *
     * @param aMin Minimum value.
     * @param aMax Maximum value.
     */
    public DataRange(int aMin, int aMax)
    {
        mType = Data.Type.Integer;
        mMinInteger = aMin;
        mMaxInteger = aMax;
    }

    /**
     * Constructor accepts a minimum and maximum value.
     *
     * @param aMin Minimum value.
     * @param aMax Maximum value.
     */
    public DataRange(long aMin, long aMax)
    {
        mType = Data.Type.Long;
        mMinLong = aMin;
        mMaxLong = aMax;
    }

    /**
     * Constructor accepts a minimum and maximum value.
     *
     * @param aMin Minimum value.
     * @param aMax Maximum value.
     */
    public DataRange(double aMin, double aMax)
    {
        mType = Data.Type.Double;
        mMinDouble = aMin;
        mMaxDouble = aMax;
    }

    /**
     * Constructor accepts a minimum and maximum value.
     *
     * @param aMin Minimum value.
     * @param aMax Maximum value.
     */
    public DataRange(Date aMin, Date aMax)
    {
        mType = Data.Type.DateTime;
        mMinCalendar = Calendar.getInstance();
        mMinCalendar.setTime(aMin);
        mMaxCalendar = Calendar.getInstance();
        mMaxCalendar.setTime(aMax);
    }

    /**
     * Constructor accepts a minimum and maximum value.
     *
     * @param aMin Minimum value.
     * @param aMax Maximum value.
     */
    public DataRange(Calendar aMin, Calendar aMax)
    {
        mType = Data.Type.DateTime;
        mMinCalendar = aMin;
        mMaxCalendar = aMax;
    }

    /**
     * Constructor clones an existing DataRange instance.
     *
     * @param aSrcRange Source DataRange to clone.
     */
    public DataRange(final DataRange aSrcRange)
    {
        if (aSrcRange != null)
        {
            mType = aSrcRange.mType;
            mMinLong = aSrcRange.mMinLong;
            mMaxLong = aSrcRange.mMaxLong;
            mMinDouble = aSrcRange.mMinDouble;
            mMaxDouble = aSrcRange.mMaxDouble;
            mMinInteger = aSrcRange.mMinInteger;
            mMaxInteger = aSrcRange.mMaxInteger;
            mMinCalendar = aSrcRange.mMinCalendar;
            mMaxCalendar = aSrcRange.mMaxCalendar;
            if (aSrcRange.mItems != null)
            {
                mItems = new ArrayList<String>();
                for (String iString : aSrcRange.mItems)
                    mItems.add(iString);
            }
        }
    }

    /**
     * Returns a string representation of a DataRange.
     *
     * @return String summary representation of this DataRange.
     */
    @Override
    public String toString()
    {
        return Data.typeToString(mType) + " - " + getMinMaxString();
    }

    public void add(String aString)
    {
        if (mType == Data.Type.Undefined)
            mType = Data.Type.Text;
        if (mType == Data.Type.Text)
        {
            if (mItems == null)
                mItems = new ArrayList<String>();
            mItems.add(aString);
        }
    }

    /**
     * Returns the data type of the data range.
     *
     * @return Data type.
     */
    public Data.Type getType()
    {
        return mType;
    }

    /**
     * Returns the array list of items managed by this data
     * range instance.
     *
     * @return Array list of items.
     */
    public ArrayList<String> getItems()
    {
        return mItems;
    }

    /**
     * Returns the multi-value delimiter character for the item.
     *
     * @return The multi-value delimiter character.
     */
    public char getDelimiterChar()
    {
        return mDelimiterChar;
    }

    /**
     * Assigns Multi-value delimiter character for the data.
     *
     * @param aDelimiterChar Multi-value delimiter character to assign.
     */
    public void setDelimiterChar(char aDelimiterChar)
    {
        mDelimiterChar = aDelimiterChar;
    }

    /**
     * Assigns Multi-value delimiter character for the item.
     *
     * @param aDelimiterString Multi-value delimiter character to assign.
     */
    public void setDelimiterChar(String aDelimiterString)
    {
        if (StringUtils.isNotEmpty(aDelimiterString))
            mDelimiterChar = aDelimiterString.charAt(0);
    }

    /**
     * Convenience method that translates a calendar instance to the
     * default {@Data}.FORMAT_DATETIME_DEFAULT string representation.
     *
     * @param aCalendar Calendar instance.
     *
     * @return String representation of the date/time.
     */
    private String calendarToString(Calendar aCalendar)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
        return dateFormat.format(aCalendar.getTime());
    }

    /**
     * Convenience method that translates a string representation (formatted
     * as {@Data}.FORMAT_DATETIME_DEFAULT) to a calendar instance.
     *
     * @param aValue String representation of the date/time.
     *
     * @return Calendar instance.
     */
    private Calendar stringToCalendar(String aValue)
    {
        Date valueDate;
        Calendar valueCalendar;
        SimpleDateFormat dateFormat;

        dateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
        try
        {
            valueDate = dateFormat.parse(aValue);
        }
        catch (ParseException e)
        {
            valueDate = new Date();
        }

        valueCalendar = Calendar.getInstance();
        valueCalendar.setTime(valueDate);

        return valueCalendar;
    }

    /**
     * Convenience method that identifies the minimum value in the
     * range and returns it as a string.
     *
     * @return String representation of the minimum value.
     */
    public String getMinString()
    {
        switch (mType)
        {
            case Text:
                if ((mItems != null) && (mItems.size() > 0))
                    return mItems.get(0);
                break;
            case Long:
                return mMinLong.toString();
            case Integer:
                return mMinInteger.toString();
            case Double:
                return mMinDouble.toString();
            case Date:
            case DateTime:
                return calendarToString(mMinCalendar);
            default:
                break;
        }

        return StringUtils.EMPTY;
    }

    /**
     * Convenience method that identifies the maximum value in the
     * range and returns it as a string.
     *
     * @return String representation of the maximum value.
     */
    public String getMaxString()
    {
        switch (mType)
        {
            case Text:
                if ((mItems != null) && (mItems.size() > 0))
                    return mItems.get(mItems.size()-1);
                break;
            case Long:
                return mMaxLong.toString();
            case Integer:
                return mMaxInteger.toString();
            case Double:
                return mMaxDouble.toString();
            case Date:
            case DateTime:
                return calendarToString(mMaxCalendar);
            default:
                break;
        }

        return StringUtils.EMPTY;
    }

    /**
     * Convenience method that identifies the minimum and maximum
     * values in the range and returns them as a string.
     *
     * @return String representation of the minimum and maximum values.
     */
    public String getMinMaxString()
    {
        switch (mType)
        {
            case Text:
                if ((mItems != null) && (mItems.size() > 0))
                {
                    boolean isFirst = true;
                    String mmString = "Selection: ";
                    for (String itemEntry : mItems)
                    {
                        if (isFirst)
                            isFirst = false;
                        else
                            mmString += ", ";
                        mmString += itemEntry;
                    }
                    return mmString;
                }
                break;
            case Long:
                return String.format("Range: %d - %d", mMinLong, mMaxLong);
            case Integer:
                return String.format("Range: %d - %d", mMinInteger, mMaxInteger);
            case Double:
                return String.format("Range: %.4f - %.4f", mMinDouble, mMaxDouble);
            case Date:
            case DateTime:
                return String.format("Range: %s - %s", calendarToString(mMinCalendar),
									 calendarToString(mMaxCalendar));
            default:
                break;
        }

        return StringUtils.EMPTY;
    }

    /**
     * Returns a minimum range value.
     *
     * @return Range value.
     */
    public Long getMinLong()
    {
        return mMinLong;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMinLong Minimum range value.
     */
    public void setMinLong(Long aMinLong)
    {
        mMinLong = aMinLong;
    }

    /**
     * Returns a maximum range value.
     *
     * @return Range value.
     */
    public Long getMaxLong()
    {
        return mMaxLong;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMaxLong Maximum range value.
     */
    public void setMaxLong(Long aMaxLong)
    {
        mMaxLong = aMaxLong;
    }

    /**
     * Returns a minimum range value.
     *
     * @return Range value.
     */
    public Double getMinDouble()
    {
        return mMinDouble;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMinDouble Minimum range value.
     */
    public void setMinDouble(Double aMinDouble)
    {
        mMinDouble = aMinDouble;
    }

    /**
     * Returns a maximum range value.
     *
     * @return Range value.
     */
    public Double getMaxDouble()
    {
        return mMaxDouble;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMaxDouble Maximum range value.
     */
    public void setMaxDouble(Double aMaxDouble)
    {
        mMaxDouble = aMaxDouble;
    }

    /**
     * Returns a minimum range value.
     *
     * @return Range value.
     */
    public Integer getMinInteger()
    {
        return mMinInteger;
    }

    public void setMinInteger(Integer aMinInteger)
    {
        mMinInteger = aMinInteger;
    }

    /**
     * Returns a maximum range value.
     *
     * @return Range value.
     */
    public Integer getMaxInteger()
    {
        return mMaxInteger;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMaxInteger Maximum range value.
     */
    public void setMaxInteger(Integer aMaxInteger)
    {
        mMaxInteger = aMaxInteger;
    }

    /**
     * Returns a minimum range value.
     *
     * @return Range value.
     */
    public Calendar getMinCalendar()
    {
        return mMinCalendar;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMinCalendar Minimum range value.
     */
    public void setMinCalendar(Calendar aMinCalendar)
    {
        mMinCalendar = aMinCalendar;
    }

    /**
     * Returns a maximum range value.
     *
     * @return Range value.
     */
    public Calendar getMaxCalendar()
    {
        return mMaxCalendar;
    }

    /**
     * Assigns a minimum range value.
     *
     * @param aMaxCalendar Maximum range value.
     */
    public void setMaxCalendar(Calendar aMaxCalendar)
    {
        mMaxCalendar = aMaxCalendar;
    }

    /**
     * Return <i>true</i> if the calendar instance is within the
     * defined range of values or <i>false</i> otherwise.
     *
     * @param aValue Calendar instance.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(Calendar aValue)
    {
        return ((aValue.after(mMinCalendar)) && (aValue.before(mMaxCalendar)));
    }

    /**
     * Return <i>true</i> if the date instance is within the
     * defined range of values or <i>false</i> otherwise.
     *
     * @param aValue Date instance.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(Date aValue)
    {
        Calendar valueCalendar = Calendar.getInstance();
        valueCalendar.setTime(aValue);
        return isValid(valueCalendar);
    }

    /**
     * Return <i>true</i> if the date value (formatted using the
     * format mask parameter) is within the defined range of values
     * or <i>false</i> otherwise.
     *
     * @param aValue String representation of a date/time.
     * @param aFormatMask Format mask string.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(String aValue, String aFormatMask)
    {
        if ((Data.isDateOrTime(mType)) && (StringUtils.isNotEmpty(aValue)))
        {
            try
            {
                Date valueDate = Data.createDate(aValue, aFormatMask);
                Calendar valueCalendar = Calendar.getInstance();
                valueCalendar.setTime(valueDate);
                return isValid(valueCalendar);
            }
            catch (Exception ignored)
            {
            }
        }
        return false;
    }

    /**
     * Return <i>true</i> if the value parameter is within the
     * defined range of values or <i>false</i> otherwise.
     *
     * @param aValue Value to evaluate.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(double aValue)
    {
        return ((aValue > mMinDouble) && (aValue < mMaxDouble));
    }

    /**
     * Return <i>true</i> if the value parameter is within the
     * defined range of values or <i>false</i> otherwise.
     *
     * @param aValue Value to evaluate.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(int aValue)
    {
        return ((aValue > mMinInteger) && (aValue < mMaxInteger));
    }

    /**
     * Return <i>true</i> if the value parameter is within the
     * defined range of values or <i>false</i> otherwise.
     *
     * @param aValue Value to evaluate.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(long aValue)
    {
        return ((aValue > mMinLong) && (aValue < mMaxLong));
    }

    /**
     * Return <i>true</i> if the value parameter is within the
     * defined range of values or <i>false</i> otherwise.
     *
     * @param aValue Value to evaluate.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid(String aValue)
    {
        switch (mType)
        {
            case Text:
                if ((mItems != null) && (mItems.size() > 0))
                    return mItems.contains(aValue);
                else
                    return false;
            case Integer:
                return isValid(Data.createInt(aValue));
            case Double:
                return isValid(Data.createDouble(aValue));
            case Date:
            case DateTime:
                return isValid(aValue, Data.FORMAT_DATETIME_DEFAULT);
            default:
                return false;
        }
    }

    /**
     * Determines if the two range instances are equal.
     *
     * @param aRange Range instance to base comparison on.
     *
     * @return <i>true</i> if equal, <i>false</i> otherwise.
     */
    public boolean isEqual(final DataRange aRange)
    {
        if (aRange != null)
        {
            if (mType != aRange.mType)
                return false;
            if ((mMinLong != null) && (aRange.mMinLong != null) &&
                (! mMinLong.equals(aRange.mMinLong)))
                return false;
            if ((mMaxLong != null) && (aRange.mMaxLong != null) &&
                (! mMaxLong.equals(aRange.mMaxLong)))
                return false;
            if ((mMinDouble != null) && (aRange.mMinDouble != null) &&
                (! mMinDouble.equals(aRange.mMinDouble)))
                return false;
            if ((mMaxDouble != null) && (aRange.mMaxDouble != null) &&
                (! mMaxDouble.equals(aRange.mMaxDouble)))
                return false;
            if ((mMinInteger != null) && (aRange.mMinInteger != null) &&
                (! mMinInteger.equals(aRange.mMinInteger)))
                return false;
            if ((mMaxInteger != null) && (aRange.mMaxInteger != null) &&
                (! mMaxInteger.equals(aRange.mMaxInteger)))
                return false;
            if ((mMinCalendar != null) && (aRange.mMinCalendar != null) &&
                (! mMinCalendar.equals(aRange.mMinCalendar)))
                return false;
            if ((mMaxCalendar != null) && (aRange.mMaxCalendar != null) &&
                (! mMaxCalendar.equals(aRange.mMaxCalendar)))
                return false;
            if ((mItems != null) && (aRange.mItems != null))
            {
                int totalItems = mItems.size();

                if (totalItems != aRange.mItems.size())
                    return false;

                for (int offset = 0; offset < totalItems; offset++)
                {
                    if (! StringUtils.equals(mItems.get(offset), aRange.mItems.get(offset)))
                        return false;
                }
            }

            return true;
        }

        return false;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param anObject Reference object with which to compare.
     * @return  {@code true} if this object is the same as the anObject
     *          argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object anObject)
    {
        if (this == anObject)
            return true;
        if (anObject == null || getClass() != anObject.getClass())
            return false;

        DataRange dataRange = (DataRange) anObject;

        return isEqual(dataRange);
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
        int hashCode = mMinLong != null ? mMinLong.hashCode() : 0;

        hashCode = 31 * hashCode + (mMaxLong != null ? mMaxLong.hashCode() : 0);
        hashCode = 31 * hashCode + (mMinDouble != null ? mMinDouble.hashCode() : 0);
        hashCode = 31 * hashCode + (mMaxDouble != null ? mMaxDouble.hashCode() : 0);
        hashCode = 31 * hashCode + (mMinInteger != null ? mMinInteger.hashCode() : 0);
        hashCode = 31 * hashCode + (mMaxInteger != null ? mMaxInteger.hashCode() : 0);
        hashCode = 31 * hashCode + (mMinCalendar != null ? mMinCalendar.hashCode() : 0);
        hashCode = 31 * hashCode + (mMaxCalendar != null ? mMaxCalendar.hashCode() : 0);
        hashCode = 31 * hashCode + (mItems != null ? mItems.hashCode() : 0);
        hashCode = 31 * hashCode + (int) mDelimiterChar;
        hashCode = 31 * hashCode + (mType != null ? mType.hashCode() : 0);

        return hashCode;
    }
}
