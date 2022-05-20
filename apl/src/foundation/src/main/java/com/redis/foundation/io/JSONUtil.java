/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.redis.foundation.io;

import com.google.gson.stream.JsonWriter;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The JSONUtil class provides a collection of utility methods that aid
 * in the generation and consumption of an JSON streams.
 *
 * @since 1.0
 * @author Al Cole
 */
public class JSONUtil
{
    /**
     * Writes a JSON field name/value if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, String aValue)
        throws IOException
    {
        if (StringUtils.isNotEmpty(aValue))
            aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/values if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValues Field values.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameStringValues(JsonWriter aWriter, String aName,
									         ArrayList<String> aValues)
        throws IOException
    {
        if ((aValues != null) && (aValues.size() > 0))
        {
            aWriter.name(aName).beginArray();
            for (String value : aValues)
                aWriter.value(value);
            aWriter.endArray();
        }
    }

    /**
     * Writes a JSON field name/values if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValues Field values.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameBooleanValues(JsonWriter aWriter, String aName,
                                              ArrayList<Boolean> aValues)
        throws IOException
    {
        if ((aValues != null) && (aValues.size() > 0))
        {
            aWriter.name(aName).beginArray();
            for (Boolean value : aValues)
                aWriter.value(value);
            aWriter.endArray();
        }
    }

    /**
     * Writes a JSON field name/values if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValues Field values.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameIntegerValues(JsonWriter aWriter, String aName,
                                              ArrayList<Integer> aValues)
        throws IOException
    {
        if ((aValues != null) && (aValues.size() > 0))
        {
            aWriter.name(aName).beginArray();
            for (Integer value : aValues)
                aWriter.value(value);
            aWriter.endArray();
        }
    }

    /**
     * Writes a JSON field name/values if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValues Field values.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameLongValues(JsonWriter aWriter, String aName,
                                           ArrayList<Long> aValues)
        throws IOException
    {
        if ((aValues != null) && (aValues.size() > 0))
        {
            aWriter.name(aName).beginArray();
            for (Long value : aValues)
                aWriter.value(value);
            aWriter.endArray();
        }
    }

    /**
     * Writes a JSON field name/values if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValues Field values.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameFloatValues(JsonWriter aWriter, String aName,
                                            ArrayList<Float> aValues)
        throws IOException
    {
        if ((aValues != null) && (aValues.size() > 0))
        {
            aWriter.name(aName).beginArray();
            for (Float value : aValues)
                aWriter.value(value);
            aWriter.endArray();
        }
    }

    /**
     * Writes a JSON field name/values if the value is not empty.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValues Field values.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameDoubleValues(JsonWriter aWriter, String aName,
                                             ArrayList<Double> aValues)
    throws IOException
    {
        if ((aValues != null) && (aValues.size() > 0))
        {
            aWriter.name(aName).beginArray();
            for (Double value : aValues)
                aWriter.value(value);
            aWriter.endArray();
        }
    }

    /**
     * Writes a JSON field name/value.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, char aValue)
        throws IOException
    {
        aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/value.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, int aValue)
        throws IOException
    {
        aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/value if the value is non-zero.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValueNonZero(JsonWriter aWriter, String aName, int aValue)
        throws IOException
    {
        if (aValue != 0)
            aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/value.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, long aValue)
        throws IOException
    {
        aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/value.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, double aValue)
        throws IOException
    {
        aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/value.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, boolean aValue)
        throws IOException
    {
        aWriter.name(aName).value(aValue);
    }

    /**
     * Writes a JSON field name/value.
     *
     * @param aWriter JSON writer output stream.
     * @param aName Field name.
     * @param aValue Field value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeNameValue(JsonWriter aWriter, String aName, HashMap<String, String> aValue)
        throws IOException
    {
        if (aValue.size() > 0)
        {
            aWriter.name(aName).beginObject();
            for (Map.Entry<String, String> featureEntry : aValue.entrySet())
            {
                aWriter.name(featureEntry.getKey());
                aWriter.value(featureEntry.getValue());
            }
            aWriter.endObject();
        }
    }
}
