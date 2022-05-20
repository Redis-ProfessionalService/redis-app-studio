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

package com.redis.foundation.std;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * The IOXML class provides a collection of utility methods that aid
 * in the generation and consumption of an XML DOM hierarchy.
 *
 * @since 1.0
 * @author Al Cole
 */
public class IOXML
{
    /**
     * Generates one or more space characters for indentation.
     *
     * @param aPW Print writer output stream.
     * @param aSpaceCount Count of spaces to indent.
     */
    public static void indentLine(PrintWriter aPW, int aSpaceCount)
    {
        for (int i = 0; i < aSpaceCount; i++)
            aPW.append(StrUtl.CHAR_SPACE);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, String aValue)
        throws IOException
    {
        if (StringUtils.isNotEmpty(aValue))
            aPW.printf(" %s=\"%s\"", StringEscapeUtils.escapeXml10(aName),
					   StringEscapeUtils.escapeXml10(aValue));
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, char aValue)
        throws IOException
    {
        aPW.printf(" %s=\"%c\"", StringEscapeUtils.escapeXml10(aName), aValue);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, int aValue)
            throws IOException
    {
        aPW.printf(" %s=\"%d\"", StringEscapeUtils.escapeXml10(aName), aValue);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, long aValue)
            throws IOException
    {
        aPW.printf(" %s=\"%d\"", StringEscapeUtils.escapeXml10(aName), aValue);
    }

    /**
     * Writes an XML tag attribute (name/value) while ensuring the characters
     * are properly escaped.
     *
     * @param aPW Print writer output stream.
     * @param aName Attribute name.
     * @param aValue Attribute value.
     *
     * @throws IOException I/O related exception.
     */
    public static void writeAttrNameValue(PrintWriter aPW, String aName, boolean aValue)
            throws IOException
    {
        aPW.printf(" %s=\"%s\"", StringEscapeUtils.escapeXml10(aName),
				   StrUtl.booleanToString(aValue));
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, String aValue)
            throws IOException
    {
        indentLine(aPW, anIndentAmount);
        if (StringUtils.isEmpty(aValue))
            aPW.printf("<%s></%s>%n", aName, aName);
        else
            aPW.printf("<%s>%s</%s>%n", aName, StringEscapeUtils.escapeXml10(aValue), aName);
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, int aValue)
            throws IOException
    {
        if (aValue != 0)
        {
            indentLine(aPW, anIndentAmount);
            aPW.printf("<%s>%d</%s>%n", aName, aValue, aName);
        }
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, long aValue)
            throws IOException
    {
        if (aValue != 0L)
        {
            indentLine(aPW, anIndentAmount);
            aPW.printf("<%s>%d</%s>%n", aName, aValue, aName);
        }
    }

    /**
     * Writes an XML node (name/value) while ensuring the characters are properly
     * escaped.
     *
     * @param aPW Print writer output stream.
     * @param anIndentAmount Count of spaces to indent.
     * @param aName Node name.
     * @param aValue Node value.
     * @throws IOException I/O related exception.
     */
    public static void writeNodeNameValue(PrintWriter aPW, int anIndentAmount, String aName, boolean aValue)
            throws IOException
    {
        indentLine(aPW, anIndentAmount);
        aPW.printf("<%s>%s</%s>%n", aName, StrUtl.booleanToString(aValue), aName);
    }
}
