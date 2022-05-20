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

//import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.*;

/**
 * The XMLUtl class provides utility features to applications that need
 * to process XML content.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2014
 * @since 1.0
 */
@SuppressWarnings({"UnusedDeclaration"})
public class XMLUtl
{
    public static final String XML_UPPER_NO = "NO";
    public static final String XML_UPPER_YES = "YES";
    public static final String XML_UPPER_TRUE = "TRUE";

    public static boolean isEscapeNeeded(String aString)
    {
        int offset;

        offset = aString.indexOf(StrUtl.CHAR_LESSTHAN);
        if (offset != -1)
            return true;
        offset = aString.indexOf(StrUtl.CHAR_GREATERTHAN);
        if (offset != -1)
            return true;
        offset = aString.indexOf(StrUtl.CHAR_AMPERSAND);
        if (offset != -1)
            return true;
        offset = aString.indexOf(StrUtl.CHAR_SGLQUOTE);
        if (offset != -1)
            return true;
        offset = aString.indexOf(StrUtl.CHAR_DBLQUOTE);

        return offset != -1;
    }

    /*
    public static String escapeString(String aString)
    {
        return StringEscapeUtils.escapeXml10(aString);
    }
    */

    public static void setAttrStrValue(Element anElement, String aName, String aValue)
    {
        if ((StringUtils.isNotEmpty(aName)) && (StringUtils.isNotEmpty(aValue)))
            anElement.setAttribute(aName, aValue);
    }

    public static void setAttrIntValue(Element anElement, String aName, int aValue)
    {
        Integer intObject;

        intObject = aValue;
        anElement.setAttribute(aName, intObject.toString());
    }

    public static void setAttrLongValue(Element anElement, String aName, long aValue)
    {
        Long longObject;

        if (StringUtils.isNotEmpty(aName))
        {
            longObject = aValue;
            anElement.setAttribute(aName, longObject.toString());
        }
    }

    public static void setAttrDoubleValue(Element anElement, String aName, double aValue)
    {
        Double doubleObject;

        if (StringUtils.isNotEmpty(aName))
        {
            doubleObject = aValue;
            anElement.setAttribute(aName, doubleObject.toString());
        }
    }

    public static void setAttrBoolValue(Element anElement, String aName, boolean aFlag)
    {
        if (StringUtils.isNotEmpty(aName))
        {
            if (aFlag)
                anElement.setAttribute(aName, XML_UPPER_YES);
            else
                anElement.setAttribute(aName, XML_UPPER_NO);
        }
    }

    public static boolean isAttrBoolTrue(Element anElement, String aName)
    {
        String valueString;

        if (StringUtils.isNotEmpty(aName))
        {
            valueString = anElement.getAttribute(aName);
            if (StringUtils.isNotEmpty(valueString))
            {
                if ((valueString.equalsIgnoreCase(XML_UPPER_YES)) ||
                        (valueString.equalsIgnoreCase(XML_UPPER_TRUE)))
                    return true;
            }
        }

        return false;
    }

    public static String escapeElemStrValue(String aValue)
    {
        if (StringUtils.isEmpty(aValue))
            return StringUtils.EMPTY;
        else
        {
            int offset = aValue.indexOf("<![CDATA[");
            while (offset != -1)
            {
                aValue = aValue.substring(0, offset) + aValue.substring(offset+9);
                offset = aValue.indexOf("]]>");
                if (offset != -1)
                    aValue = aValue.substring(0, offset) + aValue.substring(offset+3);
                offset = aValue.indexOf("<![CDATA[");
            }
            return String.format("<![CDATA[%s]]>", aValue);
        }
    }

    public static void makeEscElemStrValue(Document aDocument, Element anElement,
                                           String aName, String aValue)
    {
        Element subElement;

        if ((StringUtils.isNotEmpty(aName)) && (StringUtils.isNotEmpty(aValue)))
        {
            subElement = aDocument.createElement(aName);
            if (isEscapeNeeded(aValue))
                aValue = escapeElemStrValue(aValue);
            subElement.appendChild(aDocument.createTextNode(aValue));
            anElement.appendChild(subElement);
        }
    }

    public static void makeElemStrValue(Document aDocument, Element anElement,
                                        String aName, String aValue)
    {
        Element subElement;

        if ((StringUtils.isNotEmpty(aName)) && (StringUtils.isNotEmpty(aValue)))
        {
            subElement = aDocument.createElement(aName);
            subElement.appendChild(aDocument.createTextNode(aValue));
            anElement.appendChild(subElement);
        }
    }

    public static void makeElemIntValue(Document aDocument, Element anElement,
                                        String aName, int aValue)
    {
        Integer intObject;
        Element subElement;

        if (StringUtils.isNotEmpty(aName))
        {
            intObject = aValue;
            subElement = aDocument.createElement(aName);
            subElement.appendChild(aDocument.createTextNode(intObject.toString()));
            anElement.appendChild(subElement);
        }
    }

    public static void makeElemLongValue(Document aDocument, Element anElement,
                                         String aName, long aValue)
    {
        Long longObject;
        Element subElement;

        if (StringUtils.isNotEmpty(aName))
        {
            longObject = aValue;
            subElement = aDocument.createElement(aName);
            subElement.appendChild(aDocument.createTextNode(longObject.toString()));
            anElement.appendChild(subElement);
        }
    }

    public static void makeElemDoubleValue(Document aDocument, Element anElement,
                                           String aName, double aValue)
    {
        Element subElement;
        Double doubleObject;

        if (StringUtils.isNotEmpty(aName))
        {
            doubleObject = aValue;
            subElement = aDocument.createElement(aName);
            subElement.appendChild(aDocument.createTextNode(doubleObject.toString()));
            anElement.appendChild(subElement);
        }
    }

    public static void makeElemBoolValue(Document aDocument, Element anElement,
                                         String aName, boolean aFlag)
    {
        String aValue;
        Element subElement;

        if (StringUtils.isNotEmpty(aName))
        {
            if (aFlag)
                aValue = XML_UPPER_YES;
            else
                aValue = XML_UPPER_NO;
            subElement = aDocument.createElement(aName);
            subElement.appendChild(aDocument.createTextNode(aValue));
            anElement.appendChild(subElement);
        }
    }

    public static String getNodeStrValue(Node aNode)
    {
        int count;
        Node textNode;
        NodeList nodeList;

        nodeList = aNode.getChildNodes();
        count = nodeList.getLength();

        for (int i = 0; i < count; i++)
        {
            textNode = nodeList.item(i);

            if (textNode.getNodeType() == Node.TEXT_NODE)
                return textNode.getNodeValue();
        }

        return StringUtils.EMPTY;
    }

    public static String getNodeCDATAValue(Node aNode)
    {
        int count;
        Node textNode;
        NodeList nodeList;

        nodeList = aNode.getChildNodes();
        count = nodeList.getLength();

        for (int i = 0; i < count; i++)
        {
            textNode = nodeList.item(i);

            if (textNode.getNodeType() == Node.CDATA_SECTION_NODE)
                return textNode.getNodeValue();
        }

        return StringUtils.EMPTY;
    }

    public static String getElementStrValue(Element anElement)
    {
        int count;
        Node textNode;
        NodeList nodeList;

        Node curNode = (Node) anElement;
        nodeList = curNode.getChildNodes();
        count = nodeList.getLength();

        for (int i = 0; i < count; i++)
        {
            textNode = nodeList.item(i);

            if (textNode.getNodeType() == Node.TEXT_NODE)
                return textNode.getNodeValue();
        }

        return StringUtils.EMPTY;
    }
}