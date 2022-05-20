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

package com.redis.foundation.io;

import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The IO class captures the constants, enumerated types and utility methods for the IO package.
 *
 * @since 1.0
 * @author Al Cole
 */
public class IO
{
	public static final String DATADOC_XML_FORMAT_VERSION = "1.0";
	public static final String CRITERIA_XML_FORMAT_VERSION = "1.0";
	public static final String DATAGRID_XML_FORMAT_VERSION = "1.0";

// XML nodes.

	public static final String XML_ITEM_NODE_NAME = "Item";
	public static final String XML_CHILD_NODE_NAME = "Child";
	public static final String XML_DOCUMENT_NODE_NAME = "Document";
	public static final String XML_CRITERIA_NODE_NAME = "Criteria";
	public static final String XML_OPERATION_NODE_NAME = "Operation";
	public static final String XML_PROPERTIES_NODE_NAME = "Properties";
	public static final String XML_RELATIONSHIP_NODE_NAME = "Relationship";
	private IO()
	{
	}

	public static String extractType(String aClassName)
	{
		if (StringUtils.isNotEmpty(aClassName))
		{
			int offset = aClassName.lastIndexOf(StrUtl.CHAR_DOT);
			if (offset == -1)
				return aClassName;
			else
			{
				if (offset < aClassName.length()-1)
					return aClassName.substring(offset + 1);
				else
					return aClassName;
			}
		}
		return StringUtils.EMPTY;
	}

	public static boolean isTypesEqual(String aClassName1, String aClassName2)
	{
		String className1 = extractType(aClassName1);
		String className2 = extractType(aClassName2);

		return className1.equals(className2);
	}

	public static void closeQuietly(InputStream aStream)
	{
		if (aStream != null)
		{
			try
			{
				aStream.close();
			}
			catch (Exception ignored)
			{
			}
		}
	}

	public static void closeQuietly(OutputStream aStream)
	{
		if (aStream != null)
		{
			try
			{
				aStream.close();
			}
			catch (Exception ignored)
			{
			}
		}
	}

	public static void closeQuietly(Closeable aCloseable)
	{
		if (aCloseable != null)
		{
			try
			{
				aCloseable.close();
			}
			catch (Exception ignored)
			{
			}
		}
	}
}
