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

package com.redis.ds.ds_content;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.CfgMgr;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.io.DataGridCSV;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * The ContentType class is responsible for detecting matching
 * document types based on an application provided CSV file.
 * Refer to the <i>doctypes_default.csv</i> file for a reference
 * listing of types.
 *
 * @author Al Cole
 * @since 1.0
 */
public class ContentType
{
	private final AppCtx mAppCtx;
	private final CfgMgr mCfgMgr;
	private final DataGrid mDataGrid;

	/**
     * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	*/
	public ContentType(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
		mDataGrid = new DataGrid(schemaDoc());
		mCfgMgr = new CfgMgr(mAppCtx, Content.CFG_PROPERTY_PREFIX);
	}

	public DataDoc schemaDoc()
	{
		DataDoc dataDoc = new DataDoc("Document Type");
		dataDoc.add(new DataItem.Builder().name("type_name").title("Type Name").build());
		dataDoc.add(new DataItem.Builder().name("file_extension").title("File Extension").build());
		dataDoc.add(new DataItem.Builder().name("mime_type").title("MIME Type").build());
		dataDoc.add(new DataItem.Builder().name("url_pattern").title("URL Pattern").build());
		dataDoc.add(new DataItem.Builder().name("icon_name").title("Icon Name").build());

		return dataDoc;
	}

	/**
	 * Assigns the configuration property prefix to the document data source.
	 *
	 * @param aPropertyPrefix Property prefix.
	 */
	public void setCfgPropertyPrefix(String aPropertyPrefix)
	{
		mCfgMgr.setCfgPropertyPrefix(aPropertyPrefix);
	}

	/**
	 * Given a file name, return the type name that matches its
	 * extension or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aFileName File name to examine.
	 *
	 * @return Type name.
	 */
	public String nameByFileExtension(String aFileName)
	{
		if (StringUtils.isNotEmpty(aFileName))
		{
			int extOffset = aFileName.lastIndexOf(StrUtl.CHAR_DOT);
			if (extOffset != -1)
			{
				String fileExtension = aFileName.substring(extOffset);
				List<String> typeNameList = mDataGrid.stream()
													 .filter(dd -> dd.getValueByName("file_extension").equals(fileExtension))
													 .map(dd -> dd.getItemByName("type_name").getValue())
													 .limit(1).collect(toList());
				if (typeNameList.size() > 0)
					return typeNameList.get(0);
			}
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Given a URL, return the type name that matches its pattern
	 * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aURL URL to examine.
	 *
	 * @return Type name.
	 */
	public String nameByURL(String aURL)
	{
		if (StringUtils.isNotEmpty(aURL))
		{
			DataDoc dataDoc;
			String urlPattern;

			int rowCount = mDataGrid.rowCount();
			for (int row = 0; row < rowCount; row++)
			{
				dataDoc = mDataGrid.getRowAsDoc(row);
				urlPattern = dataDoc.getValueByName("url_pattern");
				if (StringUtils.isNotEmpty(urlPattern))
				{
					if (StringUtils.contains(aURL, urlPattern))
						return dataDoc.getValueByName("type_type");
				}
			}
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Given a file name, return the icon name that matches its
	 * extension or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aFileName File name to examine.
	 *
	 * @return Icon name.
	 */
	public String iconByFileExtension(String aFileName)
	{
		if (StringUtils.isNotEmpty(aFileName))
		{
			int extOffset = aFileName.lastIndexOf(StrUtl.CHAR_DOT);
			if (extOffset != -1)
			{
				String fileExtension = aFileName.substring(extOffset);
				List<String> iconNameList = mDataGrid.stream()
													 .filter(dd -> dd.getValueByName("file_extension").equals(fileExtension))
													 .map(dd -> dd.getItemByName("icon_name").getValue())
													 .limit(1).collect(toList());
				if (iconNameList.size() > 0)
					return iconNameList.get(0);
			}
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Given a type name, return the icon name that matches it
	 * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aTypeName Type name to examine.
	 *
	 * @return Icon name.
	 */
	public String iconByTypeName(String aTypeName)
	{
		if (StringUtils.isNotEmpty(aTypeName))
		{
			List<String> iconNameList = mDataGrid.stream()
												 .filter(dd -> dd.getValueByName("type_name").equals(aTypeName))
												 .map(dd -> dd.getItemByName("icon_name").getValue())
												 .limit(1).collect(toList());
			if (iconNameList.size() > 0)
				return iconNameList.get(0);
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Given a type name, return the MIME type that matches it
	 * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aTypeName Type name to examine.
	 *
	 * @return MIME type.
	 */
	public String mimeByTypeName(String aTypeName)
	{
		if (StringUtils.isNotEmpty(aTypeName))
		{
			List<String> mimeTypeList = mDataGrid.stream()
												 .filter(dd -> dd.getValueByName("type_name").equals(aTypeName))
												 .map(dd -> dd.getItemByName("mime_type").getValue())
												 .limit(1).collect(toList());
			if (mimeTypeList.size() > 0)
				return mimeTypeList.get(0);
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Given a MIME type, return the type name that matches it
	 * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aMIMEType MIME type to examine.
	 *
	 * @return Type name.
	 */
	public String nameByMIMEType(String aMIMEType)
	{
		if (StringUtils.isNotEmpty(aMIMEType))
		{
			List<String> typeNameList = mDataGrid.stream()
												 .filter(dd -> dd.getValueByName("mime_type").equals(aMIMEType))
												 .map(dd -> dd.getItemByName("mime_type").getValue())
												 .limit(1).collect(toList());
			if (typeNameList.size() > 0)
				return typeNameList.get(0);
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Given a MIME type, return the icon name that matches it
	 * or <code>Content.CONTENT_TYPE_UNKNOWN</code>.
	 *
	 * @param aMIMEType MIME type to examine.
	 *
	 * @return Icon name.
	 */
	public String iconByMIMEType(String aMIMEType)
	{
		if (StringUtils.isNotEmpty(aMIMEType))
		{
			List<String> iconNameList = mDataGrid.stream()
												 .filter(dd -> dd.getValueByName("type_name").equals(aMIMEType))
												 .map(dd -> dd.getItemByName("icon_name").getValue())
												 .limit(1).collect(toList());
			if (iconNameList.size() > 0)
				return iconNameList.get(0);
		}

		return Content.CONTENT_TYPE_UNKNOWN;
	}

	/**
	 * Parses a CSV file identified by the path/file name parameter
	 * and loads it into an internally managed <i>DataGrid</i>.
	 *
	 * @param aPathFileName Absolute file name.
	 * @param aWithHeaders If <i>true</i>, then column headers will be
	 *                     recognized in the CSV file.
	 *
	 * @throws IOException I/O related exception.
	 */
	public void load(String aPathFileName, boolean aWithHeaders)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "load");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		DataGridCSV dataGridCSV = new DataGridCSV(mDataGrid);
		dataGridCSV.load(aPathFileName, aWithHeaders);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Parses a CSV file identified by the path/file name parameter
	 * and loads it into an internally managed <i>DataGrid</i>.
	 *
	 * @throws IOException I/O related exception.
	 * @throws FCException Missing property variable.
	 */
	public void load()
		throws IOException, FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "load");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String propertyName = "doc_type_file";
		String docTypeFileName = mCfgMgr.getString(propertyName, Content.CONTENT_DOCTYPE_FILE_DEFAULT);
		if (StringUtils.isEmpty(docTypeFileName))
		{
			String msgStr = String.format("Content property '%s' is undefined.",
										  mCfgMgr.getPrefix() + "." + propertyName);
			throw new FCException(msgStr);
		}
		String docTypePathFileName = String.format("%s%c%s", mAppCtx.getString(mAppCtx.APP_PROPERTY_CFG_PATH),
												   File.separatorChar, docTypeFileName);
		load(docTypePathFileName, true);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
