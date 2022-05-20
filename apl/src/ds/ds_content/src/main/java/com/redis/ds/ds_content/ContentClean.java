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
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ContentClean
{
	private AppCtx mAppCtx;

	/**
	 * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	 */
	public ContentClean(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
	}

	private boolean isASCII(char aChar)
	{
		return aChar < 128;
	}

	private String stripControl(String aValue)
	{
		String cleanValue;

		if (StringUtils.isNotEmpty(aValue))
		{
			String[] nlReplace = new String[]{" ", " ", " "};
			String[] nlPattern = new String[]{"\r", "\n", "\t"};

			String replaceValue = StringUtils.replaceEach(aValue, nlPattern, nlReplace);
			replaceValue = StringUtils.trim(replaceValue);
			cleanValue = replaceValue.replaceAll("\\p{Cntrl}", "");

			StringBuilder asciiValue = new StringBuilder(cleanValue.length());
			int strLength = cleanValue.length();
			for (int i = 0; i < strLength; i++)
			{
				if (isASCII(cleanValue.charAt(i)))
					asciiValue.append(cleanValue.charAt(i));
			}
			cleanValue = asciiValue.toString();
		}
		else
			cleanValue = StringUtils.EMPTY;

		return cleanValue;
	}

	private String stripSpaces(String aValue)
	{
		String cleanValue;

		if (StringUtils.isNotEmpty(aValue))
		{
			String trimValue = StringUtils.trim(aValue);
			cleanValue = trimValue.replaceAll("\\s+", " ");
		}
		else
			cleanValue = StringUtils.EMPTY;

		return cleanValue;
	}

	private String stripDots(String aValue)
	{
		String cleanValue;

		if (StringUtils.isNotEmpty(aValue))
			cleanValue = aValue.replaceAll("\\.+", ".");
		else
			cleanValue = StringUtils.EMPTY;

		return cleanValue;
	}

	/**
	 * Strips control, non-ASCII and consecutive dots from the value string.
	 *
	 * @param aValue String value of characters.
	 *
	 * @return Stripped string of control, non-ASCII and consecutive dots
	 */
	public String strip(String aValue)
	{
		Logger appLogger = mAppCtx.getLogger(this, "strip");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String cleanControl = stripControl(aValue);
		String cleanSpaces = stripSpaces(cleanControl);
		String cleanOfValues = stripDots(cleanSpaces);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return cleanOfValues;
	}

	private ArrayList<String> loadFileList(String aPathFileName)
		throws IOException
	{
		Logger appLogger = mAppCtx.getLogger(this, "loadFileList");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		List<String> lineList;

		try (FileReader fileReader = new FileReader(aPathFileName))
		{
			lineList = IOUtils.readLines(fileReader);
		}

		ArrayList<String> fileList = new ArrayList<>();

		for (String followString : lineList)
		{
			if (! StringUtils.startsWith(followString, "#"))
				fileList.add(followString);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return fileList;
	}

	/**
	 * Reads the text input file name into memory, cleans the lines and writes
	 * it to the output file name.
	 *
	 * @param anInputPathFileName Input path file name to clean
	 * @param anOutputPathFileName Output path file name to create
	 *
	 * @throws IOException I/O exception
	 */
	public void readCleanWriteFile(String anInputPathFileName, String anOutputPathFileName)
		throws IOException
	{
		String cleanLine;
		Logger appLogger = mAppCtx.getLogger(this, "readCleanWriteFile");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		ArrayList<String> fileInputLines = loadFileList(anInputPathFileName);
		try (PrintWriter printWriter = new PrintWriter(anOutputPathFileName, StrUtl.CHARSET_UTF_8))
		{
			for (String inputLine : fileInputLines)
			{
				cleanLine = strip(inputLine);
				printWriter.printf("%s%n", cleanLine);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
