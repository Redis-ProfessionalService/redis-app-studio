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
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.io.IO;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tika.Tika;
import org.apache.tika.fork.ForkParser;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;

import java.io.*;
import java.net.URL;
import java.util.Optional;

/**
 * The ContentParser class is responsible for parsing textual content
 * from a file.  The Apache Tika™ toolkit detects and extracts metadata and
 * text content from various documents - from PPT to CSV to PDF - using
 * existing parser libraries. Tika unifies these parsers under a single
 * interface to allow you to easily parse over a thousand different file
 * types. Tika is useful for search engine indexing, content analysis,
 * translation, and much more.
 *
 * @see <a href="http://tika.apache.org/">Apache Tika</a>
 * @see <a href="http://www.massapi.com/class/org/apache/tika/fork/ForkParser.html">Apache Tika ForkParser</a>
 * @see <a href="https://www.tutorialspoint.com/tika/tika_metadata_extraction.htm">Apache Tika Metadata Tutorial</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class ContentParser
{
	private AppCtx mAppCtx;
	private CfgMgr mCfgMgr;
	private DataDoc mDataDoc;

	/**
	 * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	 */
	public ContentParser(AppCtx anAppCtx)
	{
		mAppCtx = anAppCtx;
		mCfgMgr = new CfgMgr(mAppCtx, Content.CFG_PROPERTY_PREFIX);
	}

	/**
	 * Constructor accepts an application context parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppCtx Application context.
	 * @param aDataDoc Data document instance used for meta data population
	 */
	public ContentParser(AppCtx anAppCtx, DataDoc aDataDoc)
	{
		mAppCtx = anAppCtx;
		mDataDoc = aDataDoc;
		mCfgMgr = new CfgMgr(mAppCtx, Content.CFG_PROPERTY_PREFIX);
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
	 * Quick test to determine if the file is valid for content
	 * extraction.
	 *
	 * @param aFile File instance.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFileValid(File aFile)
	{
		if ((aFile != null) && (aFile.exists()))
			return aFile.length() > 0L;
		else
			return false;
	}

	/**
	 * Quick test to determine if the file is valid for content
	 * extraction.
	 *
	 * @param aPathFileName Path/File name.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isFileValid(String aPathFileName)
	{
		return isFileValid(new File(aPathFileName));
	}

	/**
	 * Uses the Tika subsystem to detect the file type.  The details of
	 * that detection approach are described on the Content Detection
	 * web page.
	 *
	 * @param aFile File instance.
	 *
	 * @return String representation of the file type.
	 *
	 * @see <a href="http://tika.apache.org/1.24/detection.html">Content Detection</a>
	 *
	 */
	public String detectType(File aFile)
	{
		Logger appLogger = mAppCtx.getLogger(this, "detectType");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String contentType = Content.CONTENT_TYPE_DEFAULT;

		if (isFileValid(aFile))
		{
			Tika tikaFacade = new Tika();
			try
			{
				contentType = tikaFacade.detect(aFile);
			}
			catch (IOException e)
			{
				String msgStr = String.format("%s: %s", aFile.getAbsolutePath(), e.getMessage());
				appLogger.error(msgStr, e);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return contentType;
	}

	/**
	 * Uses the Tika subsystem to detect the file type. The details of
	 * that detection approach are described on the Content Detection
	 * web page.
	 *
	 * @param aURL URL of the resource.
	 *
	 * @return String representation of the file type.
	 *
	 * @see <a href="http://tika.apache.org/1.6/detection.html">Content Detection</a>
	 */
	public String detectType(URL aURL)
	{
		Logger appLogger = mAppCtx.getLogger(this, "detectType");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String contentType = Content.CONTENT_TYPE_DEFAULT;

		if (aURL != null)
		{
			Tika tikaFacade = new Tika();
			try
			{
				contentType = tikaFacade.detect(aURL);
			}
			catch (IOException e)
			{
				String msgStr = String.format("%s: %s", aURL.toString(), e.getMessage());
				appLogger.error(msgStr, e);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return contentType;
	}

	/**
	 * Uses the Tika subsystem to detect the file type.  The details of
	 * that detection approach are described on the Content Detection
	 * web page.
	 * The type detection is based on known file name extensions.
	 * <p>
	 * The given name can also be a URL or a full file path. In such cases
	 * only the file name part of the string is used for type detection.
	 * </p>
	 *
	 * @param aName Name of the document.
	 *
	 * @return String representation of the file type.
	 *
	 * @see <a href="http://tika.apache.org/1.6/detection.html">Content Detection</a>
	 */
	public String detectType(String aName)
	{
		Logger appLogger = mAppCtx.getLogger(this, "detectType");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String contentType = Content.CONTENT_TYPE_DEFAULT;

		if (StringUtils.isNotEmpty(aName))
		{
			Tika tikaFacade = new Tika();
			contentType = tikaFacade.detect(aName);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return contentType;
	}

	private void addAssignItem(String aName, String aValue)
	{
		if (mDataDoc != null)
		{
			Optional<DataItem> optDataItem = mDataDoc.getItemByNameOptional(aName);
			if (optDataItem.isPresent())
			{
				DataItem dataItem = optDataItem.get();
				dataItem.setValue(aValue);
			}
			else
				mDataDoc.add(new DataItem.Builder().name(aName).title(Data.nameToTitle(aName)).value(aValue).build());
		}
	}

	/**
	 * This method will extract the textual content from the input file
	 * and write it to the writer stream.  If a document instance has been
	 * registered with the class, then meta data items will dynamically
	 * be assigned as they are discovered.
	 *
	 * @param anInFile Input file instance.
	 * @param aWriter Output writer stream.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	@SuppressWarnings("deprecation")
	public void process(File anInFile, Writer aWriter)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (isFileValid(anInFile))
		{
			appLogger.debug(String.format("[%s] %s", detectType(anInFile), anInFile.getAbsolutePath()));

			ForkParser forkParser = null;
			Metadata tikaMetaData = new Metadata();
			tikaMetaData.set(Metadata.RESOURCE_NAME_KEY, anInFile.getName());
			int contentLimit = mCfgMgr.getInteger("content_limit", Content.CONTENT_LIMIT_DEFAULT);

			InputStream inputStream = null;
			try
			{
				Parser tikaParser;
				ParseContext parseContext;

				inputStream = TikaInputStream.get(anInFile.toPath());
				if (mCfgMgr.isStringTrue("tika_fork_parser"))
				{
					forkParser = new ForkParser(ContentParser.class.getClassLoader(), new AutoDetectParser());
					String javaCmdStr = mCfgMgr.getString("tika_fork_java_cmd");
					if (StringUtils.isNotEmpty(javaCmdStr))
						forkParser.setJavaCommand(javaCmdStr);
					int poolSize = mCfgMgr.getInteger("tika_fork_pool_size", 5);
					if (poolSize > 0)
						forkParser.setPoolSize(poolSize);
					tikaParser = forkParser;
					parseContext = new ParseContext();
				}
				else
				{
					tikaParser = new AutoDetectParser();
					parseContext = new ParseContext();
				}

				WriteOutContentHandler writeOutContentHandler = new WriteOutContentHandler(aWriter, contentLimit);
				tikaParser.parse(inputStream, writeOutContentHandler, tikaMetaData, parseContext);
			}
			catch (Exception e)
			{
				String eMsg = e.getMessage();
				String msgStr = String.format("%s: %s", anInFile.getAbsolutePath(), eMsg);

/* The following logic checks to see if this exception was triggered simply because
the total character limit threshold was hit.  If that is all it was, then return true. */

				if (StringUtils.startsWith(eMsg, "Your document contained more than"))
					appLogger.warn(msgStr);
				else
					throw new FCException(msgStr);
			}
			finally
			{
				if (inputStream != null)
					IOUtils.closeQuietly(inputStream);
			}

			if ((mDataDoc != null) && (mCfgMgr.isStringTrue("content_metadata")))
			{
				String mdValue;
				String[] metaDataNames = tikaMetaData.names();
				for (String mdName : metaDataNames)
				{
					mdValue = tikaMetaData.get(mdName);
					if (StringUtils.isNotEmpty(mdValue))
						addAssignItem(Content.CONTENT_FIELD_METADATA + mdName, mdValue);
				}
			}

			if (forkParser != null)
				forkParser.close();
		}
		else
		{
			String msgStr = String.format("%s: Does not exist or is empty.", anInFile.getAbsolutePath());
			throw new FCException(msgStr);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * This method will extract the textual content from the URL
	 * and write it to the writer stream.  If a document instance has been
	 * registered with the class, then meta data items will dynamically
	 * be assigned as they are discovered.
	 *
	 * @param aURL URL of the resource.
	 * @param aWriter Output writer stream.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	@SuppressWarnings("deprecation")
	public void process(URL aURL, Writer aWriter)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if ((aURL == null) || (aWriter == null))
			throw new FCException("One or more parameters are null.");

		String documentName = aURL.toString();
		appLogger.debug(String.format("[%s] %s", detectType(aURL), documentName));

		Metadata tikaMetaData = new Metadata();
		int contentLimit = mCfgMgr.getInteger("content_limit", Content.CONTENT_LIMIT_DEFAULT);

		InputStream inputStream = null;
		try
		{
			Parser tikaParser;
			ParseContext parseContext;

			inputStream = TikaInputStream.get(aURL);
			if (mCfgMgr.isStringTrue("tika_fork_parser"))
			{
				ForkParser forkParser = new ForkParser(ContentParser.class.getClassLoader(), new AutoDetectParser());
				String javaCmdStr = mCfgMgr.getString("tika_fork_java_cmd");
				if (StringUtils.isNotEmpty(javaCmdStr))
					forkParser.setJavaCommand(javaCmdStr);
				int poolSize = mCfgMgr.getInteger("tika_fork_pool_size", 5);
				if (poolSize > 0)
					forkParser.setPoolSize(poolSize);
				tikaParser = forkParser;
				parseContext = new ParseContext();
			}
			else
			{
				tikaParser = new AutoDetectParser();
				parseContext = new ParseContext();
			}

			WriteOutContentHandler writeOutContentHandler = new WriteOutContentHandler(aWriter, contentLimit);
			tikaParser.parse(inputStream, writeOutContentHandler, tikaMetaData, parseContext);
		}
		catch (Exception e)
		{
			String eMsg = e.getMessage();
			String msgStr = String.format("%s: %s", documentName, eMsg);

/* The following logic checks to see if this exception was triggered simply because
the total character limit threshold was hit.  If that is all it was, then return true. */

			if (StringUtils.startsWith(eMsg, "Your document contained more than"))
				appLogger.warn(msgStr);
			else
				throw new FCException(msgStr);
		}
		finally
		{
			if (inputStream != null)
				IOUtils.closeQuietly(inputStream);
		}

		if ((mDataDoc != null) && (mCfgMgr.isStringTrue("content_metadata")))
		{
			String mdValue;
			String[] metaDataNames = tikaMetaData.names();
			for (String mdName : metaDataNames)
			{
				mdValue = tikaMetaData.get(mdName);
				if (StringUtils.isNotEmpty(mdValue))
					addAssignItem(Content.CONTENT_FIELD_METADATA + mdName, mdValue);
			}
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * This method will extract the textual content from the input file
	 * and write it to the output file.  If a document instance has been
	 * registered with the class, then meta data items will dynamically
	 * be assigned as they are discovered.
	 *
	 * @param anInFile Input file instance.
	 * @param anOutFile Output file instance.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	public void process(File anInFile, File anOutFile)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		BufferedWriter bufferedWriter = null;
		String contentEncoding = mCfgMgr.getString("content_encoding", StrUtl.CHARSET_UTF_8);
		try
		{
			FileOutputStream fileOutputStream = new FileOutputStream(anOutFile);
			bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream, contentEncoding));
			process(anInFile, bufferedWriter);
		}
		catch (IOException e)
		{
			String msgStr = String.format("%s: %s", anInFile.getAbsolutePath(), e.getMessage());
			appLogger.error(msgStr, e);
			throw new FCException(e);
		}
		finally
		{
			if (bufferedWriter != null)
				IO.closeQuietly(bufferedWriter);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * This method will extract the textual content from the input file
	 * name and write it to the output file name.  If a document instance has been
	 * registered with the class, then meta data items will dynamically
	 * be assigned as they are discovered.
	 *
	 * @param anInputPathFileName Input path/file name.
	 * @param anOutputPathFileName Output path/file name.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	public void process(String anInputPathFileName, String anOutputPathFileName)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		process(new File(anInputPathFileName), new File(anOutputPathFileName));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}

	/**
	 * This method will extract the textual content from the input file
	 * and capture it in a string.  If a document instance has been registered
	 * with the class, then meta data items will dynamically be assigned
	 * as they are discovered.
	 *
	 * @param anInFile Input file instance.
	 *
	 * @return String representation of the textual content.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	public String process(File anInFile)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		StringWriter stringWriter = new StringWriter();
		try (PrintWriter printWriter = new PrintWriter(stringWriter))
		{
			process(anInFile, printWriter);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return stringWriter.toString();
	}

	/**
	 * This method will extract the textual content from the URL and
	 * capture it in a string.  If a document instance has been registered
	 * with the class, then meta data items will dynamically be assigned
	 * as they are discovered.
	 *
	 * @param aURL URL of the resource.
	 *
	 * @return String representation of the textual content.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	public String process(URL aURL)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		StringWriter stringWriter = new StringWriter();
		try (PrintWriter printWriter = new PrintWriter(stringWriter))
		{
			process(aURL, printWriter);
		}

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return stringWriter.toString();
	}

	/**
	 * This method will extract the textual content from the input file
	 * and capture it in a string.  If a document instance has been registered
	 * with the class, then meta data items will dynamically be assigned
	 * as they are discovered.
	 *
	 * @param anInputPathFileName Input path/file name.
	 *
	 * @return String representation of the textual content.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	public String process(String anInputPathFileName)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		String contentString = process(new File(anInputPathFileName));

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return contentString;
	}

	/**
	 * This method will extract the textual content from the input file
	 * and capture it in the content field.  If a document instance has been
	 * registered with the class, then meta data items will dynamically
	 * be assigned as they are discovered.
	 *
	 * @param anInputPathFileName Input path/file name.
	 * @param aContentItem Content data item instance.
	 *
	 * @throws FCException Thrown when IOExceptions are detected.
	 */
	public void process(String anInputPathFileName, DataItem aContentItem)
		throws FCException
	{
		Logger appLogger = mAppCtx.getLogger(this, "process");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		if (aContentItem == null)
			throw new FCException("Content data field is null.");

		String contentString = process(anInputPathFileName);
		if (StringUtils.isNotEmpty(contentString))
			aContentItem.setValue(contentString);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
	}
}
