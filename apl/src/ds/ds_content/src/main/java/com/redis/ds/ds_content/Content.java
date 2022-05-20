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

import com.redis.foundation.std.DigitalHash;

import java.io.IOException;
import java.util.UUID;

/**
 * The Content class captures the constants, enumerated types
 * and utility methods for the content text extraction package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Content
{
	public static final String CFG_PROPERTY_PREFIX = "ds.content";

	public static final String CFG_CONTENT_LIMIT = "content_limit";
	public static final String CFG_CONTENT_ENCODING = "content_encoding";

	public static final int CONTENT_LIMIT_DEFAULT = 250000;

	public static final String CONTENT_TYPE_HTML = "text/html";
	public static final String CONTENT_TYPE_UNKNOWN = "Unknown";
	public static final String CONTENT_TYPE_TXT_CSV = "text/csv";
	public static final String CONTENT_TYPE_APP_CSV = "application/csv";
	public static final String CONTENT_TYPE_DEFAULT = CONTENT_TYPE_UNKNOWN;

	public static final String CONTENT_FIELD_METADATA = "rl_md_";

	public static final String CONTENT_DOCTYPE_FILE_DEFAULT = "document_types.csv";

	private Content()
	{
	}

	/**
	 * Generates a unique hash string using the MD5 algorithm using
	 * the path/file name.
	 *
	 * @param aPathFileName Name of path/file to base hash on.
	 *
	 * @return Unique hash string.
	 */
	public static String hashId(String aPathFileName)
	{
		String hashId;

		DigitalHash digitalHash = new DigitalHash();
		try
		{
			digitalHash.processBuffer(aPathFileName);
			hashId = digitalHash.getHashSequence();
		}
		catch (IOException e)
		{
			UUID uniqueId = UUID.randomUUID();
			hashId = uniqueId.toString();
		}

		return hashId;
	}
}
