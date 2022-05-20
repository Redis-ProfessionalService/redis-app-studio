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

package com.redis.foundation.mail;

import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

/**
 * The Mail class captures the constants, enumerated types
 * and utility methods for the Mail Manager package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Mail
{
	public static final String CFG_PROPERTY_PREFIX = "app.mail";

	public static final String STATUS_SUCCESS = "Success";
	public static final String STATUS_FAILURE = "Failure";

	public static final String MESSAGE_NONE = "None.";

	private Mail()
	{
	}

	/**
	 * Convenience method that extracts a first name from an email address
	 * formatted as 'first.last@company.com'.  The first name will have its
	 * first letter capitalized.
	 *
	 * @param anEmailAddress Email address.
	 *
	 * @return Proper first name.
	 */
	public static String extractFirstName(String anEmailAddress)
	{
		String firstName = StringUtils.EMPTY;

		if (StringUtils.isNotEmpty(anEmailAddress))
		{
			int offset = anEmailAddress.indexOf(StrUtl.CHAR_DOT);
			if (offset > 0)
				firstName = StrUtl.firstCharToUpper(anEmailAddress.substring(0, offset));
			else
			{
				offset = anEmailAddress.indexOf(StrUtl.CHAR_AT);
				if (offset > 0)
					firstName = StrUtl.firstCharToUpper(anEmailAddress.substring(0, offset));
			}
		}

		return firstName;
	}

	/**
	 * Convenience method that extracts a last name from an email address
	 * formatted as 'first.last@company.com'.  The last name will have its
	 * first letter capitalized.
	 *
	 * @param anEmailAddress Email address.
	 *
	 * @return Proper last name.
	 */
	public static String extractLastName(String anEmailAddress)
	{
		String lastName = StringUtils.EMPTY;

		if (StringUtils.isNotEmpty(anEmailAddress))
		{
			int offset2 = anEmailAddress.indexOf(StrUtl.CHAR_AT);
			if (offset2 > 0)
			{
				int offset1 = anEmailAddress.indexOf(StrUtl.CHAR_DOT);
				if (offset1 > 0)
					lastName = StrUtl.firstCharToUpper(anEmailAddress.substring(offset1+1, offset2));
				else
					lastName = StrUtl.firstCharToUpper(anEmailAddress.substring(0, offset2));
			}
		}

		return lastName;
	}
}
