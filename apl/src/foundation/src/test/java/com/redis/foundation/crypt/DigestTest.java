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

package com.redis.foundation.crypt;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DigestTest
{
	private final String DIGEST_MESSAGE = "Jasypt is a java library which allows the developer to add basic encryption capabilities to his/her projects with minimum effort, and without the need of having deep knowledge on how cryptography works.";

	@Before
	public void setup()
	{
	}

	@Test
	public void exercise()
	throws UnsupportedEncodingException
	{
		Digest digestPassword = new Digest();

		String plainPassword = "This is a Test 12345 !@#$%^&*() of a password.";
		String encryptedPassword = digestPassword.encryptPassword(plainPassword);

		assertTrue("Passwords", digestPassword.isPasswordValid(plainPassword, encryptedPassword));

		Digest digestMessage = new Digest();

		String hashMessage1 = digestMessage.process(DIGEST_MESSAGE);
		String hashMessage2 = digestMessage.process(DIGEST_MESSAGE);

		assertNotNull(hashMessage1);
		assertNotNull(hashMessage2);

		byte[] hashMessage3 = digestMessage.process(DIGEST_MESSAGE.getBytes("UTF-8"));
		byte[] hashMessage4 = digestMessage.process(DIGEST_MESSAGE.getBytes("UTF-8"));

		assertNotNull(hashMessage3);
		assertNotNull(hashMessage4);
	}

	@After
	public void cleanup()
	{
	}
}
