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

import org.jasypt.digest.StandardByteDigester;
import org.jasypt.digest.StandardStringDigester;
import org.jasypt.util.password.StrongPasswordEncryptor;

/**
 * The Digest provides a collection of basic methods for
 * hashing/digesting messages using a random salt algorithm.
 * The process is one directional - you cannot retrieve the
 * original message - only compare that two hash values are
 * identical.
 * <p>
 * This class utilizes the
 * <a href="http://www.jasypt.org/">jasypt</a>
 * framework to manage the transformations.
 * </p>
 *
 * @see <a href="http://www.jasypt.org/">Java Simplified Encryption</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class Digest
{
	/**
	 * Default constructor.
	 */
	public Digest()
	{
	}

	/**
	 * Process the parameter message using a hashing algorithm.
	 *
	 * @param aMessage Message to digest.
	 *
	 * @return Hashed value.
	 */
	public String process(String aMessage)
	{
		StandardStringDigester standardStringDigester = new StandardStringDigester();
		standardStringDigester.setAlgorithm("SHA-256");
		standardStringDigester.setIterations(50000);
		standardStringDigester.setSaltSizeBytes(16);
		standardStringDigester.initialize();

		return standardStringDigester.digest(aMessage);
	}

	/**
	 * Process the parameter message using a hashing algorithm.
	 *
	 * @param aMessage Message to digest.
	 *
	 * @return Hashed value.
	 */
	public byte[] process(byte[] aMessage)
	{
		StandardByteDigester standardByteDigester = new StandardByteDigester();
		standardByteDigester.setAlgorithm("SHA-256");
		standardByteDigester.setIterations(50000);
		standardByteDigester.setSaltSizeBytes(16);
		standardByteDigester.initialize();

		return standardByteDigester.digest(aMessage);
	}

	/**
	 * This is a one-way hashing algorithm for passwords.
	 *
	 * @param aPassword Password to hash.
	 *
	 * @return Encrypted hash value.
	 */
	public String encryptPassword(String aPassword)
	{
		StrongPasswordEncryptor strongPasswordEncryptor = new StrongPasswordEncryptor();

		return strongPasswordEncryptor.encryptPassword(aPassword);
	}

	/**
	 * Identifies if the plain password matches a previously hashed password
	 * value.
	 *
	 * @param aPassword Password (plain text).
	 *
	 * @param aHashPassword Previously hashed password value.
	 *
	 * @return <i>true</i> if they match or <i>false</i> otherwise.
	 */
	public boolean isPasswordValid(String aPassword, String aHashPassword)
	{
		StrongPasswordEncryptor strongPasswordEncryptor = new StrongPasswordEncryptor();

		return strongPasswordEncryptor.checkPassword(aPassword, aHashPassword);
	}
}
