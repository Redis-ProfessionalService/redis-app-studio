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

import org.apache.commons.lang3.StringUtils;
import org.jasypt.util.numeric.BasicDecimalNumberEncryptor;
import org.jasypt.util.numeric.BasicIntegerNumberEncryptor;
import org.jasypt.util.text.BasicTextEncryptor;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * The Secret provides a collection of basic methods for encrypting
 * and decrypting messages using a password.  The process is bi-directional
 * so the approach is considered less secure.
 * <p>
 * This class utilizes the
 * <a href="http://www.jasypt.org/">Java Simplified Encryption</a>
 * framework to manage the transformations.
 * </p>
 *
 * @see <a href="http://www.jasypt.org/">Java Simplified Encryption</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class Secret
{
	private String mPassword;

	/**
	 * Default constructor.
	 */
	public Secret()
	{
		mPassword = StringUtils.EMPTY;
	}

	/**
	 * Constructor that accepts a password that will be used
	 * for encryption.
	 *
	 * @param aPassword Password string.
	 */
	public Secret(String aPassword)
	{
		setPassword(aPassword);
	}

	/**
	 * Assigns a password string for data encryption.
	 *
	 * @param aPassword Password string.
	 */
	public void setPassword(String aPassword)
	{
		mPassword = aPassword;
	}

	/**
	 * Returns the password string used for data
	 * encryption.
	 *
	 * @return Password string.
	 */
	public String getPassword()
	{
		return mPassword;
	}

	/**
	 * Encrypts the parameter using the internal password string.
	 *
	 * @param aMessage Message to encrypt.
	 *
	 * @return An encrypted message.
	 */
	public String encrypt(String aMessage)
	{
		BasicTextEncryptor basicTextEncryptor = new BasicTextEncryptor();
		basicTextEncryptor.setPassword(mPassword);

		return basicTextEncryptor.encrypt(aMessage);
	}

	/**
	 * Decrypts a previously encrypted message using the internal
	 * password string.
	 *
	 * @param anEncryptedMessage Encrypted message.
	 *
	 * @return Unencrypted message.
	 */
	public String decrypt(String anEncryptedMessage)
	{
		BasicTextEncryptor basicTextEncryptor = new BasicTextEncryptor();
		basicTextEncryptor.setPassword(mPassword);

		return basicTextEncryptor.decrypt(anEncryptedMessage);
	}

	/**
	 * Encrypts the parameter using the internal password string.
	 *
	 * @param aNumber Number to encrypt.
	 *
	 * @return An encrypted number.
	 */
	public BigInteger encrypt(BigInteger aNumber)
	{
		BasicIntegerNumberEncryptor basicIntegerNumberEncryptor = new BasicIntegerNumberEncryptor();
		basicIntegerNumberEncryptor.setPassword(mPassword);

		return basicIntegerNumberEncryptor.encrypt(aNumber);
	}

	/**
	 * Decrypts a previously encrypted number using the internal
	 * password string.
	 *
	 * @param aNumber Encrypted message.
	 *
	 * @return Unencrypted number.
	 */
	public BigInteger decrypt(BigInteger aNumber)
	{
		BasicIntegerNumberEncryptor basicIntegerNumberEncryptor = new BasicIntegerNumberEncryptor();
		basicIntegerNumberEncryptor.setPassword(mPassword);

		return basicIntegerNumberEncryptor.decrypt(aNumber);
	}

	/**
	 * Encrypts the parameter using the internal password string.
	 *
	 * @param aNumber Number to encrypt.
	 *
	 * @return An encrypted number.
	 */
	public BigDecimal encrypt(BigDecimal aNumber)
	{
		BasicDecimalNumberEncryptor basicDecimalNumberEncryptor = new BasicDecimalNumberEncryptor();
		basicDecimalNumberEncryptor.setPassword(mPassword);

		return basicDecimalNumberEncryptor.encrypt(aNumber);
	}

	/**
	 * Decrypts a previously encrypted number using the internal
	 * password string.
	 *
	 * @param aNumber Encrypted message.
	 *
	 * @return Unencrypted number.
	 */
	public BigDecimal decrypt(BigDecimal aNumber)
	{
		BasicDecimalNumberEncryptor basicDecimalNumberEncryptor = new BasicDecimalNumberEncryptor();
		basicDecimalNumberEncryptor.setPassword(mPassword);

		return basicDecimalNumberEncryptor.decrypt(aNumber);
	}
}
