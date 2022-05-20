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

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The DigitalHash class provides a simple collection of methods to generate
 * digital hash sequence values (based on the MD5 message digest algorithm)
 * for <i>String</i> objects and files.
 * <p>
 *     Probability of two hashes accidentally colliding is 1 in 340 undecillion
 *     282 decillion 366 nonillion 920 octillion 938 septillion 463 sextillion
 *     463 quintillion 374 quadrillion 607 trillion 431 billion 768 million 211
 *     thousand 456.
 * </p>
 * <p>
 *     To have a 50% chance of any hash colliding with any other hash you need 2^64
 *     hashes. This means that to get a collision, on average, you'll need to hash
 *     6 billion files per second for 100 years.
 * </p>
 *
 * @see <a href="http://www.mkyong.com/java/java-md5-hashing-example/">MD5 Checksum</a>
 * @see <a href="http://stackoverflow.com/questions/201705/how-many-random-elements-before-md5-produces-collisions">Collision Odds</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DigitalHash
{
    private final int FILEIO_BUFFER_SIZE = 8096;

    private MessageDigest mMsgDigest;

    /**
     * Initializes the MD5 message digest subsystem.
     */
    public DigitalHash()
    {
        try
        {
            mMsgDigest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            mMsgDigest = null;
        }
    }

    /**
     * Resets the message digest algorithm and empties the internal hash
     * string.  This should be invoked when you wish to restart another
     * hash calculation sequence.
     */
    public void reset()
    {
        mMsgDigest.reset();
    }

    /**
     * Converts the message digest byte sequence values into a hexadecimal
     * string representation.
     *
     * @return A hexadecimal string.
     */
    public String getHashSequence()
    {
        byte[] digestBytes;
        StringBuilder hexStrBuilder;

        if (mMsgDigest == null)
            return StringUtils.EMPTY;
        else
        {
            digestBytes = mMsgDigest.digest();
            hexStrBuilder = new StringBuilder();

            for (byte digestByte : digestBytes)
            {
                if ((digestByte >= 0) && (digestByte < 16))
                    hexStrBuilder.append("0");
                hexStrBuilder.append(Integer.toHexString(0xFF & digestByte));
            }
            return hexStrBuilder.toString();
        }
    }

    /**
     * Process the contents of the string buffer through the MD5 message
     * digest algorithm.
     *
     * @param aBuffer A string buffer.
     *
     * @throws IOException If the ByteArrayInputStream cannot read the
     * string buffer properly.
     */
    @SuppressWarnings({"StatementWithEmptyBody"})
    public void processBuffer(String aBuffer)
        throws IOException
    {
        if (StringUtils.isNotEmpty(aBuffer))
            mMsgDigest.update(aBuffer.getBytes(StrUtl.CHARSET_UTF_8));
    }

    /**
     * Process the contents of the file through the MD5 message
     * digest algorithm.
     * <p>
     * <b>Note:</b> This method calculates the hash value based
     * on the raw byte in the file (no encoding is performed).
     * </p>
     *
     * @param aFile Representing the file to process.
     *
     * @throws IOException If the FileInputStream cannot read the
     * file properly.
     */
    public void processFile(File aFile)
        throws IOException
    {
        int bytesRead;

        FileInputStream  fileIS = new FileInputStream(aFile);
        byte[] byteArray = new byte[FILEIO_BUFFER_SIZE];

        while ((bytesRead = fileIS.read(byteArray, 0, FILEIO_BUFFER_SIZE)) != -1)
            mMsgDigest.update(byteArray, 0, bytesRead);

        fileIS.close();
    }

    /**
     * Process the contents of the file through the MD5 message
     * digest algorithm.
     *
     * @param aFileName Identifying the file to process.
     *
     * @throws IOException If the FileInputStream cannot read the
     * file properly.
     */
    public void processFile(String aFileName)
        throws IOException
    {
        processFile(new File(aFileName));
    }
}
