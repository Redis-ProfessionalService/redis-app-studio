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

import java.nio.ByteBuffer;

/**
 * The BufUtl class provides utility methods for managing serialization
 * and de-serialization of core Java data types.  This class is heavily
 * referenced by the <code>PacketBuffer</code> class.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2019
 * @since 1.0
 */
@SuppressWarnings({"UnusedDeclaration"})
public class BufUtl
{
    private static final int DATATYPE_CHAR_SIZE = 2;
    private static final int DATATYPE_SHORT_SIZE = 2;
    private static final int DATATYPE_BOOLEAN_SIZE = 2;
    private static final int DATATYPE_INTEGER_SIZE = 4;
    private static final int DATATYPE_FLOAT_SIZE = 4;
    private static final int DATATYPE_LONG_SIZE = 8;
    private static final int DATATYPE_DOUBLE_SIZE = 8;

    /**
     * Returns the size (in bytes) of <i>char</i> type.
     *
     * @param aChar Character parameter.
     * @return Size of a <i>char</i> type.
     */
    public static int getSize(char aChar)
    {
        return DATATYPE_CHAR_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>boolean</i> type.
     *
     * @param aFlag Boolean parameter.
     * @return Size of a <i>boolean</i> type.
     */
    public static int getSize(boolean aFlag)
    {
        return DATATYPE_BOOLEAN_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>short</i> type.
     *
     * @param aNumber Short parameter.
     * @return Size of a <i>short</i> type.
     */
    public static int getSize(short aNumber)
    {
        return DATATYPE_SHORT_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>int</i> type.
     *
     * @param aNumber Integer parameter.
     * @return Size of a <i>int</i> type.
     */
    public static int getSize(int aNumber)
    {
        return DATATYPE_INTEGER_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>long</i> type.
     *
     * @param aNumber Long parameter.
     * @return Size of a <i>long</i> type.
     */
    public static int getSize(long aNumber)
    {
        return DATATYPE_LONG_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>float</i> type.
     *
     * @param aNumber Float parameter.
     * @return Size of a <i>float</i> type.
     */
    public static int getSize(float aNumber)
    {
        return DATATYPE_FLOAT_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>double</i> type.
     *
     * @param aNumber Double parameter.
     * @return Size of a <i>double</i> type.
     */
    public static int getSize(double aNumber)
    {
        return DATATYPE_DOUBLE_SIZE;
    }

    /**
     * Returns the size (in bytes) of <i>String</i> type.
     *
     * @param aString String parameter.
     * @return Size of a <i>String</i> type.
     */
    public static int getSize(String aString)
    {
        if (StringUtils.isEmpty(aString))
            return DATATYPE_INTEGER_SIZE;
        else
            return DATATYPE_INTEGER_SIZE + (aString.length() * DATATYPE_CHAR_SIZE);
    }

    /**
     * Resets the <code>ByteBuffer</code> position and adds the opcode and
     * version values to it.
     *
     * @param aBuffer Packet byte buffer object.
     * @param anOpCode Application specific operation code value.
     * @param aVersion Application specific operation version code value.
     */
    public static void setHeader(ByteBuffer aBuffer, int anOpCode, int aVersion)
    {
        if (aBuffer != null)
        {
            aBuffer.mark();
            aBuffer.position(0);
            aBuffer.putInt(anOpCode);
            aBuffer.putInt(aVersion);
            aBuffer.reset();
        }
    }

    /**
     * Retrieves the operation code stored within the header of the
     * <code>ByteBuffer</code> object.
     *
     * @param aBuffer Packet byte buffer object.
     * @return Application specific operation code value.
     */
    public static int getOpCode(ByteBuffer aBuffer)
    {
        int opCode;

        if (aBuffer == null)
            return -1;
        else
        {
            aBuffer.mark();
            aBuffer.position(0);
            opCode = aBuffer.getInt();
            aBuffer.reset();

            return opCode;
        }
    }

    /**
     * Retrieves the operation version code stored within the header of the
     * <code>ByteBuffer</code> object.
     *
     * @param aBuffer Packet byte buffer object.
     * @return Application specific operation code version value.
     */
    @SuppressWarnings({"UnusedAssignment"})
    public static int getVersion(ByteBuffer aBuffer)
    {
        int opCode, versionId;

        if (aBuffer == null)
            return -1;
        else
        {
            aBuffer.mark();
            aBuffer.position(0);
            opCode = aBuffer.getInt();
            versionId = aBuffer.getInt();
            aBuffer.reset();

            return versionId;
        }
    }

    /**
     * Stores the <i>boolean</i> flag parameter value into the
     * <code>ByteBuffer</code> object.
     *
     * @param aBuffer Packet byte buffer object.
     * @param aFlag A boolean flag value.
     */
    public static void putBool(ByteBuffer aBuffer, boolean aFlag)
    {
        if (aFlag)
            aBuffer.putShort((short) 1);
        else
            aBuffer.putShort((short) 0);
    }

    /**
     * Retrieves a <i>boolean</i> value stored within the body of the
     * <code>ByteBuffer</code> object.
     *
     * @param aBuffer Packet byte buffer object.
     * @return <i>true</i> or <i>false</i> based on the value stored within
     * the byte buffer object.
     */
    public static boolean getBool(ByteBuffer aBuffer)
    {
        short flagValue;

        flagValue = aBuffer.getShort();
        return (flagValue == 1);
    }

    /**
     * Stores the <i>String</i> parameter value into the
     * <code>ByteBuffer</code> object.
     *
     * @param aBuffer Packet byte buffer object.
     * @param aString A non-null string.
     */
    public static void putString(ByteBuffer aBuffer, String aString)
    {
        int strLength;

        if ((aBuffer != null) && (aString != null))
        {
            strLength = aString.length();
            aBuffer.putInt(strLength);
            for (int i = 0; i < strLength; i++)
                aBuffer.putChar(aString.charAt(i));
        }
    }

    /**
     * Retrieves a <i>String</i> value stored within the body of the
     * <code>ByteBuffer</code> object.
     *
     * @param aBuffer Packet byte buffer object.
     * @return A <i>String</i> object.
     */
    public static String getString(ByteBuffer aBuffer)
    {
        int strLength;
        StringBuilder strBuilder;

        strLength = aBuffer.getInt();
        if (strLength > 0)
            strBuilder = new StringBuilder(strLength+10);
        else
            strBuilder = new StringBuilder();
        for (int i = 0; i < strLength; i++)
            strBuilder.append(aBuffer.getChar());
        return strBuilder.toString();
    }
}
