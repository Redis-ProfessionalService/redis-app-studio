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

/**
 * The BitUtl class provides utility methods for bitwise mask
 * operations.  The goal of the class was to improve code readability.
 * An EnumSet is a better way to model bits.  If you decide to serialize
 * the enum, then you look at the ordinal() method.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2019
 * @since 1.0
 */
public class BitUtl
{
    /**
     * The <code>set</code> method uses the bitwise OR operator to assign
     * the individual bits of the bit mask parameter.
     * @param aValue Base value to apply bit mask to.
     * @param aBitMask Mask value that should be applied to the base.
     * @return The result from the OR operator.
     */
    public static int set(int aValue, int aBitMask)
    {
        return aValue |= aBitMask;
    }

    /**
     * The <code>isSet</code> method will return <i>true</i> if the bits
     * associated with the bit mask has been set.
     * @param aValue Base value to apply bit mask to.
     * @param aBitMask aBitMask Mask value that should be applied to the base.
     * @return <i>true</i> if the bits has been true, <i>false</i> otherwise.
     */
    public static boolean isSet(int aValue, int aBitMask)
    {
        return (aValue & aBitMask) == aBitMask;
    }

    /**
     * The <code>reset</code> method uses the bitwise XOR operator to assign
     * the individual bits of the bit mask parameter.
     * @param aValue Base value to apply bit mask to.
     * @param aBitMask Mask value that should be applied to the base.
     * @return The result value from the XOR operator.
     */
    public static int reset(int aValue, int aBitMask)
    {
        return aValue &= ~aBitMask;
    }
}
