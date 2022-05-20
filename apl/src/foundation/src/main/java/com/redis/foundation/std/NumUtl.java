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
 * The NumUtl class provides static convenience methods for determining the
 * type of value the number represents.
 * <p>
 * The Apache Commons has a number of good utility methods for numeric values.
 * http://commons.apache.org/lang/api-release/org/apache/commons/lang/math/package-summary.html
 * </p>
 *
 * @author Al Cole
 * @version 1.0 Jan 2, 2019
 * @since 1.0
 */
public class NumUtl
{
    /**
     * Determines if the numeric value is odd.
     * @param aValue Numeric value to evaluate.
     * @return <i>true</i> if the value is odd and <i>false</i> otherwise.
     */
    public static boolean isOdd(int aValue)
    {
        return ((aValue %2) == 1);
    }

    /**
     * Determines if the numeric value is even.
     * @param aValue  Numeric value to evaluate.
     * @return <i>true</i> if the value is even and <i>false</i> otherwise.
     */
    public static boolean isEven(int aValue)
    {
        return ((aValue %2) == 0);
    }
}

