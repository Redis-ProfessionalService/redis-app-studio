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

import org.apache.commons.lang3.time.DateUtils;

/**
 * The Sleep class provides utility methods for pausing program
 * execution.
 * <p>
 * The Apache Commons has a number of good utility methods for this.
 *
 * http://commons.apache.org/proper/commons-lang/javadocs/api-release/index.html
 * </p>
 * @author Al Cole
 * @version 1.0 Jan 2, 2014
 * @since 1.0
 */
public class Sleep
{
    public static void forMilliseconds(long aMilliseconds)
    {
        try { Thread.sleep(aMilliseconds); } catch (InterruptedException ignored) {}
    }

    public static void forSeconds(int aSeconds)
    {
        forMilliseconds(aSeconds * DateUtils.MILLIS_PER_SECOND);
    }

    public static void forMinutes(int aMinutes)
    {
        forMilliseconds(aMinutes * DateUtils.MILLIS_PER_MINUTE);
    }

    public static void forHours(int aHours)
    {
        forMilliseconds(aHours * DateUtils.MILLIS_PER_HOUR);
    }

    public static void forDays(int aDays)
    {
        forMilliseconds(aDays * DateUtils.MILLIS_PER_DAY);
    }
}
