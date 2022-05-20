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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

/**
 * The Platform class provides utility methods for platform-related operations.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2014
 * @since 1.0
 */
public class Platform
{
    public static final double FORMAT_SIZE_IN_KB = 1024;
    public static final double FORMAT_SIZE_IN_MB = 1024 * FORMAT_SIZE_IN_KB;
    public static final double FORMAT_SIZE_IN_GB = 1024 * FORMAT_SIZE_IN_MB;
    public static final double FORMAT_SIZE_IN_TB = 1024 * FORMAT_SIZE_IN_GB;

    public static final String PLATFORM_UNIX = "UNIX";
    public static final String PLATFORM_LINUX = "Linux";
    public static final String PLATFORM_MACOS = "Mac OS";
    public static final String PLATFORM_WINDOWS = "Windows";

    /**
     * Determines if the current platform that the JVM is executing within is
     * a Windows-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isWindows()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_WINDOWS);
    }

    /**
     * Determines if the current platform that the JVM is executing within is
     * a Mac-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isMac()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_MACOS);
    }

    /**
     * Determines if the current platform that the JVM is executing within is
     * a Linux-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isLinux()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_LINUX);
    }

    /**
     * Determines if the current platform that the JVM is executing within is
     * a UNIX-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isUNIX()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_UNIX);
    }

    /**
     * Convenience method that returns the host name of the current machine.
     *
     * @return Host name.
     */
    public static String getHostName()
    {
        String hostName;

        try
        {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostName = inetAddress.getHostName();
        }
        catch (UnknownHostException e)
        {
            hostName = "localhost";
        }

        return hostName;
    }

    /**
     * Convenience method that returns the host IP address of the current machine.
     *
     * @return Host IP address.
     */
    public static String getHostIPAddress()
    {
        String hostAddress;

        try
        {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostAddress = inetAddress.getHostAddress();
        }
        catch (UnknownHostException e)
        {
            hostAddress = "127.0.0.1";
        }

        return hostAddress;
    }

    /**
     * Convenience method that returns the full qualified domain name of the current machine.
     *
     * @return Fully Qualified Domain Name
     */
    public static String getFullyQualifiedDomainName()
    {
        String hostName;

        try
        {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostName = inetAddress.getCanonicalHostName();
        }
        catch (UnknownHostException e)
        {
            hostName = "localhost";
        }

        return hostName;
    }

    public static String bytesToString(long aSizeInBytes)
    {

        NumberFormat numberFormat = new DecimalFormat();
        numberFormat.setMaximumFractionDigits(2);
        try
        {
            if (aSizeInBytes < FORMAT_SIZE_IN_KB)
            {
                return numberFormat.format(aSizeInBytes) + " byte(s)";
            }
            else if (aSizeInBytes < FORMAT_SIZE_IN_MB)
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_KB) + " KB";
            }
            else if (aSizeInBytes < FORMAT_SIZE_IN_GB)
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_MB) + " MB";
            }
            else if (aSizeInBytes < FORMAT_SIZE_IN_TB)
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_GB) + " GB";
            }
            else
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_TB) + " TB";
            }
        }
        catch (Exception e)
        {
            return aSizeInBytes + " byte(s)";
        }
    }

    /**
     * Create a log message containing JVM Heap Memory statistics.
     * <p>totalMemory(): Returns the total amount of memory in the
     * Java virtual machine. The value returned by this method may
     * vary over time, depending on the host environment. Note that
     * the amount of memory required to hold an object of any given
     * type may be implementation-dependent.</p>
     * <p>maxMemory(): Returns the maximum amount of memory that the
     * Java virtual machine will attempt to use. If there is no inherent
     * limit then the value Long.MAX_VALUE will be returned.</p>
     * <p>freeMemory(): Returns the amount of free memory in the Java
     * Virtual Machine. Calling the gc method may result in increasing
     * the value returned by freeMemory.</p>
     * <p>In reference to your question, maxMemory() returns the -Xmx value.
     * You may be wondering why there is a totalMemory() AND a maxMemory().
     * The answer is that the JVM allocates memory lazily.</p>
     *
     * @param aTitle Title to save with log entry.
     *
     * @return Log message.
     *
     * @see <a href="http://stackoverflow.com/questions/3571203/what-is-the-exact-meaning-of-runtime-getruntime-totalmemory-and-freememory">Runtime Memory</a>
     * @see <a href="http://www.mkyong.com/java/find-out-your-java-heap-memory-size/">Heap Memory</a>
     * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html">JavaDoc Runtime</a>
     */
    public static String jvmLogMessage(String aTitle)
    {
        Runtime jvmRuntime = Runtime.getRuntime();

        if (StringUtils.isEmpty(aTitle))
            aTitle = "JVM";

        long maxMemory = jvmRuntime.maxMemory();
        long freeMemory = jvmRuntime.freeMemory();
        long totalMemory = jvmRuntime.totalMemory();
        long usedMemory = totalMemory - freeMemory;
        long availMemory = maxMemory - usedMemory;

        String logMsg = String.format("%s: Processors: %d, Mem Max: %s, Mem Total: %s, Mem Used: %s, Mem Avail: %s",
                                      aTitle, jvmRuntime.availableProcessors(),
                                      bytesToString(maxMemory),
                                      bytesToString(totalMemory),
                                      bytesToString(usedMemory),
                                      bytesToString(availMemory));
        return logMsg;
    }
}
