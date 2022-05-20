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

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * The WebUtl class provides utility features to applications that need
 * to construct web URIs.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2019
 * @since 1.0
 */
public class WebUtl
{
    public static String encodeValue(String aValue)
    {
        if (StringUtils.isEmpty(aValue))
            return StringUtils.EMPTY;

        int offset = aValue.indexOf('%');
        if (offset != -1)
            aValue = aValue.replace("%", "%25");
        offset = aValue.indexOf('|');
        if (offset != -1)
            aValue = aValue.replace("|", "%7C");
        offset = aValue.indexOf('~');
        if (offset != -1)
            aValue = aValue.replace("~", "%7E");
        offset = aValue.indexOf(';');
        if (offset != -1)
            aValue = aValue.replace(";", "%3B");
        offset = aValue.indexOf('/');
        if (offset != -1)
            aValue = aValue.replace("/", "%2F");
        offset = aValue.indexOf('?');
        if (offset != -1)
            aValue = aValue.replace("?", "%3F");
        offset = aValue.indexOf(':');
        if (offset != -1)
            aValue = aValue.replace(":", "%3A");
        offset = aValue.indexOf('&');
        if (offset != -1)
            aValue = aValue.replace("&", "%26");
        offset = aValue.indexOf('=');
        if (offset != -1)
            aValue = aValue.replace("=", "%3D");
        offset = aValue.indexOf('+');
        if (offset != -1)
            aValue = aValue.replace("+", "%2B");
        offset = aValue.indexOf('$');
        if (offset != -1)
            aValue = aValue.replace("$", "%24");
        offset = aValue.indexOf(',');
        if (offset != -1)
            aValue = aValue.replace(",", "%2C");
        offset = aValue.indexOf('#');
        if (offset != -1)
            aValue = aValue.replace("#", "%23");
        offset = aValue.indexOf('^');
        if (offset != -1)
            aValue = aValue.replace("^", "%5E");
        offset = aValue.indexOf('[');
        if (offset != -1)
            aValue = aValue.replace("[", "%5B");
        offset = aValue.indexOf(']');
        if (offset != -1)
            aValue = aValue.replace("]", "%5D");
        offset = aValue.indexOf('\"');
        if (offset != -1)
            aValue = aValue.replace("\"", "%22");
        offset = aValue.indexOf('\\');
        if (offset != -1)
            aValue = aValue.replace("\\", "%5C");
        offset = aValue.indexOf(' ');
        if (offset != -1)
            aValue = aValue.replace(" ", "+");

        return aValue;
    }

    /*
     * This class perform application/x-www-form-urlencoded-type encoding rather than
     * percent encoding, therefore replacing with + is a correct behaviour.
     */
    public static String urlEncodeValue(String aValue)
    {
        String encodedValue;

        try
        {
            encodedValue = URLEncoder.encode(aValue, StrUtl.CHARSET_UTF_8);
            int offset = encodedValue.indexOf(StrUtl.CHAR_PLUS);
            if (offset != -1)
                encodedValue = StringUtils.replace(encodedValue, "+", "%20");
        }
        catch (UnsupportedEncodingException e)
        {
            encodedValue = aValue;
        }

        return encodedValue;
    }

    public static String urlDecodeValue(String aValue)
    {
        String decodedValue;

        try
        {
            decodedValue = URLDecoder.decode(aValue, StrUtl.CHARSET_UTF_8);
        }
        catch (UnsupportedEncodingException e)
        {
            decodedValue = aValue;
        }

        return decodedValue;
    }

    public static String urlExtractFileName(String aURL)
    {
        if (StringUtils.isNotEmpty(aURL))
        {
            try
            {
                URL webURL = new URL(aURL);
                String fileName = webURL.getFile();
                if (fileName.indexOf('/') == -1)
                    return fileName;
            }
            catch (MalformedURLException ignored)
            {
            }

            int offset = aURL.lastIndexOf('/');
            if (offset != -1)
                return aURL.substring(offset+1);
        }

        return aURL;
    }
}
