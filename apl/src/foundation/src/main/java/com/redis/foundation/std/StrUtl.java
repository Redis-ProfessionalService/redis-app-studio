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

import com.redis.foundation.data.Data;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;

/**
 * The StrUtl class provides utility methods for manipulating and
 * evaluating <i>String</i> objects. The goal of this class is to
 * centralize commonly used string manipulation methods and to
 * encourage code re-use.
 * <p>
 * The Apache Commons has a number of good utility methods for string
 * values.
 * http://commons.apache.org/proper/commons-lang/index.html
 * </p>
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("UnusedDeclaration")
public class StrUtl
{

// The following constants are defined to aid in code documentation.

    public static final char CHAR_AT = '@';
    public static final char CHAR_DOT = '.';
    public static final char CHAR_NULL = '\0';
    public static final char CHAR_COLON = ':';
    public static final char CHAR_PIPE = '|';
    public static final char CHAR_PLUS = '+';
    public static final char CHAR_SPACE = ' ';
    public static final char CHAR_COMMA = ',';
    public static final char CHAR_EQUAL = '=';
    public static final char CHAR_POUND = '#';
    public static final char CHAR_HYPHEN = '-';
    public static final char CHAR_DOLLAR = '$';
    public static final char CHAR_PERCENT = '%';
    public static final char CHAR_LESSTHAN = '<';
    public static final char CHAR_EXCLAMATION = '!';
    public static final char CHAR_GREATERTHAN = '>';
    public static final char CHAR_AMPERSAND = '&';
    public static final char CHAR_QUESTMARK = '?';
    public static final char CHAR_UNDERLINE = '_';
    public static final char CHAR_SGLQUOTE = '\'';
    public static final char CHAR_DBLQUOTE = '"';
    public static final char CHAR_ASTERISK = '*';
    public static final char CHAR_SEMICOLON = ';';
    public static final char CHAR_BACKSLASH = '\\';
    public static final char CHAR_FORWARDSLASH = '/';
    public static final char CHAR_PAREN_OPEN = '(';
    public static final char CHAR_PAREN_CLOSE = ')';
    public static final char CHAR_BRACKET_OPEN = '[';
    public static final char CHAR_BRACKET_CLOSE = ']';
    public static final char CHAR_CURLYBRACE_OPEN = '{';
    public static final char CHAR_CURLYBRACE_CLOSE = '}';

    public static final String STRING_EQUAL = "=";
    public static final String STRING_HYPHEN = "-";
    public static final String STRING_AMPERSAND = "&";
    public static final String STRING_LESSTHAN = "<";
    public static final String STRING_GREATTHAN = ">";
    public static final String STRING_SGLQUOTE = "'";
    public static final String STRING_DBLQUOTE = "\"";
    public static final String STRING_NO = "no";
    public static final String STRING_YES = "yes";
    public static final String STRING_TRUE = "true";
    public static final String STRING_FALSE = "false";
    public static final String STRING_PAREN_OPEN = "[";
    public static final String STRING_PAREN_CLOSE = "]";
    public static final String STRING_BRACKET_OPEN = "[";
    public static final String STRING_BRACKET_CLOSE = "]";

//http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html

    public static final String CHARSET_UTF_8 = "UTF-8";
    public static final String CHARSET_UTF_16 = "UTF-16";
    public static final String CHARSET_US_ASCII = "US-ASCII";
    public static final String CHARSET_ISO_8859_1 = "ISO-8859-1";

// Password related constants.

    public static final int STRUTL_PASSWORD_SIZE = 24;
    private static final int STRUTL_ROT13PW_SIZE = 25;     // Net 24 characters
    private static final char STRUTL_CHAR_PWBEGIN = '<';
    private static final char STRUTL_CHAR_PWFINISH = '>';

// Size in bytes to human readable format constants

    public static final double SIZE_IN_KB = 1024;
    public static final double SIZE_IN_MB = 1024 * SIZE_IN_KB;
    public static final double SIZE_IN_GB = 1024 * SIZE_IN_MB;
    public static final double SIZE_IN_TB = 1024 * SIZE_IN_GB;

    /**
     * Given a lowercase name, this method will make it a proper
     * name by capitalizing the first character.
     *
     * @param aName Lowercase name.
     *
     * @return Proper name.
     */
    public static String firstCharToUpper(String aName)
    {
        String properName = StringUtils.EMPTY;

        if (StringUtils.isNotEmpty(aName))
            properName = aName.substring(0, 1).toUpperCase() + aName.substring(1);

        return properName;
    }

    /**
     * Given a <i>boolean</i> value, return a string object representation of
     * its value.
     *
     * @param aFlag A boolean flag value.
     * @return <code>StrUtl.STRING_TRUE</code> or <code>StrUtl.STRING_FALSE</code>
     */
    public static String booleanToString(boolean aFlag)
    {
        if (aFlag)
            return StrUtl.STRING_TRUE;
        else
            return StrUtl.STRING_FALSE;
    }

    /**
     * Given a <i>String</i> object representing a boolean value of
     * <code>StrUtl.STRING_TRUE</code> or <code>StrUtl.STRING_FALSE</code>
     * it will return <i>true</i> or <i>false</i>.
     *
     * @param aString A non-empty string value.
     * @return <i>true</i> or <i>false</i> depending on <i>String</i>
     * value.
     */
    public static boolean stringToBoolean(String aString)
    {
        if (StringUtils.isNotEmpty(aString))
        {
            if ((aString.equalsIgnoreCase(StrUtl.STRING_YES)) ||
                (aString.equalsIgnoreCase(StrUtl.STRING_TRUE)) ||
                (aString.equals("1")))
                return true;
        }
        return false;
    }

    /**
     * Remove all references to a character from a string.
     *
     * @param aString A source string.
     * @param aChar Identifies the character to remove.
     * @return A newly constructed  <i>String</i> object with all
     * the characters removed.
     */
    public static String removeAllChar(String aString, char aChar)
    {
        char ch;
        int strLength;
        StringBuilder strBuilder;

        if (StringUtils.isEmpty(aString))
            return aString;
        else
        {
            strLength = aString.length();
            strBuilder = new StringBuilder();
            for (int i = 0; i < strLength; i++)
            {
                ch = aString.charAt(i);
                if (ch != aChar)
                    strBuilder.append(ch);
            }

            return strBuilder.toString();
        }
    }

    /**
     * Removes a duplicate character (meaning it repeats two more more times)
     * from the string.
     *
     * @param aString A source string.
     * @param aChar A repeating character.
     *
     * @return Update string.
     */
    public static String removeDuplicateChar(String aString, char aChar)
    {
        int strLength;
        char prevChar, nextChar;
        StringBuilder strBuilder;

        if (StringUtils.isEmpty(aString))
            return aString;
        else
        {
            prevChar = 0;
            strLength = aString.length();
            strBuilder = new StringBuilder();
            for (int i = 0; i < strLength; i++)
            {
                nextChar = aString.charAt(i);
                if (nextChar == aChar)
                {
                    if (nextChar != prevChar)
                        strBuilder.append(nextChar);
                }
                else
                    strBuilder.append(nextChar);
                prevChar = nextChar;
            }

            return strBuilder.toString();
        }
    }

    /**
     * Trims the last character from the string.
     *
     * @param aString A source string.
     *
     * @return Trimmed string.
     */
    public static String trimLastChar(String aString)
    {
        char ch;
        int strLength;
        StringBuilder strBuilder;

        if (StringUtils.isEmpty(aString))
            return aString;
        else
        {
            strLength = aString.length();
            return aString.substring(0, strLength-1);
        }
    }

    /**
     * Replaces all references to a character with a string.
     *
     * @param aSource A source string.
     * @param aChar Identifies the character to replace.
     * @param aReplacement A replacement string for the matching character.
     * @return A newly constructed  <i>String</i> object with all
     * the characters replaced.
     */
    public static String replaceCharWithString(String aSource, char aChar, String aReplacement)
    {
        char ch;
        int strLength;
        StringBuilder strBuilder;

        if (StringUtils.isEmpty(aSource))
            return aSource;
        else
        {
            strLength = aSource.length();
            strBuilder = new StringBuilder();
            for (int i = 0; i < strLength; i++)
            {
                ch = aSource.charAt(i);
                if (ch == aChar)
                    strBuilder.append(aReplacement);
                else
                    strBuilder.append(ch);
            }

            return strBuilder.toString();
        }
    }

    /**
     * Escapes the target character with the escape character.
     *
     * @param aSource Source string
     * @param aTargetChar Target character
     * @param anEscapeChar Escape character
     *
     * @return Escaped string
     */
    public static String escapeChar(String aSource, char aTargetChar, char anEscapeChar)
    {
        char ch;
        int strLength;
        StringBuilder strBuilder;

        if (StringUtils.isEmpty(aSource))
            return aSource;
        else
        {
            strLength = aSource.length();
            strBuilder = new StringBuilder();
            for (int i = 0; i < strLength; i++)
            {
                ch = aSource.charAt(i);
                if (ch == aTargetChar)
                    strBuilder.append(anEscapeChar);
                strBuilder.append(ch);
            }

            return strBuilder.toString();
        }
    }

    /**
     * Given a string and a desired width (think console presentation), this
     * method will pad the string with the appropriate number of space characters.
     *
     * @param aString A non-empty string.
     * @param aWidth The width of the presentation console (e.g. 80 characters).
     * @return A newly constructed  <i>String</i> object with the original
     * text centered for the <i>aWidth</i> value.
     */
    public static String centerSpaces(String aString, int aWidth)
    {
        StringBuilder strBuilder;
        int strLength, middleOffset;

        if (StringUtils.isEmpty(aString))
            return aString;
        else
        {
            strLength = aString.length();
            middleOffset = (aWidth / 2) - (strLength / 2);

            if ((strLength > 0) && (middleOffset > 0) && (strLength < aWidth))
            {
                strBuilder = new StringBuilder();
                for (int i = 0; i < middleOffset; i++)
                    strBuilder.append(CHAR_SPACE);
                strBuilder.append(aString);
            }
            else
                strBuilder = new StringBuilder(aString);

            return strBuilder.toString();
        }
    }

    /**
     * Given a string and a starting offset position (within the string),
     * this method will identify the position (character offset) where
     * the last word in the string can be found.
     *
     * @param aString A non-empty string of words.
     * @param aPosition Starting offset to evaluate from.
     * @return The offset where the last word can be found or the value of
     * <i>-1</i> to signify that there were no space seperated words left in the
     * string.
     */
    public static int lastWordIndex(String aString, int aPosition)
    {
        int i;
        boolean isFound;

        if (StringUtils.isEmpty(aString))
            return -1;
        else
        {
            isFound = false;
            i = Math.min(aString.length(), aPosition) - 1;

            while ((! isFound) && (i >= 0))
            {
                if (aString.charAt(i) == CHAR_SPACE)
                    isFound = true;
                else
                    i--;
            }

            if (isFound)
                return i;
            else
                return -1;
        }
    }

    /**
     * Given a string with a paragraph of words, a left margin pad size,
     * the width of the console or report page and an output stream, this
     * method will break the string into a formatted paragraph.
     *
     * @param aString A collection of words.
     * @param aLeftMargin Each line will be padded with this many spaces.
     * @param aLineWidth Identifies where characters should be wrapped.
     * @param aStream An output stream where each line will be written to.
     */
    public static void wrapToStream(String aString, int aLeftMargin, int aLineWidth,
                                    PrintStream aStream)
    {
        String subStr;
        boolean isDone;
        StringBuilder strBuilder;
        int strPos, wrapPos, startPos, strLength;

        if ((StringUtils.isEmpty(aString)) || (aStream == null))
            return;

        strLength = aString.length();
        aLineWidth = Math.max(10, aLineWidth);
        aLeftMargin = Math.max(0, aLeftMargin);

        strPos = 0;
        isDone = false;

        while ((! isDone) && (strPos < strLength))
        {
            subStr = aString.substring(strPos);

            strBuilder = new StringBuilder();
            for (int i = 0; i < aLeftMargin; i++)
                strBuilder.append(CHAR_SPACE);

            startPos = 0;
            while (subStr.charAt(startPos) == CHAR_SPACE)
                startPos++;

            strBuilder.append(subStr.substring(startPos));

            if (strBuilder.length() < aLineWidth)
            {
                aStream.printf("%s%n", strBuilder.toString());
                isDone = true;
            }
            else
            {
                wrapPos = lastWordIndex(strBuilder.toString(), aLineWidth);
                if (wrapPos == -1)
                    break;          // exception condition
                else
                {
                    aStream.printf("%s%n", strBuilder.substring(0, wrapPos));
                    strPos += wrapPos;
                }
            }
        }
    }

    /**
     * Uses a simple Caesar-cypher encryption to replace each English letter
     * with the one 13 places forward or back along the alphabet, so that
     * "The butler did it!" becomes "Gur ohgyre qvq vg!".   major advantage
     * of rot13 over rot(N) for other N is that it is self-inverse, so the
     * same code can be used for encoding and decoding.
     *
     * @param aString A plain text string to encode.
     * @return An encrypted <i>String</i> object.
     */
    public static String simple13Rotation(String aString)
    {
        char ch, chUp;
        int strLength;
        StringBuilder strBuilder;

        if (StringUtils.isEmpty(aString))
            return aString;
        else
        {
            strBuilder = new StringBuilder();

            strLength = aString.length();
            for (int i = 0; i < strLength; i++)
            {
                ch = aString.charAt(i);
                if (Character.isLetter(ch))
                {
                    chUp = Character.toUpperCase(ch);
                    if ((chUp >= 'A') && (chUp <= 'M'))
                        ch += 13;
                    else
                        ch -= 13;
                }
                strBuilder.append(ch);
            }

            return strBuilder.toString();
        }
    }

    /**
     * Uses a simple Caesar-cypher encryption to replace each English letter
     * with the one 13 places forward or back along the alphabet, so that
     * "The butler did it!" becomes "Gur ohgyre qvq vg!".   major advantage
     * of rot13 over rot(N) for other N is that it is self-inverse, so the
     * same code can be used for encoding and decoding.
     * <p>
     * <i>Note: The returned string will be wrapped with the less-than and
     * greater-than characters to signify that they were encrypted by this method.
     * These wrappers will be expected when the string is recovered.</i>
     * </p>
     *
     * @param aStringPlain A plain text string to encode.
     * @return An encrypted <i>String</i> object.
     */
    public static String hidePassword(String aStringPlain)
    {
        char ch;
        boolean isAllUpper;
        StringBuilder strBuilder;
        int strLength, someNumber;

        if (StringUtils.isEmpty(aStringPlain))
            return aStringPlain;
        else
        {
            strBuilder = new StringBuilder();
            strLength = aStringPlain.length();
            if (strLength > STRUTL_ROT13PW_SIZE)
                strLength = STRUTL_ROT13PW_SIZE - 1;

            isAllUpper = true;

            for (int i = 0; i < strLength; i++)
            {
                ch = aStringPlain.charAt(i);
                if (Character.isLowerCase(ch))
                {
                    isAllUpper = false;
                    break;
                }
            }

            strBuilder.append(STRUTL_CHAR_PWBEGIN);

            if (isAllUpper)
                ch = 'A';
            else
                ch = 'a';
            ch += strLength;
            strBuilder.append(ch);

            if (strLength > STRUTL_ROT13PW_SIZE)
                strBuilder.append(aStringPlain, 0, STRUTL_ROT13PW_SIZE);
            else
                strBuilder.append(aStringPlain);

            someNumber = 2;
            for (int i = strLength; i < STRUTL_ROT13PW_SIZE; i++)
            {
                if (isAllUpper)
                    ch = 'A';
                else
                    ch = 'a';
                ch += i;
                if ((NumUtl.isEven(i)) && (! isAllUpper))
                    ch = Character.toLowerCase(ch);
                else if ((i % 3) == 0)
                {
                    ch = '1';
                    ch += someNumber;
                    someNumber++;
                }
                strBuilder.append(ch);
            }

            strBuilder.append(STRUTL_CHAR_PWFINISH);

            return simple13Rotation(strBuilder.toString());
        }
    }

    /**
     * Determines if the string has been encrypted to hide is contents.
     *
     * @param aString A simple string value.
     *
     * @return <i>true</i> if hidden (encrypted) and <i>false</i> otherwise.
     */
    public static boolean isHidden(String aString)
    {
        if (StringUtils.isNotEmpty(aString))
        {
            int strLength = aString.length();
            if (strLength > 1)
            {
                char chStart = aString.charAt(0);
                char chEnd = aString.charAt(strLength-1);
                if ((chStart == STRUTL_CHAR_PWBEGIN) && (chEnd == STRUTL_CHAR_PWFINISH))
                    return true;
            }
        }
        return false;
    }

    /**
     * Recovers a previously encrypted <code>hidePassword</code> string
     * and returns it in its original form.
     * <p>
     * <b>Note:</b> The input string must be wrapped with the less-than and
     * greater-than characters to signify that they were previously
     * encrypted by the <code>hidePassword</code> method.
     * </p>
     *
     * @param aStringPassword An encrypted password string.
     *
     * @return A decrypted <i>String</i> object.
     */
    public static String recoverPassword(String aStringPassword)
    {
        char chStart, chEnd;
        String plainPassword;
        int strLength, pwSize;

        if (StringUtils.isEmpty(aStringPassword))
            return aStringPassword;
        else
        {
            strLength = aStringPassword.length();
            if (strLength > 1)
            {
                chStart = aStringPassword.charAt(0);
                chEnd = aStringPassword.charAt(strLength-1);
                if ((chStart != STRUTL_CHAR_PWBEGIN) && (chEnd != STRUTL_CHAR_PWFINISH))
                    return aStringPassword;
            }
            else
                return aStringPassword;

            plainPassword = simple13Rotation(aStringPassword.substring(1));
            strLength = plainPassword.length();
            plainPassword = plainPassword.substring(0, strLength-1);

            chStart = Character.toUpperCase(plainPassword.charAt(0));
            pwSize = (chStart - 'A') + 1;
            if ((pwSize <= 0) || (pwSize > STRUTL_ROT13PW_SIZE))
                return aStringPassword;

            return plainPassword.substring(1, pwSize);
        }
    }

    /**
     * Determines if the last character in the string matches.
     *
     * @param aString A source string.
     * @param aChar Identifies the character to match.
     *
     * @return <code>true</code> if character exists or <code>false</code>
     */
    public static boolean endsWithChar(String aString, char aChar)
    {
        if (StringUtils.isEmpty(aString))
            return false;
        else
        {
            int strLength = aString.length();
            return (aString.charAt(strLength-1) == aChar);
        }
    }

    /**
     * Appends the character to the string if it is missing.
     *
     * @param aString A source string.
     * @param aChar Identifies the character to append (if missing).
     *
     * @return The updated string if the character was missing.
     */
    public static String appendCharIfMissing(String aString, char aChar)
    {
        if (StringUtils.isNotEmpty(aString))
        {
            if (! endsWithChar(aString, aChar))
                return aString + aChar;
        }

        return aString;
    }

    /**
     * Escape <code>aCharToEscape</code> in the string
     * with a Java escape character (backslash).
     *
     * @param aString String to process.
     * @param aCharToEscape The character to be escaped.
     *
     * @return An escaped string
     */
    public static String escapeStringWithBackslash(String aString, char aCharToEscape)
    {
        char curChar;

        if (StringUtils.isEmpty(aString))
            return StringUtils.EMPTY;

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < aString.length(); i++)
        {
            curChar = aString.charAt(i);

            if (curChar == aCharToEscape)
                stringBuilder.append(CHAR_BACKSLASH);

            stringBuilder.append(curChar);
        }

        return stringBuilder.toString();
    }

    /**
     * Unescape <code>aCharToEscape</code> in the string recognizing
     * a Java escape character (backslash).
     *
     * @param aString String to process.
     * @param aCharToEscape The character to be escaped.
     *
     * @return An escaped string
     */
    public static String unEscapeStringWithBackslash(String aString, char aCharToEscape)
    {
        char curChar, nextChar;

        if (StringUtils.isEmpty(aString))
            return StringUtils.EMPTY;

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < aString.length(); i++)
        {
            curChar = aString.charAt(i);
            if ((i + 1) < aString.length())
            {
                nextChar = aString.charAt(i + 1);
                if ((curChar == CHAR_BACKSLASH) && (nextChar == aCharToEscape))
                    continue;
            }
            stringBuilder.append(curChar);
        }

        return stringBuilder.toString();
    }

    /**
     * Detects if the delimiter character (used to delineate multiple values)
     * exists in the string.
     *
     * @param aString String to examine
     * @param aDelimiterChar Delimiter character.
     *
     * @return <i>true</i> if match count greater-than zero and <i>false</i> otherwise
     */
    public static boolean isMultiValue(String aString, char aDelimiterChar)
    {
        return StringUtils.countMatches(aString, aDelimiterChar) > 0;
    }

    /**
     * Collapses the list of strings down to one string.
     * Each value is separated by a delimiter character.
     *
     * @param aStrings List of strings to process.
     * @param aDelimiterChar Delimiter character.
     *
     * @return A collapsed string.
     */
    public static String collapseToSingle(final ArrayList<String> aStrings, char aDelimiterChar)
    {
        StringBuilder stringBuilder = new StringBuilder();

        if ((aStrings != null) && (aStrings.size() > 0))
        {
            for (String strValue : aStrings)
            {
                if (stringBuilder.length() == 0)
                    stringBuilder.append(escapeStringWithBackslash(strValue, aDelimiterChar));
                else
                {
                    stringBuilder.append(aDelimiterChar);
                    stringBuilder.append(escapeStringWithBackslash(strValue, aDelimiterChar));
                }
            }
        }

        if (stringBuilder.length() == 0)
            return StringUtils.EMPTY;
        else
            return stringBuilder.toString();
    }

    /**
     * Expand the string into a list of individual values
     * using the delimiter character to identify each one.
     *
     * @param aString One or more values separated by a
     *                delimiter character.
     * @param aDelimiterChar Delimiter character.
     *
     * @return An ArrayList of String instances.
     */
    public static ArrayList<String> expandToList(final String aString, char aDelimiterChar)
    {
        ArrayList<String> stringList = new ArrayList<String>();

        if (StringUtils.isNotEmpty(aString))
        {
            String delimiterString = Character.toString(aDelimiterChar);
            if (aDelimiterChar == StrUtl.CHAR_PIPE)
                delimiterString = String.format("%c%c", StrUtl.CHAR_BACKSLASH, aDelimiterChar);
            String regExPattern = String.format("(?<!\\\\)%s", delimiterString);

            String[] valueArray = aString.split(regExPattern);
            for (String strValue : valueArray)
                stringList.add(unEscapeStringWithBackslash(strValue, aDelimiterChar));
        }

        return stringList;
    }

    /**
     * Convenience method that converts a generic ArrayList of
     * Objects into an array of string values.
     *
     * @param anArrayList Array of value object instances.
     *
     * @return An array of values extracted from the array list.
     */
    public static String[] convertToMulti(ArrayList<?> anArrayList)
    {
        if ((anArrayList == null) || (anArrayList.size() == 0))
        {
            String[] emptyList = new String[1];
            emptyList[0] = StringUtils.EMPTY;
            return emptyList;
        }

        int offset = 0;
        String[] multiValues = new String[anArrayList.size()];

        for (Object arrayObject : anArrayList)
        {
            if (arrayObject instanceof Integer)
            {
                Integer integerValue = (Integer) arrayObject;
                multiValues[offset++] = integerValue.toString();
            }
            else if (arrayObject instanceof Long)
            {
                Long longValue = (Long) arrayObject;
                multiValues[offset++] = longValue.toString();
            }
            else if (arrayObject instanceof Float)
            {
                Float floatValue = (Float) arrayObject;
                multiValues[offset++] = floatValue.toString();
            }
            else if (arrayObject instanceof Double)
            {
                Double doubleValue = (Double) arrayObject;
                multiValues[offset++] = doubleValue.toString();
            }
            else if (arrayObject instanceof Date)
            {
                Date dateValue = (Date) arrayObject;
                multiValues[offset++] = Data.dateValueFormatted(dateValue, Data.FORMAT_DATETIME_DEFAULT);
            }
            else
                multiValues[offset++] = arrayObject.toString();
        }

        return multiValues;
    }

    /**
     * Determines if the patten (with one or more wildcards) matches
     * the string.
     *
     * @param aString String to evaluate.
     * @param aPattern Wildcard pattern.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public static boolean isWildcardMatch(String aString, String aPattern)
    {
        if ((StringUtils.isEmpty(aString)) || ((StringUtils.isEmpty(aPattern))))
            return false;
        else
        {
            int offset = aPattern.indexOf(CHAR_ASTERISK);
            if (offset == -1)
                return StringUtils.equals(aString, aPattern);
            else
            {
                String subString = aString;
                String[] patternStrings = aPattern.split("\\*");
                for (String patternString : patternStrings)
                {
                    offset = subString.indexOf(patternString);
                    if (offset == -1)
                        return false;
                    else
                        subString = subString.substring(offset + patternString.length());
                }
                return true;
            }
        }
    }

    /**
     * Formats the size (in bytes) in a human readable string of
     * bytes, KB, MB, GB and TB.
     *
     * @param aSizeInBytes Size in bytes.
     *
     * @return Formatted string.
     */
    public static String bytesToString(long aSizeInBytes)
    {
        NumberFormat numberFormat = new DecimalFormat();
        numberFormat.setMaximumFractionDigits(2);

        try
        {
            if (aSizeInBytes < SIZE_IN_KB)
                return numberFormat.format(aSizeInBytes) + " Byte(s)";
            else if (aSizeInBytes < SIZE_IN_MB)
                return numberFormat.format(aSizeInBytes/ SIZE_IN_KB) + " KB";
            else if (aSizeInBytes < SIZE_IN_GB)
                return numberFormat.format(aSizeInBytes/ SIZE_IN_MB) + " MB";
            else if (aSizeInBytes < SIZE_IN_TB)
                return numberFormat.format(aSizeInBytes/ SIZE_IN_GB) + " GB";
            else
                return numberFormat.format(aSizeInBytes/ SIZE_IN_TB) + " TB";
        }
        catch (Exception e)
        {
            return aSizeInBytes + " Byte(s)";
        }
    }
}

