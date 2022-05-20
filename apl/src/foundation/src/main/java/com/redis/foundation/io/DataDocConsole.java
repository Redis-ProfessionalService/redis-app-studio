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

package com.redis.foundation.io;

import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * The DataDocConsole class provides a convenient way for an application
 * to capture data input from the console.  The presentation of the items
 * are aligned based on title name widths.  Special handling logic exists to
 * handle password presentations.
 * <p>
 * <b>Note:</b> Only values that are visible will be processed by this class.
 * </p>
 *
 * @since 1.0
 * @author Al Cole
 */
public class DataDocConsole
{
    private DataDoc mDataDoc;
    private boolean mIsFormatted;
    private boolean mIsBasedOnTitle = true;

    /**
     * Constructor that identifies a bag prior to an edit operation.
     *
     * @param aDoc Data document of items.
     */
    public DataDocConsole(DataDoc aDoc)
    {
        mDataDoc = aDoc;
    }

    /**
     * By default, the logic will use the title as a prompt string.
     * You can change this default behaviour to use the item name.
     *
     * @param aTitleFlag If false, then the item name will be
     * used as a prompt string.
     */
    public void setUseTitleFlag(boolean aTitleFlag)
    {
        mIsBasedOnTitle = aTitleFlag;
    }

    /**
     * Assign the value formatting flag.  If <i>true</i>, then numbers
     * and dates will be generated based on the format mask.
     *
     * @param aIsFormatted True or false
     */
    public void setFormattedFlag(boolean aIsFormatted)
    {
        mIsFormatted = aIsFormatted;
    }

    private void editDoc(String aTitle)
    {
        StringBuilder stringBuilder;
        String itemTitle, itemValue, promptString, inputString;

        int maxPromptLength = 0;
        for (DataItem dataItem : mDataDoc.getItems())
        {
            if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
            {
                if (mIsBasedOnTitle)
                    itemTitle = dataItem.getTitle();
                else
                    itemTitle = dataItem.getName();
                itemValue = dataItem.getValue();
                if (StringUtils.isEmpty(itemValue))
                    promptString = itemTitle;
                else
                    promptString = String.format("%s [%s]", itemTitle, itemValue);
                maxPromptLength = Math.max(maxPromptLength, promptString.length());
            }
        }

        if (StringUtils.isNotEmpty(aTitle))
        {
            stringBuilder = new StringBuilder();
            for (int j = aTitle.length(); j < maxPromptLength; j++)
                stringBuilder.append(StrUtl.CHAR_SPACE);

            System.out.printf("%n%s%s%n%n", stringBuilder.toString(), aTitle);
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        for (DataItem dataItem : mDataDoc.getItems())
        {
            if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
            {
                if (mIsBasedOnTitle)
                    itemTitle = dataItem.getTitle();
                else
                    itemTitle = dataItem.getName();
                if (mIsFormatted)
                    itemValue = dataItem.getValueFormatted();
                else
                    itemValue = dataItem.getValue();
                if (StringUtils.isEmpty(itemValue))
                    promptString = itemTitle;
                else
                    promptString = String.format("%s [%s]", itemTitle, itemValue);

                stringBuilder = new StringBuilder();
                for (int j = promptString.length(); j < maxPromptLength; j++)
                    stringBuilder.append(StrUtl.CHAR_SPACE);

                System.out.printf("%s%s: ", stringBuilder.toString(), promptString);

                try
                {
                    inputString = stdin.readLine();
                }
                catch (IOException e)
                {
                    inputString = StringUtils.EMPTY;
                }

                if (StringUtils.isNotEmpty(inputString))
                    dataItem.setValue(inputString);
            }
        }
    }

    /**
     * Prompts the user from the console for the bag values.
     * <p>
     * <b>Note:</b> This method can only support single value
     * items.
     * </p>
     *
     * @param aTitle A title string for the presentation.
     */
    public void edit(String aTitle)
    {
        editDoc(aTitle);
    }

    /**
     * Prompts the user from the console for the bag values.
     * <p>
     * <b>Note:</b> This method can only support single value
     * items.
     * </p>
     */
    public void edit()
    {
        edit(StringUtils.EMPTY);
    }

    /**
     * Write data document name and values to the console.
     * <p>
     * <b>Note:</b> This method can only support single value
     * items.
     * </p>
     *
     * @param aPW PrintWriter instance.
     *
     * @param aTitle A title string for the presentation.
     */
    public void writeDoc(PrintWriter aPW, String aTitle)
    {
        StringBuilder stringBuilder;
        String itemTitle, itemValue, titleString;

        int maxTitleLength = 0;
        for (DataItem dataItem : mDataDoc.getItems())
        {
            if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
            {
                if (mIsBasedOnTitle)
                    itemTitle = dataItem.getTitle();
                else
                    itemTitle = dataItem.getName();
                maxTitleLength = Math.max(maxTitleLength, itemTitle.length());
            }
        }

        if (StringUtils.isNotEmpty(aTitle))
        {
            stringBuilder = new StringBuilder();
            for (int j = aTitle.length(); j < maxTitleLength; j++)
                stringBuilder.append(StrUtl.CHAR_SPACE);

            aPW.printf("%n%s%s%n%n", stringBuilder.toString(), aTitle);
        }

        for (DataItem dataItem : mDataDoc.getItems())
        {
            if (dataItem.isFeatureTrue(Data.FEATURE_IS_VISIBLE))
            {
                if (mIsBasedOnTitle)
                    itemTitle = dataItem.getTitle();
                else
                    itemTitle = dataItem.getName();
                if (dataItem.isMultiValue())
                    itemValue = StrUtl.collapseToSingle(dataItem.getValues(), StrUtl.CHAR_COMMA);
                else if (mIsFormatted)
                    itemValue = dataItem.getValueFormatted();
                else
                    itemValue = dataItem.getValue();
                stringBuilder = new StringBuilder();
                for (int j = itemTitle.length(); j < maxTitleLength; j++)
                    stringBuilder.append(StrUtl.CHAR_SPACE);

                aPW.printf("%s%s: %s%n", stringBuilder.toString(), itemTitle, itemValue);

            }
        }
    }

    /**
     * Write bag name and values to the console.
     * <p>
     * <b>Note:</b> This method can only support single value
     * items.
     * </p>
     *
     * @param aTitle A title string for the presentation.
     */
    public void write(String aTitle)
    {
        PrintWriter printWriter = new PrintWriter(System.out);
        writeDoc(printWriter, aTitle);
    }

    /**
     * Write bag name and values to the console.
     * <p>
     * <b>Note:</b> This method can only support single value
     * items.
     * </p>
     */
    public void write()
    {
        write(StringUtils.EMPTY);
    }
}
