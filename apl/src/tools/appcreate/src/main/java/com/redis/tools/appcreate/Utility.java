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

package com.redis.tools.appcreate;

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.*;

public class Utility
{
    private AppCtx mAppCtx;

    public Utility(AppCtx anAppCtx)
    {
        mAppCtx = anAppCtx;
    }

    public boolean makeFolders(String aPathName)
    {
        boolean wasCreated = true;
        Logger appLogger = mAppCtx.getLogger(this, "makeFolders");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        File pathFile = new File(aPathName);
        if (pathFile.mkdirs())
        {
            String msgStr = String.format("Created folder '%s'", aPathName);
            appLogger.info(msgStr);
            System.out.println(msgStr);
        }
        else
        {
            if (! pathFile.exists())
            {
                String errMsg = String.format("Folder '%s' cannot be created.", aPathName);
                appLogger.error(errMsg);
                wasCreated = false;
            }
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        return wasCreated;
    }

    public void docFieldReplaceInTemplate(DataDoc aDoc, String aTmpPathFileName, String aDstPathFileName)
        throws IOException
    {
        String variableName;
        Logger appLogger = mAppCtx.getLogger(this, "docFieldReplaceInTemplate");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        File dstFile = new File(aDstPathFileName);
        if (dstFile.exists())
        {
            String msgStr = String.format("File '%s' exists - skipping.", aDstPathFileName);
            appLogger.info(msgStr);
            System.out.println(msgStr);
        }
        else
        {
            FileReader fileReader = new FileReader(aTmpPathFileName);
            PrintWriter printWriter = new PrintWriter(aDstPathFileName);

            BufferedReader brLine = new BufferedReader(fileReader);
            String inLine = brLine.readLine();
            while (inLine != null)
            {
                if (StringUtils.isNotEmpty(inLine))
                {
                    for (DataItem dataItem : aDoc.getItems())
                    {
                        variableName = String.format("${%s}", dataItem.getName());
                        if (StringUtils.contains(inLine, variableName))
                            inLine = StringUtils.replace(inLine, variableName, dataItem.getValue());
                    }
                }
                printWriter.println(inLine);
                inLine = brLine.readLine();
            }
            fileReader.close();
            printWriter.close();

            String msgStr = String.format("Populated file '%s'.", aDstPathFileName);
            appLogger.info(msgStr);
            System.out.println(msgStr);
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }
}
