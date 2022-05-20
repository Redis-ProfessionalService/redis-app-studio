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
import com.redis.foundation.app.Task;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.Sleep;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

@SuppressWarnings({"FieldCanBeLocal"})
public class CreateConsoleTask implements Task
{
    private final String mRunName = "console";
    private final String mTestName = "console";

    private AppCtx mAppCtx;
    private boolean mIsAlive;
    private String mMavenTitle;
    private String mPkgPathName;
    private String mMavenGroupId;
    private String mMavenArtifactId;
    private String mJavaSrcPathName;
    private String mTestSrcPathName;
    private String mPkgRootPathName;
    private String mResourcePathName;
    private String mTemplatePathName;
    private String mRootSourcePathName;

    @Override
    public String getRunName()
    {
        return mRunName;
    }

    @Override
    public String getTestName()
    {
        return mTestName;
    }

    @Override
    public boolean isAlive()
    {
        return mIsAlive;
    }

    @Override
    public void init(AppCtx anAppMgr)
        throws FCException
    {
        mAppCtx = anAppMgr;
        Logger appLogger = mAppCtx.getLogger(this, "init");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        mAppCtx.writeCfgProperties(appLogger);

        String propertyName = "app.ins_path";
        String installPathName = mAppCtx.getString(propertyName);
        if (StringUtils.isEmpty(installPathName))
        {
            appLogger.error(propertyName + ": Is undefined");
            return;
        }
        propertyName = "pkg.root_path";
        mRootSourcePathName = mAppCtx.getString(propertyName);
        if (StringUtils.isEmpty(mRootSourcePathName))
        {
            appLogger.error(propertyName + ": Is undefined");
            return;
        }
        propertyName = "pkg.maven_title";
        mMavenTitle = mAppCtx.getString(propertyName);
        if (StringUtils.isEmpty(mMavenTitle))
        {
            appLogger.error(propertyName + ": Is undefined");
            return;
        }
        propertyName = "pkg.maven_group_id";
        mMavenGroupId = mAppCtx.getString(propertyName);
        if (StringUtils.isEmpty(mMavenGroupId))
        {
            appLogger.error(propertyName + ": Is undefined");
            return;
        }
        mPkgPathName = StringUtils.replace(mMavenGroupId, ".", "/");
        propertyName = "pkg.maven_artifact_id";
        mMavenArtifactId = mAppCtx.getString(propertyName);
        if (StringUtils.isEmpty(mMavenArtifactId))
        {
            appLogger.error(propertyName + ": Is undefined");
            return;
        }

        String javaClassPathName = StringUtils.replaceChars(mMavenGroupId + "." + mMavenArtifactId,
                                                            StrUtl.CHAR_DOT, File.separatorChar);
        mPkgRootPathName = String.format("%s%c%s", mRootSourcePathName,
                                         File.separatorChar, mMavenArtifactId);
        mJavaSrcPathName = String.format("%s%csrc%cmain%cjava%c%s", mPkgRootPathName,
                                         File.separatorChar, File.separatorChar,
                                         File.separatorChar, File.separatorChar,
                                         javaClassPathName);
        mTestSrcPathName = String.format("%s%csrc%ctest%cjava", mPkgRootPathName,
                                         File.separatorChar, File.separatorChar,
                                         File.separatorChar);
        mResourcePathName = String.format("%s%csrc%cmain%cresources", mPkgRootPathName,
                                          File.separatorChar, File.separatorChar,
                                          File.separatorChar);
        mTemplatePathName = String.format("%s%ctemplate%c%s", installPathName,
                                          File.separatorChar, File.separatorChar,
                                          getRunName());
        File templatePathFile = new File(mTemplatePathName);
        if (! templatePathFile.exists())
        {
            appLogger.error(mTemplatePathName + ": Does not exist");
            return;
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        mIsAlive = true;
    }

    @Override
    public void test()
        throws FCException
    {
        Logger appLogger = mAppCtx.getLogger(this, "test");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (! isAlive())
        {
            appLogger.error("Initialization failed - must abort test method.");
            return;
        }

        appLogger.info("The test method was invoked.");
        Sleep.forSeconds(1);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    @SuppressWarnings({"RedundantIfStatement"})
    private boolean createFolders(Utility aUtility)
    {
        String dsPathName = String.format("%s%cds", mResourcePathName, File.separatorChar);
        String binPathName = String.format("%s%c%s%cbin", mRootSourcePathName, File.separatorChar,
                                           mMavenArtifactId, File.separatorChar);
        String logPathName = String.format("%s%c%s%clog", mRootSourcePathName, File.separatorChar,
                                           mMavenArtifactId, File.separatorChar);
        String kitPathName = String.format("%s%c%s%ckit", mRootSourcePathName, File.separatorChar,
                                           mMavenArtifactId, File.separatorChar);

        if (! aUtility.makeFolders(mRootSourcePathName))
            return false;
        else if (! aUtility.makeFolders(logPathName))
            return false;
        else if (! aUtility.makeFolders(dsPathName))
            return false;
        else if (! aUtility.makeFolders(kitPathName))
            return false;
        else if (! aUtility.makeFolders(binPathName))
            return false;
        else if (! aUtility.makeFolders(mJavaSrcPathName))
            return false;
        else if (! aUtility.makeFolders(mTestSrcPathName))
            return false;
        else if (! aUtility.makeFolders(mResourcePathName))
            return false;
        else
            return true;
    }

    private void processFolder(Utility aUtility, DataDoc aDoc, String aTemplatePathName)
        throws IOException
    {
        String fileName, pkgPathFileName;
        Logger appLogger = mAppCtx.getLogger(this, "processFolder");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        File templateFile = new File(aTemplatePathName);
        if (templateFile.exists())
        {
            File[] templateFileList = templateFile.listFiles();
            if (templateFileList != null)
            {
                for (File tmpFile : templateFileList)
                {
                    if (tmpFile.isDirectory())
                        continue;

                    fileName = tmpFile.getName();
                    if (fileName.equals(".svn"))
                        continue;

                    if (StringUtils.endsWithIgnoreCase(fileName, ".java"))
                    {
                        pkgPathFileName = String.format("%s%c%s", mJavaSrcPathName, File.separatorChar, fileName);
                        aUtility.docFieldReplaceInTemplate(aDoc, tmpFile.getAbsolutePath(), pkgPathFileName);
                    }
                    else if (StringUtils.equalsIgnoreCase(fileName, "pom.xml"))
                    {
                        pkgPathFileName = String.format("%s%c%s%c%s", mRootSourcePathName, File.separatorChar,
                                                        mMavenArtifactId, File.separatorChar, fileName);
                        aUtility.docFieldReplaceInTemplate(aDoc, tmpFile.getAbsolutePath(), pkgPathFileName);
                    }
                    else if (StringUtils.equalsIgnoreCase(fileName, "build.xml"))
                    {
                        pkgPathFileName = String.format("%s%c%s%c%s", mRootSourcePathName, File.separatorChar,
                                                        mMavenArtifactId, File.separatorChar, fileName);
                        aUtility.docFieldReplaceInTemplate(aDoc, tmpFile.getAbsolutePath(), pkgPathFileName);
                    }
                    else if (StringUtils.containsIgnoreCase(tmpFile.getAbsolutePath(), "resource"))
                    {
                        pkgPathFileName = String.format("%s%c%s", mResourcePathName, File.separatorChar, fileName);
                        aUtility.docFieldReplaceInTemplate(aDoc, tmpFile.getAbsolutePath(), pkgPathFileName);
                    }
                    else if (StringUtils.equalsIgnoreCase(fileName, "start_console.sh"))
                    {
                        pkgPathFileName = String.format("%s%cbin%cstart_%s.sh", mPkgRootPathName, File.separatorChar,
                                                        File.separatorChar, mMavenArtifactId);
                        aUtility.docFieldReplaceInTemplate(aDoc, tmpFile.getAbsolutePath(), pkgPathFileName);
                    }
                    else if (StringUtils.equalsIgnoreCase(fileName, "stop_console.sh"))
                    {
                        pkgPathFileName = String.format("%s%cbin%cstop_%s.sh", mPkgRootPathName, File.separatorChar,
                                                        File.separatorChar, mMavenArtifactId);
                        aUtility.docFieldReplaceInTemplate(aDoc, tmpFile.getAbsolutePath(), pkgPathFileName);
                    }
                }
            }
        }
        else
            appLogger.error(aTemplatePathName + ": Does not exist");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    private void docAddField(DataDoc aDoc, String aFieldName, String aFieldValue)
    {
        aDoc.add(new DataItem.Builder().name(aFieldName).value(aFieldValue).build());
    }

    private void docAddField(DataDoc aDoc, String aPropertyName)
    {
        String propertyValue = mAppCtx.getString("pkg." + aPropertyName);
        docAddField(aDoc, aPropertyName, propertyValue);
    }

    private void copyUpdateFiles(Utility aUtility)
    {
        Logger appLogger = mAppCtx.getLogger(this, "copyUpdateFiles");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        DataDoc dataDoc = new DataDoc("App Creator Properties");

        docAddField(dataDoc, "maven_title");
        docAddField(dataDoc, "maven_group_id");
        docAddField(dataDoc, "maven_artifact_id");
        docAddField(dataDoc, "maven_group_path", mPkgPathName);


        String javaPackageName = mMavenGroupId + "." + mMavenArtifactId;
        docAddField(dataDoc, "java_package_name", javaPackageName);
        docAddField(dataDoc, "java_package_name_client", javaPackageName + ".client");
        docAddField(dataDoc, "java_package_name_server", javaPackageName + ".client");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
        docAddField(dataDoc, "ts_now", simpleDateFormat.format(new Date()));

        String templateSrcPathName = String.format("%s%csrc", mTemplatePathName, File.separatorChar);
        String templateBinPathName = String.format("%s%cbin", mTemplatePathName, File.separatorChar);
        String templateCfgPathName = String.format("%s%cresource", mTemplatePathName, File.separatorChar);

        try
        {
            processFolder(aUtility, dataDoc, mTemplatePathName);
            processFolder(aUtility, dataDoc, templateSrcPathName);
            processFolder(aUtility, dataDoc, templateCfgPathName);
            processFolder(aUtility, dataDoc, templateBinPathName);
        }
        catch (IOException e)
        {
            String errMsg = String.format("%s: %s", mTemplatePathName, e.getMessage());
            appLogger.error(errMsg, e);
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    @Override
    public void run()
    {
        Logger appLogger = mAppCtx.getLogger(this, "run");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (! isAlive())
        {
            appLogger.error("Initialization failed - must abort run method.");
            return;
        }

        Utility acUtility = new Utility(mAppCtx);
        if (! createFolders(acUtility))
        {
            appLogger.error("Unable to create package folders.");
            return;
        }

        copyUpdateFiles(acUtility);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

    @Override
    public void shutdown()
    {
        Logger appLogger = mAppCtx.getLogger(this, "shutdown");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (isAlive())
        {
            mIsAlive = false;
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }
}
