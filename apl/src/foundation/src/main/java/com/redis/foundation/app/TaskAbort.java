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

package com.redis.foundation.app;

import com.redis.foundation.std.Sleep;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * The TaskAbort class is a handler for the <code>Runtime.addShutdownHook()</code>
 * method.  It primary responsibility is to notify all executing tasks that
 * a JVM shutdown is imminent.
 * <p>
 * <b>Note:</b>&nbsp;This is a specialized class for the AppCtx and should be
 * avoided for general applications.
 * </p>
 */
public class TaskAbort extends Thread
{
    private AppCtx mAppCtx;
    private ArrayList<Task> mTaskList;

    public TaskAbort(AppCtx anAppCtx, ArrayList<Task> aTaskList)
    {
        mAppCtx = anAppCtx;
        mTaskList = aTaskList;
    }

    @Override
    public void run()
    {
        Logger appLogger = mAppCtx.getLogger(this, "init");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        mAppCtx.setIsAliveFlag(false);
        appLogger.warn("Abort request received - shutting down tasks.");

        for (Task appTask : mTaskList)
        {
            if (appTask.isAlive())
                appTask.shutdown();
        }

// Allow a little time for the resources to finish up.

        Sleep.forSeconds(1);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }
}
