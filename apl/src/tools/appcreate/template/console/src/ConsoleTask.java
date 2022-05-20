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

package ${maven_group_id}.${maven_artifact_id};

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.Task;
import com.redis.foundation.std.FCException;
import com.redis.foundation.std.Sleep;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ConsoleTask implements a collection of methods that the
 * Application Context will invoke over the lifecycle of a Java
 * thread.
 */
@SuppressWarnings("FieldCanBeLocal")
class ConsoleTask implements Task
{
    private final String mRunName = "console";
    private final String mTestName = "console";

    private AppCtx mAppCtx;
    private AtomicBoolean mIsAlive;

	/**
     * Returns the name of the run task.  This name will be used
     * by the application context to identify which task in the
     * list to run (based on command line arguments).
     *
     * @return Name of the run task.
     */
    @Override
    public String getRunName()
    {
        return mRunName;
    }

	/**
     * Returns the name of the test task.  This name will be used
     * by the application context to identify which task in the
     * list to test (based on command line arguments).
     *
     * @return Name of the test task.
     */
    @Override
    public String getTestName()
    {
        return mTestName;
    }

	/**
     * Returns <i>true</i> if this task was properly initialized
     * and is currently executing.
     *
     * @return <i>true</i> or <i>false</i>
     */
    @Override
    public boolean isAlive()
    {
        if (mIsAlive == null)
            mIsAlive = new AtomicBoolean(false);

        return mIsAlive.get();
    }

	/**
     * If this task is scheduled to be executed (e.g. its run/test
     * name matches the command line arguments), then this method
     * is guaranteed to be executed prior to the thread being
     * started.
     *
     * @param anAppCtx Application context instance.
     *
     * @throws FCException Application specific exception.
     */
    @Override
    public void init(AppCtx anAppCtx)
        throws FCException
    {
        mAppCtx = anAppCtx;
        Logger appLogger = mAppCtx.getLogger(this, "init");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

		mIsAlive = new AtomicBoolean(false);

        appLogger.info("The init method was invoked.");
        Sleep.forSeconds(1);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

        mIsAlive.set(true);
    }

	/**
     * Each task supports a method dedicated to testing or exercising
     * a subset of application features without having to run the
     * mainline thread of task logic.
     *
     * @throws FCException Application specific exception.
     */
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

	/**
     * The {@link Runnable}.run() will be executed after the task
     * has been successfully initialized.  This method is where
     * the application specific logic should be concentrated for
     * the task.
     */
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

        appLogger.info("The run method was invoked.");
        Sleep.forSeconds(1);

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }

	/**
     * Once the task has completed its run, then this method
     * will be invoked by the Application Context to enable
     * the task to release any resources it was holding.
     * <p>
     * <b>Note:</b>If the JVM detects and external shutdown
     * event (e.g. service is being stopped), then the
     * Application Context will asynchronously invoke this
     * in hopes that the task can save its state prior to
     * the process exiting.
	 * </p>
     */
    @Override
    public void shutdown()
    {
        Logger appLogger = mAppCtx.getLogger(this, "shutdown");

        appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);

        if (isAlive())
        {
            appLogger.info("The shutdown method was invoked.");
            Sleep.forSeconds(1);
            mIsAlive.set(false);
        }

        appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);
    }
}
