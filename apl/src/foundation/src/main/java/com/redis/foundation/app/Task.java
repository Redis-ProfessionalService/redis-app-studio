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

import com.redis.foundation.std.FCException;

/**
 * A Task is an interface that defines a collection of methods
 * that the Application Manager will expect an application to
 * implement in order to successfully run within its context.
 *
 * @since 1.0
 * @author Al Cole
 */
public interface Task extends Runnable
{
    /**
     * Returns the name of the run task.  This name will be used
     * by the application manager to identify which task in the
     * list to run (based on command line arguments).
     *
     * @return Name of the run task.
     */
    public String getRunName();

    /**
     * Returns the name of the test task.  This name will be used
     * by the application manager to identify which task in the
     * list to test (based on command line arguments).
     *
     * @return Name of the test task.
     */
    public String getTestName();

    /**
     * Returns <i>true</i> if this task was properly initialized
     * and is currently executing.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAlive();

    /**
     * If this task is scheduled to be executed (e.g. its run/test
     * name matches the command line arguments), then this method
     * is guaranteed to be executed prior to the thread being
     * started.
     *
     * @param anAppCtx Application manager instance.
     *
     * @throws FCException Application specific exception.
     */
    public void init(AppCtx anAppCtx) throws FCException;

    /**
     * Each task supports a method dedicated to testing or exercising
     * a subset of application features without having to run the
     * mainline thread of task logic.
     *
     * @throws FCException Application specific exception.
     */
    public void test() throws FCException;

    /**
     * Once the task has completed its run, then this method
     * will be invoked by the Application Manager to enable
     * the task to release any resources it was holding.
     * <p>
     * <b>Note:</b>If the JVM detects and external shutdown
     * event (e.g. service is being stopped), then the
     * Application Manager will asynchronously invoke this
     * in hopes that the task can save its state prior to
     * the process exiting.
     * </p>
     */
    public void shutdown();
}
