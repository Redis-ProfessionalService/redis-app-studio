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

/**
 * A Foundation Class exception and its subclasses are a form of
 * Throwable that indicates conditions that an application developer
 * might want to catch.
 */
public class FCException extends Exception
{
    static final long serialVersionUID = 1L;

    // http://today.java.net/article/2006/04/04/exception-handling-antipatterns
    /**
     * Default constructor.
     */
    public FCException()
    {
        super();
    }

    /**
     * Constructor accepts a default message for the exception.
     *
     * @param aMessage Message describing the exception.
     */
    public FCException(String aMessage)
    {
        super(aMessage);
    }

    /**
     * Constructor accepts a default message and a related
     * throwable object (e.g. context stack trace) for the
     * exception.
     *
     * @param aMessage Message describing the exception.
     * @param aCause An object capturing the context of
     *               the exception.
     */
    public FCException(String aMessage, Throwable aCause)
    {
        super(aMessage, aCause);
    }

    /**
     * Constructor accepts a throwable object (e.g. context
     * stack trace) for the exception.
     *
     * @param aCause An object capturing the context of
     *               the exception.
     */
    public FCException(Throwable aCause)
    {
        super(aCause);
    }

    /**
     * Constructor accepts a default message and a related
     * throwable object (e.g. context stack trace) for the
     * exception.  This form of the exception accepts Java
     * 7 parameters which are not fully supported at this
     * time.
     *
     * @param aMessage Message describing the exception.
     * @param aCause An object capturing the context of
     *               the exception.
     * @param aEnableSuppression Whether or not suppression
     *                           is enabled or disabled.
     * @param aWritableStackTrace whether or not the stack
     *                            trace should be writable.
     */
    public FCException(String aMessage, Throwable aCause,
                       boolean aEnableSuppression, boolean aWritableStackTrace)
    {
        super(aMessage, aCause, aEnableSuppression, aWritableStackTrace);
    }
}
