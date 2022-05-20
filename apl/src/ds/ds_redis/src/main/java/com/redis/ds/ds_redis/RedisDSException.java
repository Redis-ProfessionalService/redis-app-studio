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

package com.redis.ds.ds_redis;

import com.redis.foundation.std.FCException;

/**
 * A RedisDSException and its subclasses are a form of Throwable that
 * indicates conditions that an application developer might want
 * to catch.
 */
public class RedisDSException extends FCException
{
	static final long serialVersionUID = 10L;

	/**
	 * Default constructor.
	 */
	public RedisDSException()
	{
		super();
	}

	/**
	 * Constructor accepts a default message for the exception.
	 *
	 * @param aMessage Message describing the exception.
	 */
	public RedisDSException(String aMessage)
	{
		super(aMessage);
	}
}