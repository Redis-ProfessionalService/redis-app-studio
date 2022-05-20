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

package com.redis.foundation.ds;

import com.redis.foundation.data.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DSCriteriaTest
{
	@Before
	public void setup()
	{
	}

	public void exercise1()
	{
		String scAdvCriteriaJSON = "{ \"_constructor\":\"AdvancedCriteria\", \"operator\":\"and\", \"criteria\":[ { \"fieldName\":\"userName\", \"operator\":\"iStartsWith\", \"value\":\"R\" }, { \"fieldName\":\"employeeType\", \"operator\":\"iContains\", \"value\":\"time\" }, { \"fieldName\":\"salary\", \"operator\":\"greaterThan\", \"value\":100 } ] }";
		DSCriteria dsCriteria = new DSCriteria("SC Critera", scAdvCriteriaJSON);
		Assert.assertEquals(3, dsCriteria.count());
	}

	public void exercise2()
	{
		String scAdvCriteriaJSON = "{ \"_constructor\":\"AdvancedCriteria\", \"operator\":\"and\", \"criteria\":[ { \"fieldName\":\"userName\", \"operator\":\"iStartsWith\", \"value\":\"R\" }, { \"fieldName\":\"employeeType\", \"operator\":\"inSet\", \"value\":[ \"full time\", \"part time\", \"contract\" ] },{ \"operator\":\"iBetweenInclusive\", \"fieldName\":\"salary\", \"start\":10000, \"end\":30000 } ] }";
		DSCriteria dsCriteria = new DSCriteria("SC Critera", scAdvCriteriaJSON);
		Assert.assertEquals(3, dsCriteria.count());
	}

	@Test
	public void exercise()
	{
		exercise1();
		exercise2();
	}

	@After
	public void cleanup()
	{
	}
}
