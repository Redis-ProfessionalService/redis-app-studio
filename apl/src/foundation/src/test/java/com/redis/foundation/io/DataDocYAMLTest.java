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

import com.redis.foundation.data.DataDoc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataDocYAMLTest
{
	@Before
	public void setup()
	{
	}

	@Test
	public void exerciseYAML()
	{
		String yamlString1 = "---\n" +
				"- hosts: webservers\n" +
				"\n" +
				"  vars:\n" +
				"    http_port: 80\n" +
				"    max_clients: 200\n" +
				"\n" +
				"  remote_user: root\n" +
				"\n" +
				"  tasks:\n" +
				"  - name: ensure apache is at the latest version\n" +
				"    yum:\n" +
				"      name: httpd\n" +
				"      state: latest\n" +
				"\n" +
				"  - name: write the apache config file\n" +
				"    template:\n" +
				"      src: /srv/httpd.j2\n" +
				"      dest: /etc/httpd.conf\n" +
				"    notify:\n" +
				"    - restart apache\n" +
				"\n" +
				"  - name: ensure apache is running\n" +
				"    service:\n" +
				"      name: httpd\n" +
				"      state: started\n" +
				"\n" +
				"  handlers:\n" +
				"    - name: restart apache\n" +
				"      service:\n" +
				"        name: httpd\n" +
				"        state: restarted";
		String yamlString2 = "---\n" +
				" doe: \"a deer, a female deer\"\n" +
				" ray: \"a drop of golden sun\"\n" +
				" pi: 3.14159\n" +
				" xmas: true\n" +
				" french-hens: 3\n" +
				" calling-birds:\n" +
				"   - huey\n" +
				"   - dewey\n" +
				"   - louie\n" +
				"   - fred\n" +
				" xmas-fifth-day:\n" +
				"   calling-birds: four\n" +
				"   french-hens: 3\n" +
				"   golden-rings: 5\n" +
				"   partridges:\n" +
				"     count: 1\n" +
				"     location: \"a pear tree\"\n" +
				"   turtle-doves: two" +
				"...\n" +
				"---\n" +
				"bar: foo\n" +
				"foo: bar\n" +
				"...\n" +
				"---\n" +
				"one: two\n" +
				"three: four";

		DataDocYAML dataDocYAML = new DataDocYAML();
		Optional<DataDoc> optDataDoc = dataDocYAML.loadFromString(yamlString1);
		assertTrue("Data Doc Exists 1", optDataDoc.isPresent());
		List<DataDoc> dataDocList = dataDocYAML.loadListFromString(yamlString2);
		assertEquals(3, dataDocList.size());

		optDataDoc = dataDocYAML.load("data/example-yaml.yml");
		assertTrue("Data Doc Exists 3", optDataDoc.isPresent());
	}

	@After
	public void cleanup()
	{
	}
}
