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
import com.redis.foundation.data.DataItem;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

/**
 * This I/O utility class provides methods to load data document instances from
 * YAML documents.  This YAML encoding is handled by the SnakeYAML package.
 *
 * @see <a href="https://bitbucket.org/asomov/snakeyaml-engine/src/master/src/main/java/org/snakeyaml/engine/v2/">SnakeYAML Parser</a>
 * @see <a href="https://yaml.org/spec/1.2/spec.html">YAML Ain’t Markup Language (YAML) Version 1.2</a>
 * @see <a href="https://blog.stackpath.com/yaml/">What is YAML</a>
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class DataDocYAML
{
	private Yaml mYaml;

	/**
	 * Default constructor.
	 */
	public DataDocYAML()
	{
		mYaml = new Yaml();
	}

	private void processLinkedHashMap(DataDoc aDataDoc, LinkedHashMap<String,Object> aLinkedHashMap)
		throws IOException
	{
		if (aLinkedHashMap != null)
		{
			aLinkedHashMap.entrySet().forEach(entry -> {
				String objectName = entry.getKey();
				Object objectValue = entry.getValue();
				if (objectValue instanceof LinkedHashMap)
				{
					LinkedHashMap<String,Object> linkedHashMap = (LinkedHashMap<String,Object>) objectValue;
					DataDoc dataDoc = new DataDoc(String.format("YAML '%s'", objectName));
					try
					{
						processLinkedHashMap(dataDoc, linkedHashMap);
						aDataDoc.addChild(objectName, dataDoc);
					}
					catch (IOException ignored)
					{
					}
				}
				else if (objectValue instanceof ArrayList)
				{
					ArrayList<Object> arrayList = (ArrayList<Object>) objectValue;
					DataDoc dataDoc = new DataDoc(String.format("YAML '%s'", objectName));
					try
					{
						processArrayList(dataDoc, objectName, arrayList);
						aDataDoc.addChild(objectName, dataDoc);
					}
					catch (IOException ignored)
					{
					}
				}
				else
					aDataDoc.add(new DataItem.Builder().name(objectName).value(objectValue.toString()).build());
			});
			aDataDoc.count();
		}
	}

	private void processArrayList(DataDoc aDataDoc, String aName, ArrayList<Object> anArrayList)
		throws IOException
	{
		DataDoc dataDoc;
		DataItem dataItem;
		Optional<DataItem> optDataItem;

		for (Object objectEntry : anArrayList)
		{
			if (objectEntry instanceof LinkedHashMap)
			{
				LinkedHashMap<String,Object> linkedHashMap = (LinkedHashMap<String,Object>) objectEntry;
				if (StringUtils.isNotEmpty(aName))
				{
					dataDoc = new DataDoc(String.format("YAML '%s'", aName));
					processLinkedHashMap(dataDoc, linkedHashMap);
					aDataDoc.addChild(aName, dataDoc);
				}
				else
					processLinkedHashMap(aDataDoc, linkedHashMap);
			}
			else
			{
				optDataItem = aDataDoc.getItemByNameOptional(aName);
				if (optDataItem.isPresent())
				{
					dataItem = optDataItem.get();
					dataItem.addValue(objectEntry.toString());
				}
				else
					aDataDoc.add(new DataItem.Builder().name(aName).value(objectEntry.toString()).build());
			}
		}
	}

	private void processJavaClasses(DataDoc aDataDoc, Object aYAMLDocument)
		throws IOException
	{
		if (aYAMLDocument != null)
		{
			System.out.printf("[%s] %s%n", aYAMLDocument.getClass().getName(), aYAMLDocument.toString());
			if (aYAMLDocument instanceof ArrayList)
			{
				ArrayList<Object> arrayList = (ArrayList<Object>) aYAMLDocument;
				processArrayList(aDataDoc, StringUtils.EMPTY, arrayList);
			}
			else if (aYAMLDocument instanceof LinkedHashMap)
			{
				LinkedHashMap linkedHashMap = (LinkedHashMap) aYAMLDocument;
				processLinkedHashMap(aDataDoc, linkedHashMap);
			}
			else
				throw new IOException(String.format("YAML Unknown Java Class: %s", aYAMLDocument.getClass().getName()));
		}
	}

	/**
	 * Parses YAML from the input stream into an optional data document instance.
	 *
	 * @param anIS Input stream
	 *
	 * @return Optional data document instance
	 *
	 * @throws IOException I/O exception
	 */
	public Optional<DataDoc> load(InputStream anIS)
		throws IOException
	{
		Optional<DataDoc> optDataDoc = Optional.empty();
		InputStreamReader inputStreamReader = new InputStreamReader(anIS);

		DataDoc dataDoc = new DataDoc("YAML Document");
		Iterable<Object> yamlDocuments = mYaml.loadAll(inputStreamReader);
		for (Object yamlDocument : yamlDocuments)
		{
			try
			{
				processJavaClasses(dataDoc, yamlDocument);
				optDataDoc = Optional.of(dataDoc);
			}
			catch (IOException e)
			{
				dataDoc.setTitle(e.getMessage());
			}
		}

		return optDataDoc;
	}

	/**
	 * Parses YAML from the input stream into a list of data document instances.
	 *
	 * @param anIS Input stream
	 *
	 * @return list data document instance
	 */
	public List<DataDoc> loadList(InputStream anIS)
	{
		DataDoc dataDoc;
		ArrayList<DataDoc> dataDocList = new ArrayList<>();
		InputStreamReader inputStreamReader = new InputStreamReader(anIS);

		Iterable<Object> yamlDocuments = mYaml.loadAll(inputStreamReader);
		for (Object yamlDocument : yamlDocuments)
		{
			dataDoc = new DataDoc("YAML Document");
			try
			{
				processJavaClasses(dataDoc, yamlDocument);
			}
			catch (IOException e)
			{
				dataDoc.setTitle(e.getMessage());
			}
			dataDocList.add(dataDoc);
		}

		return dataDocList;
	}

	/**
	 * Parses YAML path/file name into an optional data document instance.
	 *
	 * @param aPathFileName Path file name
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> load(String aPathFileName)
	{
		Optional<DataDoc> optDataDoc = Optional.empty();

		File yamlFile = new File(aPathFileName);
		if (yamlFile.exists())
		{
			try (FileInputStream fileInputStream = new FileInputStream(yamlFile))
			{
				optDataDoc = load(fileInputStream);
			}
			catch (IOException ignored)
			{
			}
		}

		return optDataDoc;
	}

	/**
	 * Loads collection of YAML documents from the path/file name into a
	 * list of data document instances.
	 *
	 * @param aPathFileName Path file name
	 *
	 * @return List of data documents
	 *
	 * @throws IOException I/O exception
	 */
	public List<DataDoc> loadList(String aPathFileName)
		throws IOException
	{
		List<DataDoc> dataDocList = new ArrayList<>();

		File yamlFile = new File(aPathFileName);
		if (yamlFile.exists())
		{
			try (FileInputStream fileInputStream = new FileInputStream(yamlFile))
			{
				dataDocList = loadList(fileInputStream);
			}
		}

		return dataDocList;
	}

	/**
	 * Parses YAML string into an optional data document instance.
	 *
	 * @param aYAMLString A YAML formatted object string
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> loadFromString(String aYAMLString)
	{
		Optional<DataDoc> optDataDoc = Optional.empty();

		if (StringUtils.isNotEmpty(aYAMLString))
		{
			DataDoc dataDoc = new DataDoc("YAML Document");
			Iterable<Object> yamlDocuments = mYaml.loadAll(aYAMLString);
			for (Object yamlDocument : yamlDocuments)
			{
				try
				{
					processJavaClasses(dataDoc, yamlDocument);
				}
				catch (IOException e)
				{
					dataDoc.setTitle(e.getMessage());
				}
				optDataDoc = Optional.of(dataDoc);
			}
		}

		return optDataDoc;
	}

	/**
	 * Parses YAML string into a list of data document instances.
	 *
	 * @param aYAMLString A YAML formatted object string
	 *
	 * @return List of data documents
	 */
	public List<DataDoc> loadListFromString(String aYAMLString)
	{
		DataDoc dataDoc;
		ArrayList<DataDoc> dataDocList = new ArrayList<>();

		if (StringUtils.isNotEmpty(aYAMLString))
		{
			Iterable<Object> yamlDocuments = mYaml.loadAll(aYAMLString);
			for (Object yamlDocument : yamlDocuments)
			{
				dataDoc = new DataDoc("YAML Document");
				try
				{
					processJavaClasses(dataDoc, yamlDocument);
				}
				catch (IOException e)
				{
					dataDoc.setTitle(e.getMessage());
				}
				dataDocList.add(dataDoc);
			}
		}

		return dataDocList;
	}
}
