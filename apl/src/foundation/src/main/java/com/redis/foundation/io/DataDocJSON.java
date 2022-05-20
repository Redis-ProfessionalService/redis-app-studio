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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This I/O utility class provides methods to save/load data document instances as
 * JSON documents.  This JSON encoding is handled by Google's GSON package.
 *
 * @see <a href="https://github.com/google/gson/blob/master/UserGuide.md">Gson User Guide</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DataDocJSON
{
	/**
	 * Default constructor.
	 */
	public DataDocJSON()
	{
	}

	/**
	 * Writes a data item as an object to a JsonWriter stream.
	 *
	 * @param aWriter Json write stream
	 * @param aName Name of JSON field
	 * @param aDataItem Data item instance
	 *
	 * @throws IOException I/O exception
	 */
	public void write(JsonWriter aWriter, String aName, DataItem aDataItem)
		throws IOException
	{
		if (aDataItem.isValueAssigned())
		{
			switch (aDataItem.getType())
			{
				case Integer:
					if (aDataItem.isMultiValue())
						JSONUtil.writeNameIntegerValues(aWriter, aName, aDataItem.getValuesAsInteger());
					else
						JSONUtil.writeNameValue(aWriter, aName, aDataItem.getValueAsInteger());
					break;
				case Long:
					if (aDataItem.isMultiValue())
						JSONUtil.writeNameLongValues(aWriter, aName, aDataItem.getValuesAsLong());
					else
						JSONUtil.writeNameValue(aWriter, aName, aDataItem.getValueAsLong());
					break;
				case Float:
					if (aDataItem.isMultiValue())
						JSONUtil.writeNameFloatValues(aWriter, aName, aDataItem.getValuesAsFloat());
					else
						JSONUtil.writeNameValue(aWriter, aName, aDataItem.getValueAsFloat());
					break;
				case Double:
					if (aDataItem.isMultiValue())
						JSONUtil.writeNameDoubleValues(aWriter, aName, aDataItem.getValuesAsDouble());
					else
						JSONUtil.writeNameValue(aWriter, aName, aDataItem.getValueAsDouble());
					break;
				case Boolean:
					if (aDataItem.isMultiValue())
						JSONUtil.writeNameBooleanValues(aWriter, aName, aDataItem.getValuesAsBoolean());
					else
						JSONUtil.writeNameValue(aWriter, aName, aDataItem.getValueAsBoolean());
					break;
				default:
					if (aDataItem.isMultiValue())
						JSONUtil.writeNameStringValues(aWriter, aName, aDataItem.getValues());
					else
						JSONUtil.writeNameValue(aWriter, aName, aDataItem.getValue());
					break;
			}
		}
	}

	/**
	 * Writes a data item as an object to a JsonWriter stream.
	 *
	 * @param aWriter Json write stream
	 * @param aDataItem Data item instance
	 *
	 * @throws IOException I/O exception
	 */
	public void save(JsonWriter aWriter, DataItem aDataItem)
		throws IOException
	{
		write(aWriter, aDataItem.getName(), aDataItem);
	}

	/**
	 * Writes a data document as an object to a JsonWriter stream.
	 *
	 * @param aWriter Json write stream
	 * @param aDataDoc Data document instance
	 *
	 * @throws IOException I/O exception
	 */
	public void save(JsonWriter aWriter, DataDoc aDataDoc)
		throws IOException
	{
		aWriter.beginObject();
		JSONUtil.writeNameValue(aWriter, "document_name", aDataDoc.getName());
		for (DataItem dataItem : aDataDoc.getItems())
			save(aWriter, dataItem);
		if (aDataDoc.childrenCount() > 0)
		{
			aDataDoc.getChildDocs().entrySet().stream()
					.forEach(e -> {
						try
						{
							int listSize = e.getValue().size();
							aWriter.name(e.getKey());
							if (listSize > 1)
								aWriter.beginArray();
							for (DataDoc dataDoc : e.getValue())
								save(aWriter, dataDoc);
							if (listSize > 1)
								aWriter.endArray();
						}
						catch (IOException ignored)
						{
						}
					});
		}
		aWriter.endObject();
	}

	/**
	 * Writes a data document as an object to a JsonWriter stream.
	 *
	 * @param anOS Output stream
	 * @param aDataDoc Data document instance
	 *
	 * @throws IOException I/O exception
	 */
	public void save(OutputStream anOS, DataDoc aDataDoc)
		throws IOException
	{
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS, StandardCharsets.UTF_8);
		try (JsonWriter jsonWriter = new JsonWriter(outputStreamWriter))
		{
			jsonWriter.setIndent(" ");
			save(jsonWriter, aDataDoc);
		}
	}

	/**
	 * Writes a list of data documents as objects to a JsonWriter stream.
	 *
	 * @param anOS Output stream
	 * @param aDataDocList Data document list
	 *
	 * @throws IOException I/O exception
	 */
	public void save(OutputStream anOS, List<DataDoc> aDataDocList)
			throws IOException
	{
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS, StandardCharsets.UTF_8);
		try (JsonWriter jsonWriter = new JsonWriter(outputStreamWriter))
		{
			jsonWriter.setIndent(" ");
			jsonWriter.beginArray();
			for (DataDoc dataDoc : aDataDocList)
				save(jsonWriter, dataDoc);
			jsonWriter.endArray();
		}
	}

	/**
	 * Writes a data document as an object to file identified by the parameter path file name.
	 *
	 * @param aPathFileName Path file name
	 * @param aDataDoc Data document instance
	 *
	 * @throws IOException I/O exception
	 */
	public void save(String aPathFileName, DataDoc aDataDoc)
		throws IOException
	{
		try (FileOutputStream fileOutputStream = new FileOutputStream(aPathFileName))
		{
			save(fileOutputStream, aDataDoc);
		}
	}

	/**
	 * Writes the data document list as objects to file identified by the parameter path file name.
	 *
	 * @param aPathFileName Path file name
	 * @param aDataDocList Data document list
	 *
	 * @throws IOException I/O exception
	 */
	public void save(String aPathFileName, List<DataDoc> aDataDocList)
		throws IOException
	{
		try (FileOutputStream fileOutputStream = new FileOutputStream(aPathFileName))
		{
			save(fileOutputStream, aDataDocList);
		}
	}

	/**
	 * Saves the data document as a JSON object to a string.
	 *
	 * @param aDataDoc Data document instance
	 *
	 * @return JSON string capturing the data document items
	 *
	 * @throws IOException I/O exception
	 */
	public String saveAsAString(DataDoc aDataDoc)
		throws IOException
	{
		StringWriter stringWriter = new StringWriter();
		try (PrintWriter printWriter = new PrintWriter(stringWriter))
		{
			try (JsonWriter jsonWriter = new JsonWriter(printWriter))
			{
				save(jsonWriter, aDataDoc);
			}
		}

		return stringWriter.toString();
	}

	protected boolean isNextTokenAnArray(JsonReader aReader)
			throws IOException
	{
		JsonToken jsonToken = aReader.peek();

		return (jsonToken == JsonToken.BEGIN_ARRAY);
	}

	protected boolean isNextTokenAnObject(JsonReader aReader)
		throws IOException
	{
		JsonToken jsonToken = aReader.peek();

		return (jsonToken == JsonToken.BEGIN_OBJECT);
	}

	protected void assignValueByTokenType(JsonReader aReader, JsonToken aToken, DataItem aDataItem)
		throws IOException
	{
		String jsonValue;

		switch (aToken)
		{
			case BOOLEAN:
				aDataItem.addValue(aReader.nextBoolean());
				break;
			case NUMBER:
				jsonValue = aReader.nextString();
				if (StringUtils.containsIgnoreCase(jsonValue, "E"))
					aDataItem.addValue(Double.valueOf(jsonValue).longValue());
				else if (NumberUtils.isParsable(jsonValue))
				{
					if (StringUtils.contains(jsonValue, StrUtl.CHAR_DOT))
						aDataItem.addValue(Double.valueOf(jsonValue));
					else
					{
						if (jsonValue.length() > 9)
							aDataItem.addValue(Long.valueOf(jsonValue));
						else
							aDataItem.addValue(Integer.valueOf(jsonValue));
					}
				}
				break;
			case STRING:
				aDataItem.addValue(aReader.nextString());
				break;
			default:
				throw new IOException(String.format("Token type '%s' is a non-primitive", aToken));
		}
	}

	protected DataItem createByTokenType(JsonReader aReader, JsonToken aToken, String aName, String aTitle)
		throws IOException
	{
		DataItem dataItem = new DataItem(aName, aTitle);
		assignValueByTokenType(aReader, aToken, dataItem);
		return dataItem;
	}

	/**
	 * Loads a JSON object from the JsonReader stream into the parent data document instance.
	 *
	 * @param aReader Json reader stream
	 * @param aParentDoc Parent data document instance
	 *
	 * @throws IOException I/O exception
	 */
	public void load(JsonReader aReader, DataDoc aParentDoc)
		throws IOException
	{
		DataItem dataItem;
		String jsonName, jsonTitle;

		aReader.beginObject();
		JsonToken jsonToken = aReader.peek();
		while (jsonToken == JsonToken.NAME)
		{
			jsonName = aReader.nextName();
			if (jsonName.equals("document_name"))
				aParentDoc.setName(aReader.nextString());
			else
			{
				jsonTitle = Data.nameToTitle(jsonName);
				jsonToken = aReader.peek();
				switch (jsonToken)
				{
					case BOOLEAN:
						dataItem = createByTokenType(aReader, jsonToken, jsonName, jsonTitle);
						aParentDoc.add(dataItem);
						break;
					case NUMBER:
						dataItem = createByTokenType(aReader, jsonToken, jsonName, jsonTitle);
						aParentDoc.add(dataItem);
						break;
					case STRING:
						dataItem = createByTokenType(aReader, jsonToken, jsonName, jsonTitle);
						aParentDoc.add(dataItem);
						break;
					case NULL:
						aReader.nextNull();
						break;
					case BEGIN_ARRAY:
						aReader.beginArray();
						if (isNextTokenAnObject(aReader))
						{
							jsonToken = aReader.peek();
							while (jsonToken != JsonToken.END_ARRAY)
							{
								DataDoc childDoc = new DataDoc(jsonName);
								load(aReader, childDoc);
								aParentDoc.addChild(childDoc);
								jsonToken = aReader.peek();
							}
						}
						else
						{
							dataItem = new DataItem(jsonName, jsonTitle);
							jsonToken = aReader.peek();
							while (jsonToken != JsonToken.END_ARRAY)
							{
								assignValueByTokenType(aReader, jsonToken, dataItem);
								jsonToken = aReader.peek();
							}
							aParentDoc.add(dataItem);
						}
						aReader.endArray();
						break;
					case BEGIN_OBJECT:
						DataDoc childDoc = new DataDoc(jsonName);
						load(aReader, childDoc);
						aParentDoc.addChild(childDoc);
						break;
					default:
						aReader.skipValue();
						break;
				}
			}
			jsonToken = aReader.peek();
		}

		aReader.endObject();
	}

	/**
	 * Loads a JSON object from the JsonReader stream into an optional data document instance.
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
		InputStreamReader inputStreamReader = new InputStreamReader(anIS);
		JsonReader jsonReader = new JsonReader(inputStreamReader);
		DataDoc parentDoc = new DataDoc("JSON Parent Data Document");
		load(jsonReader, parentDoc);

		return Optional.of(parentDoc);
	}

	/**
	 * Loads a JSON object from the path/file name into an optional data document instance.
	 *
	 * @param aPathFileName Path file name
	 *
	 * @return Optional data document instance
	 *
	 * @throws IOException I/O exception
	 */
	public Optional<DataDoc> load(String aPathFileName)
		throws IOException
	{
		Optional<DataDoc> optDataDoc;

		File jsonFile = new File(aPathFileName);
		if (! jsonFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
		{
			optDataDoc = load(fileInputStream);
		}

		return optDataDoc;
	}

	/**
	 * Loads an array of JSON objects from the path/file name into a
	 * list of data document instances.  The logic is smart enough to
	 * determine if the JSON file consists of an array of objects or
	 * a simple one and parse it accordingly.
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
		DataDoc dataDoc;
		ArrayList<DataDoc> dataDocList = new ArrayList<>();

		File jsonFile = new File(aPathFileName);
		if (! jsonFile.exists())
			throw new IOException(aPathFileName + ": Does not exist.");

		try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
		{
			InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
			JsonReader jsonReader = new JsonReader(inputStreamReader);
			if (isNextTokenAnArray(jsonReader))
			{
				jsonReader.beginArray();
				int documentId = 1;
				while (isNextTokenAnObject(jsonReader))
				{
					dataDoc = new DataDoc(String.format("JSON Data Document %d", documentId++));
					load(jsonReader, dataDoc);
					if (dataDoc.count() > 0)
						dataDocList.add(dataDoc);
				}
				jsonReader.endArray();
			}
			else
			{
				dataDoc = new DataDoc("JSON Data Document");
				load(jsonReader, dataDoc);
				if (dataDoc.count() > 0)
					dataDocList.add(dataDoc);
			}
		}

		return dataDocList;
	}

	/**
	 * Parses JSON string into an optional data document instance.
	 *
	 * @param aJSONString A JSON formatted object string
	 *
	 * @return Optional data document instance
	 */
	public Optional<DataDoc> loadFromString(String aJSONString)
	{
		Optional<DataDoc> optDataDoc;

		if (StringUtils.isNotEmpty(aJSONString))
		{
			try (InputStream inputStream = new ByteArrayInputStream(aJSONString.getBytes()))
			{
				optDataDoc = load(inputStream);
			}
			catch (IOException e)
			{
				optDataDoc = Optional.empty();
			}
		}
		else
			optDataDoc = Optional.empty();

		return optDataDoc;
	}
}
