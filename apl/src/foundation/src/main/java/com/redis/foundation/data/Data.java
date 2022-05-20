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

package com.redis.foundation.data;

import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * The Data class captures the constants, enumerated types and utility methods for the data package.
 * Specifically, it defines the following:
 * <ul>
 *     <li>Field data types</li>
 *     <li>Date/Time and currency formatting constants</li>
 *     <li>Data type conversion utility methods</li>
 * </ul>
 *
 * @since 1.0
 * @author Al Cole
 */
@SuppressWarnings({"UnusedDeclaration"})
public class Data
{

// Date and time related constants.

	public static final String FORMAT_TIME_AMPM = "HH:mm:ss a";
	public static final String FORMAT_TIME_PLAIN = "HH:mm:ss";
	public static final String FORMAT_DATE_DEFAULT = "MMM-dd-yyyy";
	public static final String VALUE_DATETIME_TODAY = "DateTimeToday";
	public static final String FORMAT_TIME_DEFAULT = FORMAT_TIME_PLAIN;
	public static final String FORMAT_TIMESTAMP_PACKED = "yyMMddHHmmss";
	public static final String FORMAT_SQLISODATE_DEFAULT = "yyyy-MM-dd";
	public static final String FORMAT_MM_DD_YY_SLASH_DEFAULT = "MM/dd/yy";
	public static final String FORMAT_SQLISOTIME_DEFAULT = FORMAT_TIME_DEFAULT;
	public static final String FORMAT_DATETIME_DEFAULT = "MMM-dd-yyyy HH:mm:ss";
	public static final String FORMAT_SQLISODATETIME_DEFAULT = "yyyy-MM-dd HH:mm:ss";
	public static final String FORMAT_SQLORACLEDATE_DEFAULT = FORMAT_DATETIME_DEFAULT;
	public static final String FORMAT_ISO8601DATETIME_NOMILLI = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String FORMAT_ISO8601DATETIME_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	public static final String FORMAT_ISO8601DATETIME_MILLI2D = "yyyy-MM-dd'T'HH:mm:ss.SS'Z'";
	public static final String FORMAT_ISO8601DATETIME_MILLI3D = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final String FORMAT_RFC1123_DATE_TIME = "EEE, dd MMM yyyy HH:mm:ss 'GMT'";

// Number related constants.

	public static final String FORMAT_INTEGER_PLAIN = "#";
	public static final String FORMAT_INTEGER_COMMA = "###,###,###";
	public static final String FORMAT_INTEGER_CURRENCY_COMMA = "$###,###,###";
	public static final String FORMAT_DOUBLE_COMMA = "###,###";
	public static final String FORMAT_DOUBLE_POINT = "###.####";
	public static final String FORMAT_DOUBLE_COMMA_POINT = "###,###.####";
	public static final String FORMAT_DOUBLE_PERCENT = "%##";
	public static final String FORMAT_DOUBLE_CURRENCY_COMMA_POINT = "$###,###.##";

// Document action constants.

	public static final String ACTION_ADD_DOCUMENT = "Add";
	public static final String ACTION_UPD_DOCUMENT = "Update";
	public static final String ACTION_DEL_DOCUMENT = "Delete";

// Data item standard feature constants.

	public static final String FEATURE_IS_SECRET = "isSecret";
	public static final String FEATURE_IS_STORED = "isStored";
	public static final String FEATURE_IS_HIDDEN = "isHidden";			// in SC UI, not visible
	public static final String FEATURE_IS_SEARCH = "isSearch";
	public static final String FEATURE_IS_PRIMARY = "isPrimary";		// implies uniqueness
	public static final String FEATURE_IS_VISIBLE = "isVisible";		// in SC UI, false translates to detail view
	public static final String FEATURE_IS_SUGGEST = "isSuggest";
	public static final String FEATURE_IS_UPDATED = "isUpdated";
	public static final String FEATURE_IS_CURRENCY = "isCurrency";		// Just USD for now
	public static final String FEATURE_IS_REQUIRED = "isRequired";
	public static final String FEATURE_IS_EDITABLE = "isEditable";		// in SC UI, is editable
	public static final String FEATURE_IS_LATITUDE = "isLatitude";
	public static final String FEATURE_IS_LONGITUDE = "isLongitude";
	public static final String FEATURE_MV_DELIMITER = "delimiterChar";
	public static final String FEATURE_IS_MULTIVALUE = "isMultiValue";
	public static final String FEATURE_UI_WIDTH = "uiWidth";			// in SC, cell width
	public static final String FEATURE_UI_HEIGHT = "uiHeight";			// in SC, cell height
	public static final String FEATURE_UI_FORMAT = "uiFormat";			// in SC, presentation format
	public static final String FEATURE_UI_EDITOR = "uiEditor";			// in SC, presentation format
	public static final String FEATURE_UI_HOVER = "uiShowHover";		// in SC, cell hover
	public static final String FEATURE_DATA_FORMAT = "dataFormat";

// General data source feature constants

	public static final String FEATURE_DS_PREFIX = "_";
	public static final String FEATURE_DS_TITLE = FEATURE_DS_PREFIX + "dsTitle";
	public static final String FEATURE_DS_STORAGE = FEATURE_DS_PREFIX + "dsStorage";
	public static final String FEATURE_DS_APP_PREFIX = FEATURE_DS_PREFIX + "appPrefix";
	public static final String FEATURE_DS_STRUCTURE = FEATURE_DS_PREFIX + "dsStructure";

	public static final String FEATURE_DATA_DESCRIPTION = "dataDescription";
	public static final String FEATURE_DS_LIMIT = FEATURE_DS_PREFIX + "limit";
	public static final String FEATURE_DS_OFFSET = FEATURE_DS_PREFIX + "offset";
	public static final String FEATURE_DS_ACTION = FEATURE_DS_PREFIX + "action";
	public static final String FEATURE_DS_SEARCH = FEATURE_DS_PREFIX + "search";
	public static final String FEATURE_DS_FORMAT = FEATURE_DS_PREFIX + "format";
	public static final String FEATURE_DS_SUGGEST = FEATURE_DS_PREFIX + "suggest";
	public static final String FEATURE_DS_PHONETIC = FEATURE_DS_PREFIX + "phonetic";
	public static final String FEATURE_DS_HIGHLIGHT = FEATURE_DS_PREFIX + "highlight";
	public static final String FEATURE_DS_FETCH_POLICY = FEATURE_DS_PREFIX + "fetchPolicy";
	public static final String FEATURE_DS_FACET_VALUE_COUNT = FEATURE_DS_PREFIX + "facetValueCount";
	public static final String FEATURE_DS_FACET_NAME_VALUES = FEATURE_DS_PREFIX + "facetNameValues";
	public static final String FEATURE_DS_REDIS_STORAGE_TYPE = FEATURE_DS_PREFIX + "redisStorageType";
	public static final String FEATURE_DS_DG_CRITERION_COUNT = FEATURE_DS_PREFIX + "dgCriterionCount";

// Data item graph constants and features.

	public static final String GRAPH_COMMON_PREFIX = "common_";
	public static final String GRAPH_COMMON_ID = GRAPH_COMMON_PREFIX + "id";
	public static final String GRAPH_COMMON_ID_TITLE = "Id";
	public static final String GRAPH_COMMON_NAME = GRAPH_COMMON_PREFIX + "name";
	public static final String GRAPH_COMMON_TITLE = "Name";
	public static final String GRAPH_VERTEX_NAME = GRAPH_COMMON_PREFIX + "vertex_name";
	public static final String GRAPH_VERTEX_TITLE = "Vertex Name";
	public static final String GRAPH_VERTEX_LABEL_NAME = GRAPH_COMMON_PREFIX + "vertex_label";
	public static final String GRAPH_VERTEX_LABEL_TITLE = "Label";

	public static final String GRAPH_EDGE_TYPE_NAME = GRAPH_COMMON_PREFIX + "type";
	public static final String GRAPH_EDGE_TYPE_TITLE = "Edge Type";
	public static final String GRAPH_EDGE_WEIGHT_NAME = "edge_weight";
	public static final String GRAPH_EDGE_WEIGHT_TITLE = "Edge Weight";

	public static final String GRAPH_DST_VERTEX_NAME = GRAPH_COMMON_PREFIX + "vertex_dst_name";
	public static final String GRAPH_DST_VERTEX_TITLE = "Destination Vertex";
	public static final String GRAPH_SRC_VERTEX_ID_NAME = GRAPH_COMMON_PREFIX + "vertex_src_id";
	public static final String GRAPH_SRC_VERTEX_ID_TITLE = "Vertex Src Id";
	public static final String GRAPH_DST_VERTEX_ID_NAME = GRAPH_COMMON_PREFIX + "vertex_dst_id";
	public static final String GRAPH_DST_VERTEX_ID_TITLE = "Vertex Dst Id";
	public static final String GRAPH_EDGE_DIRECTION_NAME = GRAPH_COMMON_PREFIX + "edge_direction";
	public static final String GRAPH_EDGE_DIRECTION_TITLE = "Edge Direction";
	public static final String GRAPH_EDGE_DIRECTION_INBOUND = "Inbound";
	public static final String GRAPH_EDGE_DIRECTION_OUTBOUND = "Outbound";

	public static final String FEATURE_IS_GRAPH_TYPE = "isGraphType";
	public static final String FEATURE_IS_GRAPH_LABEL = "isGraphLabel";
	public static final String FEATURE_IS_GRAPH_TITLE = "isGraphTitle";
	public static final String FEATURE_IS_GRAPH_WEIGHT = "isGraphWeight";

// Data item JSON features.

	public static final String FEATURE_JSON_PATH = "jPath";

// Data item difference statuses.

	public static final String DIFF_STATUS_ADDED = "Added";
	public static final String DIFF_STATUS_UPDATED = "Updated";
	public static final String DIFF_STATUS_DELETED = "Deleted";
	public static final String DIFF_STATUS_UNCHANGED = "Unchanged";

// Data item validation constants.

	public static final String VALIDATION_DOC_NAME = "doc_invalid";
	public static final String VALIDATION_ITEM_CHANGED = "item_changed";
	public static final String VALIDATION_PROPERTY_NAME = "item_invalid";
	public static final String VALIDATION_MESSAGE_DEFAULT = "One or more items are invalid.";
	public static final String VALIDATION_MESSAGE_IS_REQUIRED = "A value must be assigned.";
	public static final String VALIDATION_MESSAGE_OUT_OF_RANGE = "The value is out of range.";
	public static final String VALIDATION_MESSAGE_ITEM_CHANGED = "The item value has changed.";
	public static final String VALIDATION_MESSAGE_SIZE_TOO_LARGE = "The value exceeds storage size.";
	public static final String VALIDATION_MESSAGE_PRIMARY_KEY = "There must be exactly one primary key item defined.";

// Data types.

	public static enum Type
	{
		Text, Integer, Long, Float, Double, Boolean, DateTime, Date, Undefined
	}

// Data source criteria operator.

	public static enum Operator
	{
		UNDEFINED, EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL,
		LESS_THAN, LESS_THAN_EQUAL, CONTAINS, NOT_CONTAINS, STARTS_WITH,
		NOT_STARTS_WITH, ENDS_WITH, NOT_ENDS_WITH, EQUAL_FIELD, NOT_EQUAL_FIELD,
		GREATER_THAN_FIELD, GREATER_THAN_EQUAL_FIELD, LESS_THAN_FIELD,
		LESS_THAN_EQUAL_FIELD, CONTAINS_FIELD, NOT_CONTAINS_FIELD,
		STARTS_WITH_FIELD, NOT_STARTS_WITH_FIELD, ENDS_WITH_FIELD,
		NOT_ENDS_WITH_FIELD, BETWEEN, NOT_BETWEEN, BETWEEN_INCLUSIVE, IN, NOT_IN,
		REGEX, EMPTY, NOT_EMPTY, AND, OR, HIGHLIGHT, SNIPPET, FACET, GEO_LOCATION,
		SORT
	}

// Data source sort order.

	public static enum Order
	{
		UNDEFINED, ASCENDING, DESCENDING
	}

// Data graph structures as per https://jgrapht.org/guide/UserOverview#graph-structures

	public static enum GraphStructure
	{
		SimpleGraph, SimpleWeightedGraph, SimpleDirectedGraph, SimpleDirectedWeightedGraph,
		MultiGraph, DirectedPseudograph, DirectedWeightedPseudograph, Undefined
	}

// Data graph data model (VertexEdge combination).

	public static enum GraphData
	{
		ItemItem, DocItem, DocDoc, Undefined
	}

// Data graph object.

	public static enum GraphObject
	{
		Vertex, Edge, Undefined
	}

// Data graph constants.

	public static final String MATCH_ANY_EDGES = "*";
	public static final String MATCH_ANY_VERTEXES = "*";
	public static final String FEATURE_GRAPH_ID = "graphId";
	public static final String FEATURE_GRAPH_SRC_ID = "graphSrcId";
	public static final String FEATURE_GRAPH_DST_ID = "graphDstId";

	private Data()
	{
	}

	/**
	 * Returns a string representation of a field operator.
	 *
	 * @param anOperator Field operator.
	 *
	 * @return String representation of a field operator.
	 */
	public static String operatorToString(Operator anOperator)
	{
		switch (anOperator)
		{
			case EQUAL:
				return "Equal";
			case NOT_EQUAL:
				return "NotEqual";
			case GREATER_THAN:
				return "GreaterThan";
			case GREATER_THAN_EQUAL:
				return "GreaterThanEqual";
			case LESS_THAN:
				return "LessThan";
			case LESS_THAN_EQUAL:
				return "LessThanEqual";
			case CONTAINS:
				return "Contains";
			case NOT_CONTAINS:
				return "NotContains";
			case STARTS_WITH:
				return "StartsWith";
			case NOT_STARTS_WITH:
				return "NotStartsWith";
			case ENDS_WITH:
				return "EndsWith";
			case NOT_ENDS_WITH:
				return "NotEndsWith";
			case EQUAL_FIELD:
				return "equalsField";
			case NOT_EQUAL_FIELD:
				return "notEqualField";
			case GREATER_THAN_FIELD:
				return "greaterThanField";
			case GREATER_THAN_EQUAL_FIELD:
				return "greaterOrEqualField";
			case LESS_THAN_FIELD:
				return "lessThanField";
			case LESS_THAN_EQUAL_FIELD:
				return "lessOrEqualField";
			case CONTAINS_FIELD:
				return "containsField";
			case NOT_CONTAINS_FIELD:
				return "notContainsField";
			case STARTS_WITH_FIELD:
				return "startsWithField";
			case NOT_STARTS_WITH_FIELD:
				return "notStartsWithField";
			case ENDS_WITH_FIELD:
				return "endsWithField";
			case NOT_ENDS_WITH_FIELD:
				return "notEndsWithField";
			case BETWEEN:
				return "Between";
			case NOT_BETWEEN:
				return "NotBetween";
			case BETWEEN_INCLUSIVE:
				return "BetweenInclusive";
			case REGEX:
				return "RegEx";
			case EMPTY:
				return "Empty";
			case NOT_EMPTY:
				return "NotEmpty";
			case AND:
				return "And";
			case OR:
				return "Or";
			case IN:
				return "In";
			case NOT_IN:
				return "NotIn";
			case HIGHLIGHT:
				return "Highlight";
			case SNIPPET:
				return "Snippet";
			case FACET:
				return "Facet";
			case GEO_LOCATION:
				return "GeoLocation";
			case SORT:
				return "Sort";
			default:
				return "Undefined";
		}
	}

	/**
	 * Returns the field operator matching the string representation.
	 *
	 * @param anOperator String representation of a field operator.
	 *
	 * @return Field operator.
	 */
	public static Operator stringToOperator(String anOperator)
	{
		if (StringUtils.equalsIgnoreCase(anOperator, "Equal"))
			return Operator.EQUAL;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotEqual"))
			return Operator.NOT_EQUAL;
		else if (StringUtils.equalsIgnoreCase(anOperator, "GreaterThan"))
			return Operator.GREATER_THAN;
		else if (StringUtils.equalsIgnoreCase(anOperator, "GreaterThanEqual"))
			return Operator.GREATER_THAN_EQUAL;
		else if (StringUtils.equalsIgnoreCase(anOperator, "LessThan"))
			return Operator.LESS_THAN;
		else if (StringUtils.equalsIgnoreCase(anOperator, "LessThanEqual"))
			return Operator.LESS_THAN_EQUAL;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Contains"))
			return Operator.CONTAINS;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotContains"))
			return Operator.NOT_CONTAINS;
		else if (StringUtils.equalsIgnoreCase(anOperator, "StartsWith"))
			return Operator.STARTS_WITH;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotStartsWith"))
			return Operator.NOT_STARTS_WITH;
		else if (StringUtils.equalsIgnoreCase(anOperator, "EndsWith"))
			return Operator.ENDS_WITH;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotEndsWith"))
			return Operator.NOT_ENDS_WITH;
		else if (StringUtils.equalsIgnoreCase(anOperator, "equalsField"))
			return Operator.EQUAL_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "notEqualField"))
			return Operator.NOT_EQUAL_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "greaterThanField"))
			return Operator.GREATER_THAN_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "greaterOrEqualField"))
			return Operator.GREATER_THAN_EQUAL_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "lessThanField"))
			return Operator.LESS_THAN_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "lessOrEqualField"))
			return Operator.LESS_THAN_EQUAL_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "containsField"))
			return Operator.CONTAINS_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "notContainsField"))
			return Operator.NOT_CONTAINS_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "startsWithField"))
			return Operator.STARTS_WITH_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "notStartsWithField"))
			return Operator.NOT_STARTS_WITH_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "endsWithField"))
			return Operator.ENDS_WITH_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "notEndsWithField"))
			return Operator.NOT_ENDS_WITH_FIELD;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Between"))
			return Operator.BETWEEN;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotBetween"))
			return Operator.NOT_BETWEEN;
		else if (StringUtils.equalsIgnoreCase(anOperator, "BetweenInclusive"))
			return Operator.BETWEEN_INCLUSIVE;
		else if (StringUtils.equalsIgnoreCase(anOperator, "RegEx"))
			return Operator.REGEX;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Empty"))
			return Operator.EMPTY;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotEmpty"))
			return Operator.NOT_EMPTY;
		else if (StringUtils.equalsIgnoreCase(anOperator, "And"))
			return Operator.AND;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Or"))
			return Operator.OR;
		else if (StringUtils.equalsIgnoreCase(anOperator, "In"))
			return Operator.IN;
		else if (StringUtils.equalsIgnoreCase(anOperator, "NotIn"))
			return Operator.NOT_IN;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Highlight"))
			return Operator.HIGHLIGHT;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Snippet"))
			return Operator.SNIPPET;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Facet"))
			return Operator.FACET;
		else if (StringUtils.equalsIgnoreCase(anOperator, "GeoLocation"))
			return Operator.GEO_LOCATION;
		else if (StringUtils.equalsIgnoreCase(anOperator, "Sort"))
			return Operator.SORT;
		else
			return Operator.UNDEFINED;
	}

	/**
	 * Converts a SmartClient logical operator string to a Data logical operator.
	 *
	 * @param anOperator SmartClient operator string
	 *
	 * @return Data operator
	 */
	public static Data.Operator scOperatorToDataOperator(String anOperator)
	{
		if (anOperator.equals("equals"))
			return Operator.EQUAL;
		else if (anOperator.equals("greaterThan"))
			return Operator.GREATER_THAN;
		else if (anOperator.equals("greaterOrEqual"))
			return Operator.GREATER_THAN_EQUAL;
		else if (anOperator.equals("lessThan"))
			return Operator.LESS_THAN;
		else if (anOperator.equals("lessOrEqual"))
			return Operator.LESS_THAN_EQUAL;
		else if ((anOperator.equals("between")) || (anOperator.equals("iBetween")))
			return Operator.BETWEEN;
		else if ((anOperator.equals("betweenInclusive")) || (anOperator.equals("iBetweenInclusive")))
			return Operator.BETWEEN_INCLUSIVE;
		else if ((anOperator.equals("contains")) || (anOperator.equals("iContains")))
			return Operator.CONTAINS;
		else if ((anOperator.equals("startsWith")) || (anOperator.equals("iStartsWith")))
			return Operator.STARTS_WITH;
		else if ((anOperator.equals("notStartsWith")) || (anOperator.equals("iNotStartsWith")))
			return Operator.NOT_STARTS_WITH;
		else if ((anOperator.equals("endsWith")) || (anOperator.equals("iEndsWith")))
			return Operator.ENDS_WITH;
		else if ((anOperator.equals("notEndsWith")) || (anOperator.equals("iNotEndsWith")))
			return Operator.NOT_ENDS_WITH;
		else if ((anOperator.equals("notContains")) || (anOperator.equals("iNotContains")))
			return Operator.ENDS_WITH;
		else if ((anOperator.equals("notEqual")) || (anOperator.equals("iNotEqual")))
			return Operator.NOT_EQUAL;
		else if (anOperator.equals("equalsField"))
			return Operator.EQUAL_FIELD;
		else if (anOperator.equals("greaterThanField"))
			return Operator.GREATER_THAN_FIELD;
		else if (anOperator.equals("greaterOrEqualField"))
			return Operator.GREATER_THAN_EQUAL_FIELD;
		else if (anOperator.equals("lessThanField"))
			return Operator.LESS_THAN_FIELD;
		else if (anOperator.equals("lessOrEqualField"))
			return Operator.LESS_THAN_EQUAL_FIELD;
		else if ((anOperator.equals("containsField")) || (anOperator.equals("iContainsField")))
			return Operator.CONTAINS_FIELD;
		else if ((anOperator.equals("startsWithField")) || (anOperator.equals("iStartsWithField")))
			return Operator.STARTS_WITH_FIELD;
		else if ((anOperator.equals("notStartsWithField")) || (anOperator.equals("iNotStartsWithField")))
			return Operator.NOT_STARTS_WITH_FIELD;
		else if ((anOperator.equals("endsWithField")) || (anOperator.equals("iEndsWithField")))
			return Operator.ENDS_WITH_FIELD;
		else if ((anOperator.equals("notEndsWithField")) || (anOperator.equals("iNotEndsWithField")))
			return Operator.NOT_ENDS_WITH_FIELD;
		else if ((anOperator.equals("notContainsField")) || (anOperator.equals("iNotContainsField")))
			return Operator.ENDS_WITH_FIELD;
		else if ((anOperator.equals("notEqualField")) || (anOperator.equals("iNotEqualField")))
			return Operator.NOT_EQUAL_FIELD;
		else if (anOperator.equals("isNull"))
			return Operator.EMPTY;
		else if (anOperator.equals("notNull"))
			return Operator.NOT_EMPTY;
		else if (anOperator.equals("inSet"))
			return Operator.IN;
		else if (anOperator.equals("iregexp"))
			return Operator.REGEX;
		else
			return Operator.STARTS_WITH;	// this default enables UI grid column filtering
	}

	/**
	 * Returns a string representation of a data type.
	 *
	 * @param aType Data type.
	 *
	 * @return String representation of a data type.
	 */
	public static String typeToString(Type aType)
	{
		return aType.name();
	}

	/**
	 * Returns the data type matching the string representation.
	 *
	 * @param aString String representation of a data type.
	 *
	 * @return Data type.
	 */
	public static Type stringToType(String aString)
	{
		return Type.valueOf(aString);
	}

	/**
	 * Returns a char representation of a data type.
	 *
	 * @param aType Data type.
	 *
	 * @return Character representation of a data type.
	 */
	public static char typeToChar(Type aType)
	{
		switch (aType)
		{
			case Text:
				return 'T';
			case Integer:
				return 'I';
			case Long:
				return 'L';
			case Float:
				return 'F';
			case Double:
				return 'D';
			case Boolean:
				return 'B';
			case Date:			// Event
				return 'E';
			case DateTime:		// Calendar
				return 'C';
			default:			// Undefined
				return 'U';
		}
	}

	/**
	 * Returns the data type matching the character representation.
	 *
	 * @param aChar Character representation of a data type.
	 *
	 * @return Data type.
	 */
	public static Type charToType(char aChar)
	{
		switch (aChar)
		{
			case 'T':
				return Type.Text;
			case 'I':
				return Type.Integer;
			case 'L':
				return Type.Long;
			case 'F':
				return Type.Float;
			case 'D':
				return Type.Double;
			case 'B':
				return Type.Boolean;
			case 'E':
				return Type.Date;
			case 'C':
				return Type.DateTime;
			default:
				return Type.Undefined;
		}
	}

	/**
	 * Returns <i>true</i> if the data type represents a numeric
	 * type.
	 *
	 * @param aType Data type.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isNumber(Data.Type aType)
	{
		switch (aType)
		{
			case Integer:
			case Long:
			case Float:
			case Double:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Returns <i>true</i> if the data type represents a boolean type.
	 *
	 * @param aType Data type.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isBoolean(Data.Type aType)
	{
		switch (aType)
		{
			case Boolean:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Returns <i>true</i> if the data type represents a text type.
	 *
	 * @param aType Data type.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isText(Data.Type aType)
	{
		switch (aType)
		{
			case Text:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Returns <i>true</i> if the data type represents a date or time type.
	 *
	 * @param aType Field type.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isDateOrTime(Data.Type aType)
	{
		switch (aType)
		{
			case Date:
			case DateTime:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Returns a title that has been derived from the name of the
	 * data item.  This method will handle the conversion as follows:
	 *
	 * <ul>
	 *     <li>id becomes Id</li>
	 *     <li>employee.name becomes Employee Name</li>
	 *     <li>employee_name becomes Employee Name</li>
	 *     <li>federatedName becomes Federated Name</li>
	 * </ul>
	 *
	 * The logic will ignore any other conventions and simply pass
	 * the original character forward.
	 *
	 * @param aName Name of the data item to convert.
	 *
	 * @return Title for the data item name.
	 */
	public static String nameToTitle(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
		{
			char curChar;
			boolean isLastSpace = true;
			boolean isLastLower = false;

			StringBuilder stringBuilder = new StringBuilder();
			int strLength = aName.length();
			for (int i = 0; i < strLength; i++)
			{
				curChar = aName.charAt(i);
				if ((curChar == StrUtl.CHAR_UNDERLINE) || (curChar == StrUtl.CHAR_DOT))
				{
					curChar = StrUtl.CHAR_SPACE;
					stringBuilder.append(curChar);
				}
				else if (isLastSpace)
					stringBuilder.append(Character.toUpperCase(curChar));
				else if ((Character.isUpperCase(curChar)) && (isLastLower))
				{
					stringBuilder.append(StrUtl.CHAR_SPACE);
					stringBuilder.append(curChar);
				}
				else
					stringBuilder.append(curChar);

				isLastSpace = (curChar == StrUtl.CHAR_SPACE);
				isLastLower = Character.isLowerCase(curChar);
			}

			return stringBuilder.toString();
		}
		else
			return aName;
	}

	/**
	 * Returns a data item name that has been derived from the title.  This
	 * method will handle the conversion as follows:
	 *
	 * <ul>
	 *     <li>Id becomes id</li>
	 *     <li>Employee Name becomes employee_name</li>
	 *     <li>Federated Name becomes federated_name</li>
	 * </ul>
	 *
	 * The logic will ignore any other conventions and simply pass
	 * the original character forward.
	 *
	 * @param aTitle Title string to convert.
	 *
	 * @return Data item name.
	 */
	public static String titleToName(String aTitle)
	{
		if (StringUtils.isNotEmpty(aTitle))
		{
			String itemName = StringUtils.replaceChars(aTitle.toLowerCase(), StrUtl.CHAR_SPACE, StrUtl.CHAR_UNDERLINE);
			if (StringUtils.endsWith(itemName, "(s)"))
				itemName = StringUtils.replace(itemName, "(s)", "s");
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_DOT ,StrUtl.CHAR_UNDERLINE);
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_HYPHEN ,StrUtl.CHAR_UNDERLINE);
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_FORWARDSLASH ,StrUtl.CHAR_UNDERLINE);
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_PAREN_OPEN ,StrUtl.CHAR_UNDERLINE);
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_PAREN_CLOSE ,StrUtl.CHAR_UNDERLINE);
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_BRACKET_OPEN ,StrUtl.CHAR_UNDERLINE);
			itemName = StringUtils.replaceChars(itemName, StrUtl.CHAR_BRACKET_CLOSE ,StrUtl.CHAR_UNDERLINE);
			itemName = StrUtl.removeDuplicateChar(itemName, StrUtl.CHAR_UNDERLINE);
			if (StrUtl.endsWithChar(itemName, StrUtl.CHAR_UNDERLINE))
				itemName = StrUtl.trimLastChar(itemName);

			return itemName;
		}
		else
			return aTitle;
	}

	/**
	 * Returns a Java data type class representing the data type.
	 *
	 * @param aType Data type.
	 *
	 * @return Java data type class.
	 */
	public static Class<?> getTypeClass(Data.Type aType)
	{
		switch (aType)
		{
			case Integer:
				return Integer.class;
			case Long:
				return Long.class;
			case Float:
				return Float.class;
			case Double:
				return Double.class;
			case Boolean:
				return Boolean.class;
			case Date:
			case DateTime:
				return Calendar.class;
			default:
				return String.class;
		}
	}

	/**
	 * Return a data type representing the object type.
	 *
	 * @param anObject Object instance.
	 *
	 * @return Data type.
	 */
	public static Data.Type getTypeByObject(Object anObject)
	{
		if (anObject != null)
		{
			if (anObject instanceof Integer)
				return Type.Integer;
			else if (anObject instanceof Long)
				return Type.Long;
			else if (anObject instanceof Float)
				return Type.Float;
			else if (anObject instanceof Double)
				return Type.Double;
			else if (anObject instanceof Boolean)
				return Type.Boolean;
			else if (anObject instanceof Date)
				return Type.DateTime;
			else if (anObject instanceof Calendar)
				return Type.DateTime;
		}

		return Type.Text;
	}

	/**
	 * Returns a hidden string value (e.g. a simple Caesar-cypher
	 * encryption) based on the value parameter.  This method is
	 * only intended to obscure a field value.  The developer
	 * should use other encryption methods to achieve the goal
	 * of strong encryption.
	 *
	 * @param aValue String to hide.
	 *
	 * @return Hidden string value.
	 */
	public static String hideValue(String aValue)
	{
		if (StrUtl.isHidden(aValue))
			return aValue;
		else
			return StrUtl.hidePassword(aValue);
	}

	/**
	 * Returns a previously hidden (e.g. Caesar-cypher encrypted)
	 * string to its original form.
	 *
	 * @param aValue Hidden string value.
	 *
	 * @return Decrypted string value.
	 */
	public static String recoverValue(String aValue)
	{
		if (StrUtl.isHidden(aValue))
			return StrUtl.recoverPassword(aValue);
		else
			return aValue;
	}

	/**
	 * Returns an <i>int</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static int createInt(String aValue)
	{
		if (NumberUtils.isDigits(aValue))
			return Integer.parseInt(aValue);
		else
			return Integer.MIN_VALUE;
	}

	/**
	 * Returns an <i>Integer</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static Integer createIntegerObject(String aValue)
	{
		if (NumberUtils.isDigits(aValue))
			return Integer.valueOf(aValue);
		else
			return Integer.MIN_VALUE;
	}

	/**
	 * Returns a <i>long</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static long createLong(String aValue)
	{
		if (NumberUtils.isDigits(aValue))
			return Long.parseLong(aValue);
		else
			return Long.MIN_VALUE;
	}

	/**
	 * Returns a <i>Long</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static Long createLongObject(String aValue)
	{
		if (NumberUtils.isDigits(aValue))
			return Long.valueOf(aValue);
		else
			return Long.MIN_VALUE;
	}

	/**
	 * Returns a <i>float</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static float createFloat(String aValue)
	{
		if (StringUtils.isNotEmpty(aValue))
			return Float.parseFloat(aValue);
		else
			return Float.NaN;
	}

	/**
	 * Returns a <i>Float</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static Float createFloatObject(String aValue)
	{
		if (StringUtils.isNotEmpty(aValue))
			return Float.valueOf(aValue);
		else
			return Float.NaN;
	}

	/**
	 * Returns a <i>double</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static double createDouble(String aValue)
	{
		if (StringUtils.isNotEmpty(aValue))
			return Double.parseDouble(aValue);
		else
			return Double.NaN;
	}

	/**
	 * Returns a <i>Double</i> representation of the data item
	 * value string.
	 *
	 * @param aValue Numeric string value.
	 *
	 * @return Converted value.
	 */
	public static Double createDoubleObject(String aValue)
	{
		if (StringUtils.isNotEmpty(aValue))
			return Double.valueOf(aValue);
		else
			return Double.NaN;
	}

	/**
	 * Returns <i>true</i> if the data item value represents a boolean
	 * true string (e.g. yes, true) or <i>false</i> otherwise.
	 *
	 * @param aValue Boolean string value.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isValueTrue(String aValue)
	{
		return StrUtl.stringToBoolean(aValue);
	}

	/**
	 * Returns a <i>Date</i> representation of the data item value
	 * string based on the format mask property.
	 *
	 * @param aValue Date/Time string value.
	 * @param aFormatMask SimpleDateFormat mask.
	 *
	 * @return Converted value.
	 */
	public static Date createDate(String aValue, String aFormatMask)
	{
		if (StringUtils.isNotEmpty(aValue))
		{
			ParsePosition parsePosition = new ParsePosition(0);
			SimpleDateFormat simpleDateFormat;
			if (StringUtils.isNotEmpty(aFormatMask))
				simpleDateFormat = new SimpleDateFormat(aFormatMask);
			else
				simpleDateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
			return simpleDateFormat.parse(aValue, parsePosition);
		}
		else
			return new Date();
	}

	/**
	 * Returns a <i>Date</i> representation of the data item value
	 * string based on the FORMAT_DATETIME_DEFAULT format mask
	 * property.
	 *
	 * @param aValue Date/Time string value.
	 *
	 * @return Converted value.
	 */
	public static Date createDate(String aValue)
	{
		if (StringUtils.isNotEmpty(aValue))
		{
			if (aValue.equals(Data.VALUE_DATETIME_TODAY))
				return new Date();
			else
			{
				ParsePosition parsePosition = new ParsePosition(0);
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(Data.FORMAT_DATETIME_DEFAULT);
				return simpleDateFormat.parse(aValue, parsePosition);
			}
		}
		else
			return new Date();
	}

	/**
	 * Returns a formatted <i>String</i> representation of the date
	 * parameter based on the format mask parameter.  If the format
	 * mask is <i>null</i>, then <code>Field.FORMAT_DATETIME_DEFAULT</code>
	 * will be used.
	 *
	 * @param aDate Date/Time to convert.
	 * @param aFormatMask Format mask string.
	 *
	 * @return String representation of the date/time parameter.
	 */
	public static String dateValueFormatted(Date aDate, String aFormatMask)
	{
		if (StringUtils.isEmpty(aFormatMask))
			aFormatMask = Data.FORMAT_DATETIME_DEFAULT;

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(aFormatMask);

		return simpleDateFormat.format(aDate.getTime());
	}

	/**
	 * Returns a formatted <i>String</i> representation of the timestamp
	 * parameter based on the format mask parameter.  If the format
	 * mask is <i>null</i>, then <code>Field.FORMAT_DATETIME_DEFAULT</code>
	 * will be used.
	 *
	 * @param aTimestamp Date/Time to convert.
	 * @param aFormatMask Format mask string.
	 *
	 * @return String representation of the date/time parameter.
	 */
	public static String dateValueFormatted(Timestamp aTimestamp, String aFormatMask)
	{
		if (StringUtils.isEmpty(aFormatMask))
			aFormatMask = Data.FORMAT_DATETIME_DEFAULT;

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(aFormatMask);

		return simpleDateFormat.format(aTimestamp.getTime());
	}

	/**
	 * Returns the time representation of data item value
	 * string based on the FORMAT_DATETIME_DEFAULT format mask
	 * property.
	 *
	 * @param aValue Date/Time string value.
	 *
	 * @return The number of milliseconds since January 1, 1970, 00:00:00 GMT
	 * represented by this value.
	 */
	public static long createDateLong(String aValue)
	{
		Date dateValue = createDate(aValue);
		return dateValue.getTime();
	}

	/**
	 * Identifies if the feature name is standard to the foundation
	 * data package.
	 *
	 * @param aName Name of the feature
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public static boolean isFeatureStandard(String aName)
	{
		if (StringUtils.isNotEmpty(aName))
		{
			switch (aName)
			{
				case FEATURE_IS_SECRET:
				case FEATURE_IS_STORED:
				case FEATURE_IS_HIDDEN:
				case FEATURE_IS_SEARCH:
				case FEATURE_IS_PRIMARY:
				case FEATURE_IS_VISIBLE:
				case FEATURE_IS_SUGGEST:
				case FEATURE_IS_UPDATED:
				case FEATURE_IS_REQUIRED:
				case FEATURE_IS_LATITUDE:
				case FEATURE_IS_LONGITUDE:
					return true;
			}
		}

		return false;
	}

	/**
	 * Creates a data document with items suitable for a schema editor UI.
	 *
	 * @param aName Name of the schema
	 *
	 * @return Data document representing a schema
	 */
	public static DataDoc createSchemaDoc(String aName)
	{
		DataDoc schemaDoc = new DataDoc(aName);
		schemaDoc.add(new DataItem.Builder().name("item_name").title("Item Name").build());
		schemaDoc.add(new DataItem.Builder().name("item_type").title("Item Type").build());
		schemaDoc.add(new DataItem.Builder().name("item_title").title("Item Title").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_PRIMARY).title("Is Primary").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_REQUIRED).title("Is Required").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_VISIBLE).title("Is Visible").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_SEARCH).title("Is Searchable").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_SUGGEST).title("Is Suggest").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_SECRET).title("Is Secret").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_LATITUDE).title("Is Latitude").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_LONGITUDE).title("Is Longitude").build());
		schemaDoc.add(new DataItem.Builder().type(Type.Boolean).name(FEATURE_IS_HIDDEN).title("Is Hidden").build());

		return schemaDoc;
	}

	/**
	 * Convert a data document schema definition into a data grid suitable for
	 * rendering in a schema editor UI.
	 *
	 * @param aSchemaDoc Data document instance (representing the schema)
	 * @param anIsExtended If <i>true</i>, then non standard features will be recognized
	 *
	 * @return Data grid representing the schema defintion
	 */
	public static DataGrid schemaDocToDataGrid(DataDoc aSchemaDoc, boolean anIsExtended)
	{
		HashMap<String,String> mapFeatures;

// Create our initial data grid schema based on standard item info plus features.

		String schemaName = String.format("%s Schema", aSchemaDoc.getName());
		DataDoc schemaDoc = createSchemaDoc(schemaName);
		DataGrid dataGrid = new DataGrid(schemaDoc);

// Extend the data grid schema for use defined features.

		if (anIsExtended)
		{
			Data.Type featureType;
			String featureKey, featureValue, featureTitle;

			for (DataItem dataItem : aSchemaDoc.getItems())
			{
				mapFeatures = dataItem.getFeatures();
				for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				{
					featureKey = featureEntry.getKey();
					if (! isFeatureStandard(featureKey))
					{
						if (StringUtils.startsWith(featureKey, "is"))
							featureType = Type.Boolean;
						else
						{
							featureValue = featureEntry.getValue();
							if (NumberUtils.isParsable(featureValue))
							{
								int offset = featureValue.indexOf(StrUtl.CHAR_DOT);
								if (offset == -1)
									featureType = Type.Integer;
								else
									featureType = Type.Float;
							}
							else
								featureType = Type.Text;
						}
						featureTitle = nameToTitle(featureKey);
						dataGrid.addCol(new DataItem.Builder().type(featureType).name(featureKey).title(featureTitle).build());
					}
				}
			}
		}

// Populate each row of the data grid based on the schema data document.

		for (DataItem dataItem : aSchemaDoc.getItems())
		{
			dataGrid.newRow();
			dataGrid.setValueByName("item_name", dataItem.getName());
			dataGrid.setValueByName("item_type", typeToString(dataItem.getType()));
			dataGrid.setValueByName("item_title", dataItem.getTitle());
			mapFeatures = dataItem.getFeatures();
			for (Map.Entry<String, String> featureEntry : mapFeatures.entrySet())
				dataGrid.setValueByName(featureEntry.getKey(), featureEntry.getValue());
			dataGrid.addRow();
		}

		return dataGrid;
	}

	/**
	 * Collapses a data grid representing a schema definition back into a
	 * data document schema.  This method assumes that you invoked the
	 * schemaDocToDataGrid() method to build the data grid originally.
	 *
	 * @param aDataGrid Data grid instance
	 *
	 * @return Data document schema definition
	 */
	public static DataDoc dataGridToSchemaDoc(DataGrid aDataGrid)
	{
		DataDoc dataDoc;
		DataItem schemaItem;
		Optional<DataItem> optDataItem;
		HashMap<String,String> mapFeatures;
		String itemName, itemType, itemTitle;

		DataDoc schemaDoc = new DataDoc(aDataGrid.getName());
		int rowCount = aDataGrid.rowCount();
		for (int row = 0; row < rowCount; row++)
		{
			dataDoc = aDataGrid.getRowAsDoc(row);
			itemName = dataDoc.getValueByName("item_name");
			itemType = dataDoc.getValueByName("item_type");
			itemTitle = dataDoc.getValueByName("item_title");
			if ((StringUtils.isNotEmpty(itemName)) && (StringUtils.isNotEmpty(itemType)) &&
				(StringUtils.isNotEmpty(itemTitle)))
			{
				optDataItem = schemaDoc.getItemByNameOptional(itemName);
				if (optDataItem.isEmpty())
				{
					schemaItem = new DataItem.Builder().type(stringToType(itemType)).name(itemName).title(itemTitle).build();
					for (DataItem dataItem : dataDoc.getItems())
					{
						if (! StringUtils.startsWith(dataItem.getName(), "item_"))
							dataItem.addFeature(dataItem.getName(), dataItem.getValue());
					}
					schemaDoc.add(schemaItem);
				}
			}
		}

		return schemaDoc;
	}
}
