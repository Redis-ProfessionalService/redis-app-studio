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

package com.redis.app.redis_app_studio.shared;

/**
 * The Constants class is responsible for centralizing the constants
 * used throughout the application.
 */
public class Constants
{
	public static final String APPLICATION_PREFIX_DEFAULT = "ras";

	public static final String LOG_MSG_ENTER = "Enter";
	public static final String LOG_MSG_DEPART = "Depart";

// Application data source constants.

	public static final int DS_LIMIT_SIZE_DEFAULT = 100;
	public static final int DS_CONTENT_LENGTH_DEFAULT = 250;
	public static final long DS_STREAM_REFRESH_INTERVAL = 30;								// 30 seconds
	public static final int REDIS_STREAM_COMMAND_LENGTH = 3000;
	public static final String DS_DOCUMENTS_PATH_NAME = "documents";
	public static final String DS_APPLICATIONS_PATH_NAME = "applications";
	public static final String REDIS_INSIGHT_URL_DEFAULT = "http://localhost:8001/";

	public static final String DS_DATA_STRUCTURE_FLAT_NAME = "Flat";
	public static final String DS_DATA_STRUCTURE_HIERARCHY_NAME = "Hierarchy";

	public static final String DS_DATA_FLAT_PATH_NAME = "data_flat";
	public static final String DS_DATA_HIERARCHY_PATH_NAME = "data_hierarchy";
	public static final String DS_STORAGE_TYPE_JSON_DATA = "JSON Data";
	public static final String DS_STORAGE_TYPE_JSON_SCHEMA = "JSON Schema";
	public static final String DS_STORAGE_TYPE_GRAPH_DATA = "Graph Data";
	public static final String DS_STORAGE_TYPE_GRAPH_NODES_SCHEMA = "Node Schema";
	public static final String DS_STORAGE_TYPE_GRAPH_EDGES_SCHEMA = "Relationship Schema";
	public static final String DS_STORAGE_TYPE_DATA_RECORDS = "Data Records";
	public static final String DS_STORAGE_TYPE_SCHEMA_DEFINITION = "Schema Definition";
	public static final String DS_STORAGE_DETAILS_NAME = "storage_details.csv";
	public static final String DS_REDIS_COMMANDS_NAME = "redis_commands.csv";

	public static final String DS_STORAGE_DOCUMENT_NAME = "document_name";
	public static final String DS_STORAGE_DOCUMENT_FILE = "document_file";
	public static final String DS_STORAGE_DOCUMENT_TYPE = "document_type";
	public static final String DS_STORAGE_DOCUMENT_DATE = "document_date";
	public static final String DS_STORAGE_DOCUMENT_SIZE = "document_size";
	public static final String DS_STORAGE_DOCUMENT_LINK = "document_link";
	public static final String DS_STORAGE_DOCUMENT_OWNER = "document_owner";
	public static final String DS_STORAGE_DOCUMENT_FILES = "document_files";
	public static final String DS_STORAGE_DOCUMENT_TITLE = "document_title";

	public static final String DS_RELEASE_NUMBER_NAME = "release_number";
	public static final String DS_RELEASE_FILE_NAME = "redis_app_studio_releases.csv";
	public static final String DS_STORAGE_DOCUMENT_DESCRIPTION = "document_description";

// Application criteria feature constants.

	public static final String FEATURE_LIMIT_NUMBER = "limit";
	public static final String FEATURE_OFFSET_NUMBER = "offset";
	public static final String FEATURE_ACCOUNT_NAME = "accountName";
	public static final String FEATURE_DATABASE_NAME = "databaseName";
	public static final String FEATURE_ACCOUNT_PASSWORD = "accountPassword";
	public static final String FEATURE_DETAIL_PRIMARY_ID = "detailPrimaryId";
	public static final String FEATURE_APPLICATION_PREFIX = "applicationPrefix";

// Application document	feature constants.

	public static final String FEATURE_DS_TITLE = "dsTitle";
	public static final String FEATURE_APP_PREFIX = "appPrefix";
	public static final String FEATURE_DATA_STRUCTURE = "dataStructure";
	// If you change 'ras_context', you must also update "Application.ts"
	public static final String RAS_CONTEXT_FIELD_NAME = "ras_context";		// appPrefix|dataStructure|dsTitle
	public static final String RAS_CONTEXT_FIELD_TITLE = "RAS Context";

//	Session and application context constants.

	public static final String SESSION_PROPERTY_APPCTX = "app_ctx";
	public static final String APPCTX_PROPERTY_DS_GRID = "ds_grid";
	public static final String APPCTX_PROPERTY_DS_TITLE = "ds_title";
	public static final String APPCTX_PROPERTY_DS_REDIS = "ds_redis";
	public static final String APPCTX_PROPERTY_DS_JSON = "ds_redisjson";
	public static final String APPCTX_PROPERTY_DS_JSON_GRID = "ds_json_grid";
	public static final String APPCTX_PROPERTY_DS_GRAPH = "ds_redisgraph";
	public static final String APPCTX_PROPERTY_DS_GRAPH_SR = "ds_graph_grid_sr";		// search results
	public static final String APPCTX_PROPERTY_DS_GRAPH_GRID = "ds_graph_grid";			// ds_graph grids
	public static final String APPCTX_PROPERTY_DS_GRAPH_NODE = "ds_graph_node";			// DataDoc node
	public static final String APPCTX_PROPERTY_DS_GRAPH_FS_GRID = "ds_graph_fs_grid";	// file system version of graph - used to workaround a search bug
	public static final String APPCTX_PROPERTY_DS_SEARCH = "ds_search";
	public static final String APPCTX_PROPERTY_DS_STREAM = "ds_stream";
	public static final String APPCTX_PROPERTY_DS_RESOURCE = "ds_resource";				// Saved application resource instance
	public static final String APPCTX_PROPERTY_DS_MAIN_SCHEMA = "ds_main_schema";		// Session sharing for schema operations
	public static final String APPCTX_PROPERTY_DS_REL_SCHEMA_FORM = "ds_rel_schema_form";   // Session sharing for relation grid form operations
	public static final String APPCTX_PROPERTY_DS_NODE_SCHEMA_FORM = "ds_node_schema_form";	// Session sharing for node grid form operations
	public static final String APPCTX_PROPERTY_DS_MAIN_GRID_FORM = "ds_main_grid_form";	// Session sharing for grid form operations
	public static final String APPCTX_PROPERTY_DS_GRID_DATA = "ds_grid_data";   		// Session sharing for db rebuild operations
	public static final String APPCTX_PROPERTY_DS_GRID_INDEX = "ds_grid_index"; 		// Session sharing for index rebuild operations
	public static final String APPCTX_PROPERTY_DS_DOC_COMMANDS = "ds_doc_commands";
	public static final String APPCTX_PROPERTY_DS_REDIS_CHANNEL = "ds_redis_channel";
	public static final String APPCTX_PROPERTY_DS_REDIS_STORAGE = "ds_redis_storage";

	public static final String JSON_DS_PROPERTY_TITLE = "ds_json_title";
	public static final String JSON_DS_PROPERTY_APP_PREFIX = "ds_json_app_prefix";

	public static final String GRAPH_DS_PROPERTY_TITLE = "ds_graph_title";
	public static final String GRAPH_DS_PROPERTY_APP_PREFIX = "ds_graph_app_prefix";

	public static final String APPCTX_PROPERTY_PATH_URL = "path_url";
	public static final String APPCTX_PROPERTY_HTTP_URL = "http_url";
	public static final String APPCTX_PROPERTY_SESSION_ID = "session_id";
	public static final String APPCTX_PROPERTY_REMOTE_USER = "remote_user";
	public static final String APPCTX_PROPERTY_REMOTE_ADDR = "remote_addr";
	public static final String APPCTX_PROPERTY_REFERER_URL = "referer_url";
	public static final String APPCTX_PROPERTY_CONTENT_TYPE = "content_type";

// Application session constants.

	public static final String APPCTX_PROPERTY_SESSION = "appctx_session";
	public static final String APPSES_TARGET_MEMORY = "application_memory";
	public static final String APPSES_TARGET_REDIS_CORE = "redis_db_core";
	public static final String APPSES_TARGET_REDIS_JSON = "redis_db_json";
	public static final String APPSES_TARGET_REDIS_GRAPH = "redis_db_graph";
	public static final String APPSES_TARGET_REDIS_TIMESERIES = "redis_db_time_series";
	public static final String APPRES_TARGET_REDIS_SEARCH_HASH = "redis_db_search_hash";
	public static final String APPRES_TARGET_REDIS_SEARCH_JSON = "redis_db_search_json";

	public static final String APPSES_SOURCE_FLAT_GRID_CSV = "flat_grid_csv";
	public static final String APPSES_SOURCE_HIER_GRAPH_CSV = "hier_graph_csv";
	public static final String APPSES_SOURCE_HIER_GRID_JSON = "hier_grid_json";
	public static final String APPSES_SOURCE_FLAT_TIME_SERIES = "flat_timeseries";

// Document type table constants.

	public static final String FIELD_FILE_NAME = "ras_file_name";
	public static final String FIELD_TOTAL_COUNT = "total_count";
	public static final String FIELD_DOC_TYPE_NAME = "ras_doc_type";
	public static final String FIELD_DOC_ICON_NAME = "ras_doc_icon";

// Redis App Studio advanced criteria - Ensure that the list below remains in sync with client.

	public static final String CRITERIA_RAS_PREFIX = "ras_";
	public static final String CRITERIA_RAS_LIMIT_NUMBER = CRITERIA_RAS_PREFIX + "limit";
	public static final String CRITERIA_RAS_DETAIL_ID = CRITERIA_RAS_PREFIX + "detail_id";
	public static final String CRITERIA_RAS_OFFSET_NUMBER = CRITERIA_RAS_PREFIX + "offset";
	public static final String CRITERIA_RAS_ACCOUNT_NAME = CRITERIA_RAS_PREFIX + "account_name";
	public static final String CRITERIA_RAS_DATABASE_NAME = CRITERIA_RAS_PREFIX + "database_name";
	public static final String CRITERIA_RAS_CRITERIA_NAME = CRITERIA_RAS_PREFIX + "criteria_name";
	public static final String CRITERIA_RAS_ACCOUNT_PW_HASH = CRITERIA_RAS_PREFIX + "account_pw_hash";
	public static final String CRITERIA_RAS_ACCOUNT_PASSWORD = CRITERIA_RAS_PREFIX + "account_password";
	public static final String CRITERIA_RAS_APPLICATION_PREFIX = CRITERIA_RAS_PREFIX + "application_prefix";

// Redis App Studio Cookies - Ensure that the list below remains in sync with client.

	public static final String COOKIE_ACCOUNT_NAME = "cookie_account_name";
	public static final String COOKIE_QUERY_SEARCH_TERM = "cookie_search_term";
	public static final String COOKIE_QUERY_FIELDS_LOAD_ID = "cookie_qf_load_id";
	public static final String COOKIE_ACCOUNT_PASSWORD = "cookie_account_password";

// Supported Redis database engines.

	public static enum DataType
	{
		Document, Search, Graph, JSON, TimeSeries, Undefined
	}

	private Constants()
	{
	}
}
