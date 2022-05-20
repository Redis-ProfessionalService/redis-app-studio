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

import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_json.JsonDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.data.DataRange;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.ds.SmartClientXML;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

/**
 * Application generation class for Redis App Studio.
 *
 *  @see <a href="https://www.smartclient.com/smartclient/isomorphic/system/reference/?id=type..FieldType">SmartClient Field Types</a>
 *  @see <a href="https://www.smartclient.com/smartclient-12.0/isomorphic/system/reference/?id=type..FormatString">SmartClient Data Formatting</a>
 */
public class ApplicationGenerator
{
	private GridDS mGridDS;
	private DataDoc mAppManDoc;
	private final DataDoc mAppGenDoc;
	private final SessionContext mSessionContext;

	public ApplicationGenerator(SessionContext aSessionContext, DataDoc anAppGenDoc)
	{
		mAppGenDoc = anAppGenDoc;
		mSessionContext = aSessionContext;
	}

	private boolean isPrefixAlreadyAssigned(String anAppPrefix)
	{
		boolean prefixExists;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "isPrefixAlreadyAssigned");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String dataPathName = appCtx.getProperty(appCtx.APP_PROPERTY_DAT_PATH).toString();
		String dataPattern = String.format("%cdata", File.separatorChar);
		String htmlPathName = StringUtils.removeEnd(dataPathName, dataPattern);

		File htmlPathFile = new File(htmlPathName);
		String[] filesNames = htmlPathFile.list(new PrefixFileFilter(anAppPrefix));
		prefixExists = (filesNames != null) && (filesNames.length > 0);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return prefixExists;
	}

	private String deriveHTMLPathFileName(String aFileName)
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "deriveHTMLPathFileName");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String dataPathName = appCtx.getProperty(appCtx.APP_PROPERTY_DAT_PATH).toString();
		String dataPattern = String.format("%cdata", File.separatorChar);
		String htmlPathName = StringUtils.removeEnd(dataPathName, dataPattern);
		String htmlPathFileName = String.format("%s%c%s", htmlPathName, File.separatorChar, aFileName);
		appLogger.debug(htmlPathFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return htmlPathFileName;
	}

	private String deriveDSPathFileName(String aFileName)
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "deriveDSPathFileName");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String dataPathName = appCtx.getProperty(appCtx.APP_PROPERTY_DAT_PATH).toString();
		String dataPattern = String.format("%cdata", File.separatorChar);
		String dsPattern = String.format("%cshared%cds", File.separatorChar, File.separatorChar);
		String dsPathName = StringUtils.replace(dataPathName, dataPattern, dsPattern);
		String dsPathFileName = String.format("%s%c%s", dsPathName, File.separatorChar, aFileName);
		appLogger.debug(dsPathFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return dsPathFileName;
	}

	private void adjustEdgeItemTitles(GraphDS aGraphDS, DataDoc anExtendedSchemaDoc)
	{
		Optional<DataItem> optDataItem;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "adjustEdgeItemTitles");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DataDoc edgeSchemaDoc = aGraphDS.getEdgeSchema();
		for (DataItem dataItem : anExtendedSchemaDoc.getItems())
		{
			optDataItem = edgeSchemaDoc.getItemByNameOptional(dataItem.getName());
			optDataItem.ifPresent(aDataItem -> dataItem.setTitle(aDataItem.getTitle()));

// TODO: The following is a temporary hack for the graph demonstration
			if (dataItem.getTitle().equals(Data.GRAPH_EDGE_TYPE_TITLE))
				dataItem.setTitle("Role");
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private DataDoc enrichSchemaDoc(DataDoc aSchemaDoc)
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "enrichSchemaDoc");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = new DataDoc(aSchemaDoc);
		schemaDoc.add(new DataItem.Builder().name(Constants.RAS_CONTEXT_FIELD_NAME).title(Constants.RAS_CONTEXT_FIELD_TITLE).isVisible(false).isHidden(true).build());

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return schemaDoc;
	}

	private DataDoc nodeFilterSchemaDoc(DataDoc aSchemaDoc)
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "nodeFilterSchemaDoc");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = new DataDoc(aSchemaDoc);
		schemaDoc.remove(Data.GRAPH_VERTEX_LABEL_NAME);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return schemaDoc;
	}

	private DataDoc relationshipFilterSchemaDoc(DataDoc aSchemaDoc)
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "relationshipFilterSchemaDoc");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DataDoc schemaDoc = new DataDoc(aSchemaDoc);
		schemaDoc.add(new DataItem.Builder().name(Data.GRAPH_COMMON_NAME).title(Data.GRAPH_COMMON_TITLE).isVisible(true).build());
		schemaDoc.remove(Data.GRAPH_VERTEX_NAME);
		schemaDoc.remove(Data.GRAPH_SRC_VERTEX_ID_NAME);
		schemaDoc.remove(Data.GRAPH_DST_VERTEX_ID_NAME);
		schemaDoc.remove(Data.GRAPH_EDGE_DIRECTION_NAME);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);

		return schemaDoc;
	}

	private void saveAppViewDataSource(GraphDS aGraphDS, String aPackageName)
		throws IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveAppViewDataSource");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DataDoc vertexSchemaDoc = enrichSchemaDoc(aGraphDS.getVertexGridDS().getSchema());
		String fsAppName = StringUtils.remove(mAppGenDoc.getValueByName("app_name"), StrUtl.CHAR_SPACE);
		String dsName = String.format("%s-%s", mAppGenDoc.getValueByName("app_prefix"), fsAppName);
		mAppGenDoc.setValueByName("gen_ds_1", dsName);
		String dsFileName = String.format("%s.ds.xml", dsName);
		String dsPathFileName = deriveDSPathFileName(dsFileName);
		SmartClientXML smartClientXML = new SmartClientXML();
		try (PrintWriter printWriter = new PrintWriter(dsPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<DataSource ID=\"%s-%s\" serverConstructor=\"com.redis.app.redis_app_studio.%s.AppViewGridDS\">%n",
							   mAppGenDoc.getValueByName("app_prefix"), fsAppName, aPackageName);
			smartClientXML.save(printWriter, 1, vertexSchemaDoc);
			printWriter.printf(" <operationBindings>%n");
			printWriter.printf("  <binding operationId=\"exportData\" operationType=\"fetch\" serverMethod=\"exportData\">%n");
			printWriter.printf("   <serverObject  lookupStyle=\"new\" className=\"com.redis.app.redis_app_studio.%s.AppViewExportGridDS\"/>%n",
							   aPackageName);
			printWriter.printf("  </binding>%n");
			printWriter.printf(" </operationBindings>%n");
			printWriter.printf(" <generatedBy>RAS DataSource Generator Jan-06-2022</generatedBy>%n");
			printWriter.printf("</DataSource>%n");
		}
		catch (Exception e)
		{
			throw new IOException(dsPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated node data source file: %s", dsPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, dsPathFileName);

		DataDoc edgeSchemaDoc = enrichSchemaDoc(aGraphDS.createEdgeExtendedSchema(false));
		adjustEdgeItemTitles(aGraphDS, edgeSchemaDoc);
		fsAppName = StringUtils.remove(mAppGenDoc.getValueByName("app_name"), StrUtl.CHAR_SPACE) + "Rel";
		dsName = String.format("%s-%s", mAppGenDoc.getValueByName("app_prefix"), fsAppName);
		mAppGenDoc.setValueByName("gen_ds_2", dsName);
		dsFileName = String.format("%s.ds.xml", dsName);
		dsPathFileName = deriveDSPathFileName(dsFileName);
		try (PrintWriter printWriter = new PrintWriter(dsPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<DataSource ID=\"%s-%s\" serverConstructor=\"com.redis.app.redis_app_studio.%s.AppViewGridRelDS\">%n",
							   mAppGenDoc.getValueByName("app_prefix"), fsAppName, aPackageName);
			smartClientXML.save(printWriter, 1, edgeSchemaDoc);
			printWriter.printf(" <generatedBy>RAS DataSource Generator Jan-06-2022</generatedBy>%n");
			printWriter.printf("</DataSource>%n");
		}
		catch (Exception e)
		{
			throw new IOException(dsPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated relationship data source file: %s", dsPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, dsPathFileName);

		edgeSchemaDoc = enrichSchemaDoc(aGraphDS.createEdgeExtendedSchema(true));
		adjustEdgeItemTitles(aGraphDS, edgeSchemaDoc);
		fsAppName = StringUtils.remove(mAppGenDoc.getValueByName("app_name"), StrUtl.CHAR_SPACE) + "RelOut";
		dsName = String.format("%s-%s", mAppGenDoc.getValueByName("app_prefix"), fsAppName);
		mAppGenDoc.setValueByName("gen_ds_3", dsName);
		dsFileName = String.format("%s.ds.xml", dsName);
		dsPathFileName = deriveDSPathFileName(dsFileName);
		try (PrintWriter printWriter = new PrintWriter(dsPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<DataSource ID=\"%s-%s\" serverConstructor=\"com.redis.app.redis_app_studio.%s.AppViewGridRelOutDS\">%n",
							   mAppGenDoc.getValueByName("app_prefix"), fsAppName, aPackageName);
			smartClientXML.save(printWriter, 1, edgeSchemaDoc);
			printWriter.printf(" <generatedBy>RAS DataSource Generator Jan-06-2022</generatedBy>%n");
			printWriter.printf("</DataSource>%n");
		}
		catch (Exception e)
		{
			throw new IOException(dsPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated relationship outbound grid data source file: %s", dsPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, dsPathFileName);

		if (aPackageName.equals("rg"))
		{
			DataDoc nodeFilterSchemaDoc = nodeFilterSchemaDoc(aGraphDS.getVertexGridDS().getSchema());
			fsAppName = "NodeFilter";
			dsName = String.format("%s-%s", mAppGenDoc.getValueByName("app_prefix"), fsAppName);
			mAppGenDoc.setValueByName("gen_ds_4", dsName);
			dsFileName = String.format("%s.ds.xml", dsName);
			dsPathFileName = deriveDSPathFileName(dsFileName);
			try (PrintWriter printWriter = new PrintWriter(dsPathFileName, StandardCharsets.UTF_8))
			{
				printWriter.printf("<DataSource ID=\"%s-%s\" serverConstructor=\"com.redis.app.redis_app_studio.%s.AppViewGridDS\">%n",
								   mAppGenDoc.getValueByName("app_prefix"), fsAppName, aPackageName);
				smartClientXML.save(printWriter, 1, nodeFilterSchemaDoc);
				printWriter.printf(" <generatedBy>RAS DataSource Generator Jan-06-2022</generatedBy>%n");
				printWriter.printf("</DataSource>%n");
			}
			catch (Exception e)
			{
				throw new IOException(dsPathFileName + ": " + e.getMessage());
			}
			appLogger.debug(String.format("Generated node data source file: %s", dsPathFileName));
			mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, dsPathFileName);

			DataDoc relationshipFilterSchemaDoc = relationshipFilterSchemaDoc(aGraphDS.getEdgeGridDS().getSchema());
			fsAppName = "RelationshipFilter";
			dsName = String.format("%s-%s", mAppGenDoc.getValueByName("app_prefix"), fsAppName);
			dsFileName = String.format("%s.ds.xml", dsName);
			mAppGenDoc.setValueByName("gen_ds_5", dsName);
			dsPathFileName = deriveDSPathFileName(dsFileName);
			try (PrintWriter printWriter = new PrintWriter(dsPathFileName, StandardCharsets.UTF_8))
			{
				printWriter.printf("<DataSource ID=\"%s-%s\" serverConstructor=\"com.redis.app.redis_app_studio.%s.AppViewGridDS\">%n",
								   mAppGenDoc.getValueByName("app_prefix"), fsAppName, aPackageName);
				smartClientXML.save(printWriter, 1, relationshipFilterSchemaDoc);
				printWriter.printf(" <generatedBy>RAS DataSource Generator Jan-06-2022</generatedBy>%n");
				printWriter.printf("</DataSource>%n");
			}
			catch (Exception e)
			{
				throw new IOException(dsPathFileName + ": " + e.getMessage());
			}
			appLogger.debug(String.format("Generated node data source file: %s", dsPathFileName));
			mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, dsPathFileName);
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveAppViewDataSource(DataDoc aDataDoc, String aPackageName)
		throws IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveAppViewDataSource");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String fsAppName = StringUtils.remove(mAppGenDoc.getValueByName("app_name"), StrUtl.CHAR_SPACE);
		String dsName = String.format("%s-%s", mAppGenDoc.getValueByName("app_prefix"), fsAppName);
		mAppGenDoc.setValueByName("gen_ds_1", dsName);
		String dsFileName = String.format("%s.ds.xml", dsName);
		String dsPathFileName = deriveDSPathFileName(dsFileName);
		SmartClientXML smartClientXML = new SmartClientXML();
		try (PrintWriter printWriter = new PrintWriter(dsPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<DataSource ID=\"%s-%s\" serverConstructor=\"com.redis.app.redis_app_studio.%s.AppViewGridDS\">%n",
							   mAppGenDoc.getValueByName("app_prefix"), fsAppName, aPackageName);
			smartClientXML.save(printWriter, 1, aDataDoc);
			printWriter.printf(" <operationBindings>%n");
			printWriter.printf("  <binding operationId=\"exportData\" operationType=\"fetch\" serverMethod=\"exportData\">%n");
			printWriter.printf("   <serverObject  lookupStyle=\"new\" className=\"com.redis.app.redis_app_studio.%s.AppViewExportGridDS\"/>%n",
							   aPackageName);
			printWriter.printf("  </binding>%n");
			printWriter.printf(" </operationBindings>%n");
			printWriter.printf(" <generatedBy>RAS DataSource Generator Jan-06-2022</generatedBy>%n");
			printWriter.printf("</DataSource>%n");
		}
		catch (Exception e)
		{
			throw new IOException(dsPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated data source file: %s", dsPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, dsPathFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveDataModelerHTML()
		throws IOException
	{
		String dsHTML;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveDataModelerHTML");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appName = mAppGenDoc.getValueByName("app_name");
		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		String dsName1 = mAppGenDoc.getValueByName("gen_ds_1");
		String dsName2 = mAppGenDoc.getValueByName("gen_ds_2");
		String dsName3 = mAppGenDoc.getValueByName("gen_ds_3");
		if (StringUtils.isEmpty(dsName3))
		{
			if (StringUtils.isEmpty(dsName2))
				dsHTML = String.format(",%s", dsName1);
			else
				dsHTML = String.format(",%s,%s", dsName1, dsName2);
		}
		else
			dsHTML = String.format(",%s,%s,%s", dsName1, dsName2, dsName3);
		int gridHeight = mAppGenDoc.getValueAsInteger("grid_height");
		String httpURL = (String) appCtx.getProperty(Constants.APPCTX_PROPERTY_HTTP_URL);
		String baseURI = StringUtils.removeEnd(httpURL, "isomorphic/IDACall");
		String htmlFileName = String.format("%s.html", dsName1);
		String htmlPathFileName = deriveHTMLPathFileName(htmlFileName);
		try (PrintWriter printWriter = new PrintWriter(htmlPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<!DOCTYPE html>%n");
			printWriter.printf("<!--suppress HtmlUnknownTarget -->%n");
			printWriter.printf("<html lang=\"en-US\">%n");
			printWriter.printf("\t<head>%n");
			printWriter.printf("\t\t<title>%s</title>%n", appName);
			printWriter.printf("\t\t<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">%n");
			printWriter.printf("\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"css/RW.css\">%n");
			printWriter.printf("\t\t<link rel=\"shortuc icons\" href=\"images/rw-favicon.ico\" type=“image/x-icon”>%n");
			printWriter.printf("\t\t<link href=\"images/favicon.ico\" rel=\"icon\" type=\"image/x-icon\">%n");
			printWriter.printf("\t<!-- SmartClient JavaScript Runtime Framework -->%n");
			printWriter.printf("\t\t<script>var isomorphicDir=\"isomorphic/\";</script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Core.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Foundation.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Containers.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Grids.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Forms.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_DataBinding.js\"></script>%n");
			printWriter.printf("\t<!-- Pre-load Redis App Studio Data Sources -->%n");
			printWriter.printf("\t\t<script src=\"isomorphic/skins/%s/load_skin.js\"></script>%n", mAppGenDoc.getValueByName("skin_name"));
			printWriter.printf("\t\t<script src=\"isomorphic/DataSourceLoader?dataSource=DM-ApplicationGrid,DM-SuggestList,DM-SchemaGrid,DM-NodeSchemaGrid,DM-RelSchemaGrid,DM-DataFlatGrid,DM-DataHierGraph,DM-DataHierJSON,DM-DocumentGrid,DM-GenAppForm,DM-AnalyzeGrid%s\"></script>%n", dsHTML);
			printWriter.printf("\t<!-- Redis App Studio JavaScript Source Files -->%n");
			printWriter.printf("\t\t<script SRC=\"js/shared/Application.js\"></script>%n");
			printWriter.printf("\t\t<script SRC=\"js/dm/DataModeler.js\"></script>%n");
			printWriter.printf("\t</head>%n");
			printWriter.printf("\t<body style=\"overflow:hidden\">%n");
			printWriter.printf("\t\t<script>%n");
			if (StringUtils.isEmpty(dsName3))
				printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"%s\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").gridHeight(%d).loggingEnabled(false).build();%n",
								   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, mAppGenDoc.getValueByName("ds_structure"), dsName1, mAppGenDoc.getValueByName("ds_title"), gridHeight);
			else
			{
				String graphVisualizationURL = baseURI + "isomorphic/visualize/show";
				printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"%s\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").dsAppViewRel(\"%s\").dsAppViewRelOut(\"%s\").graphVisualizationURL(\"%s\").gridHeight(%d).loggingEnabled(false).build();%n",
								   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, mAppGenDoc.getValueByName("ds_structure"), dsName1, mAppGenDoc.getValueByName("ds_title"), dsName2, dsName3, graphVisualizationURL, gridHeight);
			}
			printWriter.printf("\t\t\twindow._appContext_ = appContext;%n");
			printWriter.printf("\t\t\tlet dataModeler = new DataModeler(appContext);%n");
			printWriter.printf("\t\t\tdataModeler.init();%n");
			printWriter.printf("\t\t\tdataModeler.show();%n");
			printWriter.printf("\t\t</script>%n");
			printWriter.printf("\t</body>%n");
			printWriter.printf("</html>%n");
		}
		catch (Exception e)
		{
			throw new IOException(htmlPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated HTML file: %s", htmlPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, htmlPathFileName);
		mAppGenDoc.setValueByName("gen_html", htmlPathFileName);
		String appURL = String.format("%s%s", baseURI, htmlFileName);
		mAppGenDoc.setValueByName("gen_link", appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_LINK, appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, htmlFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveRediSearchHashHTML()
		throws IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveRediSearchHashHTML");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appName = mAppGenDoc.getValueByName("app_name");
		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		String dsName = mAppGenDoc.getValueByName("gen_ds_1");
		int gridHeight = mAppGenDoc.getValueAsInteger("grid_height");
		String redisInsightURL = appCtx.getString("dm.app.redis_insight_url", Constants.REDIS_INSIGHT_URL_DEFAULT);
		String uiFacetsEnabled = StringUtils.equals(mAppGenDoc.getValueByName("ui_facets"), "Enabled") ? "true" : "false";
		String htmlFileName = String.format("%s.html", dsName);
		String htmlPathFileName = deriveHTMLPathFileName(htmlFileName);
		try (PrintWriter printWriter = new PrintWriter(htmlPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<!DOCTYPE html>%n");
			printWriter.printf("<!--suppress HtmlUnknownTarget -->%n");
			printWriter.printf("<html lang=\"en-US\">%n");
			printWriter.printf("\t<head>%n");
			printWriter.printf("\t\t<title>%s</title>%n", appName);
			printWriter.printf("\t\t<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">%n");
			printWriter.printf("\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"css/RW.css\">%n");
			printWriter.printf("\t\t<link rel=\"shortuc icons\" href=\"images/rw-favicon.ico\" type=“image/x-icon”>%n");
			printWriter.printf("\t\t<link href=\"images/favicon.ico\" rel=\"icon\" type=\"image/x-icon\">%n");
			printWriter.printf("\t<!-- SmartClient JavaScript Runtime Framework -->%n");
			printWriter.printf("\t\t<script>var isomorphicDir=\"isomorphic/\";</script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Core.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Foundation.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Containers.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Grids.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Forms.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_DataBinding.js\"></script>%n");
			printWriter.printf("\t<!-- Pre-load Redis App Studio Data Sources -->%n");
			printWriter.printf("\t\t<script src=\"isomorphic/skins/%s/load_skin.js\"></script>%n", mAppGenDoc.getValueByName("skin_name"));
			printWriter.printf("\t\t<script src=\"isomorphic/DataSourceLoader?dataSource=RSH-Database,RSH-SuggestList,RSH-SchemaGrid,RSH-DocumentGrid,RSH-FacetGrid,RSH-AppViewGrid,RSH-DocCmdGrid,%s\"></script>%n", dsName);
			printWriter.printf("\t<!-- Redis App Studio JavaScript Source Files -->%n");
			printWriter.printf("\t\t<script SRC=\"js/shared/Application.js\"></script>%n");
			printWriter.printf("\t\t<script SRC=\"js/rsh/RediSearchHash.js\"></script>%n");
			printWriter.printf("\t</head>%n");
			printWriter.printf("\t<body style=\"overflow:hidden\">%n");
			printWriter.printf("\t\t<script>%n");
			printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"Flat\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").redisInsightURL(\"%s\").facetUIEnabled(%s).gridHeight(%d).loggingEnabled(false).build();%n",
							   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, dsName, mAppGenDoc.getValueByName("ds_title"), redisInsightURL, uiFacetsEnabled, gridHeight);
			printWriter.printf("\t\t\twindow._appContext_ = appContext;%n");
			printWriter.printf("\t\t\tlet rediSearchHash = new RediSearchHash(appContext);%n");
			printWriter.printf("\t\t\trediSearchHash.init();%n");
			printWriter.printf("\t\t\trediSearchHash.show();%n");
			printWriter.printf("\t\t</script>%n");
			printWriter.printf("\t</body>%n");
			printWriter.printf("</html>%n");
		}
		catch (Exception e)
		{
			throw new IOException(htmlPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated HTML file: %s", htmlPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, htmlPathFileName);
		mAppGenDoc.setValueByName("gen_html", htmlPathFileName);
		String httpURL = (String) appCtx.getProperty(Constants.APPCTX_PROPERTY_HTTP_URL);
		String baseURI = StringUtils.removeEnd(httpURL, "isomorphic/IDACall");
		String appURL = String.format("%s%s", baseURI, htmlFileName);
		mAppGenDoc.setValueByName("gen_link", appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_LINK, appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, htmlFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveRediSearchJsonHTML()
		throws IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveRediSearchJsonHTML");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appName = mAppGenDoc.getValueByName("app_name");
		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		String dsName = mAppGenDoc.getValueByName("gen_ds_1");
		int gridHeight = mAppGenDoc.getValueAsInteger("grid_height");
		String redisInsightURL = appCtx.getString("dm.app.redis_insight_url", Constants.REDIS_INSIGHT_URL_DEFAULT);
		String uiFacetsEnabled = StringUtils.equals(mAppGenDoc.getValueByName("ui_facets"), "Enabled") ? "true" : "false";
		String htmlFileName = String.format("%s.html", dsName);
		String htmlPathFileName = deriveHTMLPathFileName(htmlFileName);
		try (PrintWriter printWriter = new PrintWriter(htmlPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<!DOCTYPE html>%n");
			printWriter.printf("<!--suppress HtmlUnknownTarget -->%n");
			printWriter.printf("<html lang=\"en-US\">%n");
			printWriter.printf("\t<head>%n");
			printWriter.printf("\t\t<title>%s</title>%n", appName);
			printWriter.printf("\t\t<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">%n");
			printWriter.printf("\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"css/RW.css\">%n");
			printWriter.printf("\t\t<link rel=\"shortuc icons\" href=\"images/rw-favicon.ico\" type=“image/x-icon”>%n");
			printWriter.printf("\t\t<link href=\"images/favicon.ico\" rel=\"icon\" type=\"image/x-icon\">%n");
			printWriter.printf("\t<!-- SmartClient JavaScript Runtime Framework -->%n");
			printWriter.printf("\t\t<script>var isomorphicDir=\"isomorphic/\";</script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Core.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Foundation.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Containers.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Grids.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Forms.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_DataBinding.js\"></script>%n");
			printWriter.printf("\t<!-- Pre-load Redis App Studio Data Sources -->%n");
			printWriter.printf("\t\t<script src=\"isomorphic/skins/%s/load_skin.js\"></script>%n", mAppGenDoc.getValueByName("skin_name"));
			printWriter.printf("\t\t<script src=\"isomorphic/DataSourceLoader?dataSource=RSJ-Database,RSJ-SuggestList,RSJ-SchemaGrid,RSJ-DocumentGrid,RSJ-FacetGrid,RSJ-AppViewGrid,RSJ-DocCmdGrid,%s\"></script>%n", dsName);
			printWriter.printf("\t<!-- Redis App Studio JavaScript Source Files -->%n");
			printWriter.printf("\t\t<script SRC=\"js/shared/Application.js\"></script>%n");
			printWriter.printf("\t\t<script SRC=\"js/rsj/RediSearchJson.js\"></script>%n");
			printWriter.printf("\t</head>%n");
			printWriter.printf("\t<body style=\"overflow:hidden\">%n");
			printWriter.printf("\t\t<script>%n");
			printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"Hierarchy\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").redisInsightURL(\"%s\").facetUIEnabled(%s).gridHeight(%d).loggingEnabled(false).build();%n",
							   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, dsName, mAppGenDoc.getValueByName("ds_title"), redisInsightURL, uiFacetsEnabled, gridHeight);
			printWriter.printf("\t\t\twindow._appContext_ = appContext;%n");
			printWriter.printf("\t\t\tlet rediSearchJson = new RediSearchJson(appContext);%n");
			printWriter.printf("\t\t\trediSearchJson.init();%n");
			printWriter.printf("\t\t\trediSearchJson.show();%n");
			printWriter.printf("\t\t</script>%n");
			printWriter.printf("\t</body>%n");
			printWriter.printf("</html>%n");
		}
		catch (Exception e)
		{
			throw new IOException(htmlPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated HTML file: %s", htmlPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, htmlPathFileName);
		mAppGenDoc.setValueByName("gen_html", htmlPathFileName);
		String httpURL = (String) appCtx.getProperty(Constants.APPCTX_PROPERTY_HTTP_URL);
		String baseURI = StringUtils.removeEnd(httpURL, "isomorphic/IDACall");
		String appURL = String.format("%s%s", baseURI, htmlFileName);
		mAppGenDoc.setValueByName("gen_link", appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_LINK, appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, htmlFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveRedisJsonHTML()
		throws IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveRedisJsonHTML");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appName = mAppGenDoc.getValueByName("app_name");
		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		String dsName = mAppGenDoc.getValueByName("gen_ds_1");
		int gridHeight = mAppGenDoc.getValueAsInteger("grid_height");
		String redisInsightURL = appCtx.getString("dm.app.redis_insight_url", Constants.REDIS_INSIGHT_URL_DEFAULT);
		String htmlFileName = String.format("%s.html", dsName);
		String htmlPathFileName = deriveHTMLPathFileName(htmlFileName);
		try (PrintWriter printWriter = new PrintWriter(htmlPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<!DOCTYPE html>%n");
			printWriter.printf("<!--suppress HtmlUnknownTarget -->%n");
			printWriter.printf("<html lang=\"en-US\">%n");
			printWriter.printf("\t<head>%n");
			printWriter.printf("\t\t<title>%s</title>%n", appName);
			printWriter.printf("\t\t<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">%n");
			printWriter.printf("\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"css/RW.css\">%n");
			printWriter.printf("\t\t<link rel=\"shortuc icons\" href=\"images/rw-favicon.ico\" type=“image/x-icon”>%n");
			printWriter.printf("\t\t<link href=\"images/favicon.ico\" rel=\"icon\" type=\"image/x-icon\">%n");
			printWriter.printf("\t<!-- SmartClient JavaScript Runtime Framework -->%n");
			printWriter.printf("\t\t<script>var isomorphicDir=\"isomorphic/\";</script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Core.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Foundation.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Containers.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Grids.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Forms.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_DataBinding.js\"></script>%n");
			printWriter.printf("\t<!-- Pre-load Redis App Studio Data Sources -->%n");
			printWriter.printf("\t\t<script src=\"isomorphic/skins/%s/load_skin.js\"></script>%n", mAppGenDoc.getValueByName("skin_name"));
			printWriter.printf("\t\t<script src=\"isomorphic/DataSourceLoader?dataSource=RJ-Database,RJ-AppViewGrid,RJ-DocCmdGrid,RJ-SchemaGrid,%s\"></script>%n", dsName);
			printWriter.printf("\t<!-- Redis App Studio JavaScript Source Files -->%n");
			printWriter.printf("\t\t<script SRC=\"js/shared/Application.js\"></script>%n");
			printWriter.printf("\t\t<script SRC=\"js/rj/RedisJson.js\"></script>%n");
			printWriter.printf("\t</head>%n");
			printWriter.printf("\t<body style=\"overflow:hidden\">%n");
			printWriter.printf("\t\t<script>%n");
			printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"Hierarchy\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").redisInsightURL(\"%s\").gridHeight(%d).loggingEnabled(false).build();%n",
							   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, dsName, mAppGenDoc.getValueByName("ds_title"), redisInsightURL, gridHeight);
			printWriter.printf("\t\t\twindow._appContext_ = appContext;%n");
			printWriter.printf("\t\t\tlet redisJson = new RedisJson(appContext);%n");
			printWriter.printf("\t\t\tredisJson.init();%n");
			printWriter.printf("\t\t\tredisJson.show();%n");
			printWriter.printf("\t\t</script>%n");
			printWriter.printf("\t</body>%n");
			printWriter.printf("</html>%n");
		}
		catch (Exception e)
		{
			throw new IOException(htmlPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated HTML file: %s", htmlPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, htmlPathFileName);
		mAppGenDoc.setValueByName("gen_html", htmlPathFileName);
		String httpURL = (String) appCtx.getProperty(Constants.APPCTX_PROPERTY_HTTP_URL);
		String baseURI = StringUtils.removeEnd(httpURL, "isomorphic/IDACall");
		String appURL = String.format("%s%s", baseURI, htmlFileName);
		mAppGenDoc.setValueByName("gen_link", appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_LINK, appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, htmlFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveRedisCoreHTML()
		throws IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveRedisCoreHTML");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appName = mAppGenDoc.getValueByName("app_name");
		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		String dsName = mAppGenDoc.getValueByName("gen_ds_1");
		int gridHeight = mAppGenDoc.getValueAsInteger("grid_height");
		String redisInsightURL = appCtx.getString("dm.app.redis_insight_url", Constants.REDIS_INSIGHT_URL_DEFAULT);
		String htmlFileName = String.format("%s.html", dsName);
		String htmlPathFileName = deriveHTMLPathFileName(htmlFileName);
		try (PrintWriter printWriter = new PrintWriter(htmlPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<!DOCTYPE html>%n");
			printWriter.printf("<!--suppress HtmlUnknownTarget -->%n");
			printWriter.printf("<html lang=\"en-US\">%n");
			printWriter.printf("\t<head>%n");
			printWriter.printf("\t\t<title>%s</title>%n", appName);
			printWriter.printf("\t\t<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">%n");
			printWriter.printf("\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"css/RW.css\">%n");
			printWriter.printf("\t\t<link rel=\"shortuc icons\" href=\"images/rw-favicon.ico\" type=“image/x-icon”>%n");
			printWriter.printf("\t\t<link href=\"images/favicon.ico\" rel=\"icon\" type=\"image/x-icon\">%n");
			printWriter.printf("\t<!-- SmartClient JavaScript Runtime Framework -->%n");
			printWriter.printf("\t\t<script>var isomorphicDir=\"isomorphic/\";</script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Core.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Foundation.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Containers.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Grids.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Forms.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_DataBinding.js\"></script>%n");
			printWriter.printf("\t<!-- Pre-load Redis App Studio Data Sources -->%n");
			printWriter.printf("\t\t<script src=\"isomorphic/skins/%s/load_skin.js\"></script>%n", mAppGenDoc.getValueByName("skin_name"));
			printWriter.printf("\t\t<script src=\"isomorphic/DataSourceLoader?dataSource=RC-Database,RC-AppViewGrid,RC-DocCmdGrid,RC-SchemaGrid,%s\"></script>%n", dsName);
			printWriter.printf("\t<!-- Redis App Studio JavaScript Source Files -->%n");
			printWriter.printf("\t\t<script SRC=\"js/shared/Application.js\"></script>%n");
			printWriter.printf("\t\t<script SRC=\"js/rc/RedisCore.js\"></script>%n");
			printWriter.printf("\t</head>%n");
			printWriter.printf("\t<body style=\"overflow:hidden\">%n");
			printWriter.printf("\t\t<script>%n");
			printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"Flat\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").redisInsightURL(\"%s\").gridHeight(%d).loggingEnabled(false).build();%n",
							   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, dsName, mAppGenDoc.getValueByName("ds_title"), redisInsightURL, gridHeight);
			printWriter.printf("\t\t\twindow._appContext_ = appContext;%n");
			printWriter.printf("\t\t\tlet redisCore = new RedisCore(appContext);%n");
			printWriter.printf("\t\t\tredisCore.init();%n");
			printWriter.printf("\t\t\tredisCore.show();%n");
			printWriter.printf("\t\t</script>%n");
			printWriter.printf("\t</body>%n");
			printWriter.printf("</html>%n");
		}
		catch (Exception e)
		{
			throw new IOException(htmlPathFileName + ": " + e.getMessage());
		}
		appLogger.debug(String.format("Generated HTML file: %s", htmlPathFileName));
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, htmlPathFileName);
		mAppGenDoc.setValueByName("gen_html", htmlPathFileName);
		String httpURL = (String) appCtx.getProperty(Constants.APPCTX_PROPERTY_HTTP_URL);
		String baseURI = StringUtils.removeEnd(httpURL, "isomorphic/IDACall");
		String appURL = String.format("%s%s", baseURI, htmlFileName);
		mAppGenDoc.setValueByName("gen_link", appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_LINK, appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, htmlFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void saveRedisGraphHTML(GraphDS aGraphDS)
		throws IOException
	{
		DataItem dataItem;
		ArrayList<String> vertexLabels, edgeTypes;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveRedisGraphHTML");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		vertexLabels = new ArrayList<>();
		vertexLabels.add("Undefined");
		DataDoc vertexSchemaDoc = aGraphDS.getVertexGridDS().getSchema();
		Optional<DataItem> optDataItem = vertexSchemaDoc.getItemByNameOptional(Data.GRAPH_VERTEX_LABEL_NAME);
		if (optDataItem.isPresent())
		{
			dataItem = optDataItem.get();
			if (dataItem.isRangeAssigned())
				vertexLabels = dataItem.getRange().getItems();
		}
		boolean isFirst = true;
		StringBuilder vStringBuilder = new StringBuilder();
		for (String vertexLabel : vertexLabels)
		{
			if (isFirst)
				isFirst = false;
			else
				vStringBuilder.append(StrUtl.CHAR_COMMA);
			vStringBuilder.append(StrUtl.CHAR_DBLQUOTE);
			vStringBuilder.append(vertexLabel);
			vStringBuilder.append(StrUtl.CHAR_DBLQUOTE);
		}
		edgeTypes = new ArrayList<>();
		edgeTypes.add("Undefined");
		DataDoc edgeSchemaDoc = aGraphDS.getEdgeGridDS().getSchema();
		optDataItem = edgeSchemaDoc.getItemByNameOptional(Data.GRAPH_EDGE_TYPE_NAME);
		if (optDataItem.isPresent())
		{
			dataItem = optDataItem.get();
			if (dataItem.isRangeAssigned())
				edgeTypes = dataItem.getRange().getItems();
		}
		isFirst = true;
		StringBuilder eStringBuilder = new StringBuilder();
		for (String edgeType : edgeTypes)
		{
			if (isFirst)
				isFirst = false;
			else
				eStringBuilder.append(StrUtl.CHAR_COMMA);
			eStringBuilder.append(StrUtl.CHAR_DBLQUOTE);
			eStringBuilder.append(edgeType);
			eStringBuilder.append(StrUtl.CHAR_DBLQUOTE);
		}

		String appName = mAppGenDoc.getValueByName("app_name");
		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		String dsName1 = mAppGenDoc.getValueByName("gen_ds_1");
		String dsName2 = mAppGenDoc.getValueByName("gen_ds_2");
		String dsName3 = mAppGenDoc.getValueByName("gen_ds_3");
		String dsName4 = mAppGenDoc.getValueByName("gen_ds_4");
		String dsName5 = mAppGenDoc.getValueByName("gen_ds_5");
		String dsHTML = String.format(",%s,%s,%s", dsName1, dsName2, dsName3);
		int gridHeight = mAppGenDoc.getValueAsInteger("grid_height");
		String redisInsightURL = appCtx.getString("dm.app.redis_insight_url", Constants.REDIS_INSIGHT_URL_DEFAULT);
		String htmlFileName = String.format("%s.html", dsName1);
		String htmlPathFileName = deriveHTMLPathFileName(htmlFileName);
		try (PrintWriter printWriter = new PrintWriter(htmlPathFileName, StandardCharsets.UTF_8))
		{
			printWriter.printf("<!DOCTYPE html>%n");
			printWriter.printf("<!--suppress HtmlUnknownTarget -->%n");
			printWriter.printf("<html lang=\"en-US\">%n");
			printWriter.printf("\t<head>%n");
			printWriter.printf("\t\t<title>%s</title>%n", appName);
			printWriter.printf("\t\t<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">%n");
			printWriter.printf("\t\t<link type=\"text/css\" rel=\"stylesheet\" href=\"css/RW.css\">%n");
			printWriter.printf("\t\t<link rel=\"shortuc icons\" href=\"images/rw-favicon.ico\" type=“image/x-icon”>%n");
			printWriter.printf("\t\t<link href=\"images/favicon.ico\" rel=\"icon\" type=\"image/x-icon\">%n");
			printWriter.printf("\t<!-- SmartClient JavaScript Runtime Framework -->%n");
			printWriter.printf("\t\t<script>var isomorphicDir=\"isomorphic/\";</script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Core.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Foundation.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Containers.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Grids.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_Forms.js\"></script>%n");
			printWriter.printf("\t\t<script src=\"isomorphic/system/modules/ISC_DataBinding.js\"></script>%n");
			printWriter.printf("\t<!-- Pre-load Redis App Studio Data Sources -->%n");
			printWriter.printf("\t\t<script src=\"isomorphic/skins/%s/load_skin.js\"></script>%n", mAppGenDoc.getValueByName("skin_name"));
			printWriter.printf("\t\t<script src=\"isomorphic/DataSourceLoader?dataSource=RG-Database,RG-SuggestList,RG-NodeSchemaGrid,RG-RelSchemaGrid,RG-AppViewGridRel,RG-AppViewGridRelOut,RG-DocCmdGrid,%s,%s,RG-AppViewGrid%s\"></script>%n", dsName4, dsName5, dsHTML);
			printWriter.printf("\t<!-- Redis App Studio JavaScript Source Files -->%n");
			printWriter.printf("\t\t<script SRC=\"js/shared/Application.js\"></script>%n");
			printWriter.printf("\t\t<script SRC=\"js/rg/RedisGraph.js\"></script>%n");
			printWriter.printf("\t</head>%n");
			printWriter.printf("\t<body style=\"overflow:hidden\">%n");
			printWriter.printf("\t\t<script>%n");
			printWriter.printf("\t\t\tlet appContext = new AppBuilder(\"%s\", \"%s\").version(\"1.0\").prefix(\"%s\").appType(\"Default\").dsStructure(\"Hierarchy\").dsStorage(\"Filesystem\").dsAppViewName(\"%s\").dsAppViewTitle(\"%s\").dsAppViewRel(\"%s\").dsAppViewRelOut(\"%s\").graphNodeLabels(%s).graphRelTypes(%s).redisInsightURL(\"%s\").gridHeight(%d).loggingEnabled(false).build();%n",
							   mAppGenDoc.getValueByName("app_group"), appName, appPrefix, dsName1, mAppGenDoc.getValueByName("ds_title"), dsName2, dsName3, vStringBuilder.toString(), eStringBuilder.toString(), redisInsightURL, gridHeight);
			printWriter.printf("\t\t\twindow._appContext_ = appContext;%n");
			printWriter.printf("\t\t\tlet redisGraph = new RedisGraph(appContext);%n");
			printWriter.printf("\t\t\tredisGraph.init();%n");
			printWriter.printf("\t\t\tredisGraph.show();%n");
			printWriter.printf("\t\t</script>%n");
			printWriter.printf("\t</body>%n");
			printWriter.printf("</html>%n");
		}
		catch (Exception e)
		{
			throw new IOException(htmlPathFileName + ": " + e.getMessage());
		}
		mAppManDoc.addValueByName(Constants.DS_STORAGE_DOCUMENT_FILES, htmlPathFileName);
		appLogger.debug(String.format("Generated HTML file: %s", htmlPathFileName));
		mAppGenDoc.setValueByName("gen_html", htmlPathFileName);
		String httpURL = (String) appCtx.getProperty(Constants.APPCTX_PROPERTY_HTTP_URL);
		String baseURI = StringUtils.removeEnd(httpURL, "isomorphic/IDACall");
		String appURL = String.format("%s%s", baseURI, htmlFileName);
		mAppGenDoc.setValueByName("gen_link", appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_LINK, appURL);
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_NAME, htmlFileName);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}



	private void prepareAppDocument()
		throws DSException, IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "prepareAppDocument");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		DataLoader dataLoader = new DataLoader(appCtx, Constants.DS_APPLICATIONS_PATH_NAME);
		String storageCSVPathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_APPLICATIONS_PATH_NAME, Constants.DS_STORAGE_DETAILS_NAME);
		mGridDS = new GridDS(appCtx);
		mGridDS.loadData(storageCSVPathFileName, true);
		DataDoc schemaDoc = mGridDS.getSchema();
		schemaDoc.getItemByName(Constants.DS_STORAGE_DOCUMENT_NAME).enableFeature(Data.FEATURE_IS_PRIMARY);
		schemaDoc.getItemByName(Constants.DS_STORAGE_DOCUMENT_FILES).enableFeature(Data.FEATURE_IS_HIDDEN);

		mAppManDoc = new DataDoc(schemaDoc);
		mAppManDoc.resetValues();
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_TITLE, mAppGenDoc.getValueByName("app_name"));
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_DESCRIPTION, mAppGenDoc.getValueByName("ds_title"));
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_TYPE, mAppGenDoc.getValueByName("app_type"));
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_DATE, new Date());
		mAppManDoc.setValueByName(Constants.DS_STORAGE_DOCUMENT_OWNER, "User");

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	private void storeAppDocument()
		throws DSException, IOException
	{
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "storeAppDocument");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		mGridDS.add(mAppManDoc);
		DataLoader dataLoader = new DataLoader(appCtx, Constants.DS_APPLICATIONS_PATH_NAME);
		String storageCSVPathFileName = dataLoader.deriveStoragePathFileName(Constants.DS_APPLICATIONS_PATH_NAME, Constants.DS_STORAGE_DETAILS_NAME);
		mGridDS.saveData(storageCSVPathFileName, true);

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	public void saveDataFlat(DataDoc aSchemaDoc)
		throws IOException, DSException
	{
		String errMsg;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveDataFlat");

		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		if (isPrefixAlreadyAssigned(appPrefix))
		{
			errMsg = String.format("Application prefix '%s' already exists.", appPrefix);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		prepareAppDocument();

// The generation logic keys off of the application type: Data Modeler, RediSearch and Redis Core.

		DataDoc schemaDoc = enrichSchemaDoc(aSchemaDoc);
		String appType = mAppGenDoc.getValueByName("app_type");
		switch (appType)
		{
			case "Data Modeler":
				saveAppViewDataSource(schemaDoc, "dm");
				saveDataModelerHTML();
				break;
			case "RediSearch":
				saveAppViewDataSource(schemaDoc, "rsh");
				saveRediSearchHashHTML();
				break;
			case "Redis Core":
				saveAppViewDataSource(schemaDoc, "rc");
				saveRedisCoreHTML();
				break;
			default:
				errMsg = String.format("Unsupported application type '%s' - cannot generate application.", appType);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
		}
		storeAppDocument();

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	public void saveDataJson(JsonDS aJsonDS)
		throws IOException, DSException
	{
		String errMsg;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveDataJson");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		if (isPrefixAlreadyAssigned(appPrefix))
		{
			errMsg = String.format("Application prefix '%s' already exists.", appPrefix);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		prepareAppDocument();

// The generation logic keys off of the application type: Data Modeler, RediSearch and Redis Core.

		DataDoc schemaDoc = enrichSchemaDoc(aJsonDS.getSchema());
		String appType = mAppGenDoc.getValueByName("app_type");
		switch (appType)
		{
			case "Data Modeler":
				saveAppViewDataSource(schemaDoc, "dm");
				saveDataModelerHTML();
				break;
			case "RedisJSON":
				saveAppViewDataSource(schemaDoc, "rj");
				saveRedisJsonHTML();
				break;
			case "RediSearch":
				saveAppViewDataSource(schemaDoc, "rsj");
				saveRediSearchJsonHTML();
				break;
			default:
				errMsg = String.format("Unsupported application type '%s' - cannot generate application.", appType);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
		}
		storeAppDocument();

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}

	public void saveDataGraph(GraphDS aGraphDS)
		throws IOException, DSException
	{
		String errMsg;
		AppCtx appCtx = mSessionContext.getAppCtx();
		Logger appLogger = appCtx.getLogger(this, "saveDataGraph");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

		String appPrefix = mAppGenDoc.getValueByName("app_prefix");
		if (isPrefixAlreadyAssigned(appPrefix))
		{
			errMsg = String.format("Application prefix '%s' already exists.", appPrefix);
			appLogger.error(errMsg);
			throw new DSException(errMsg);
		}
		prepareAppDocument();

// The generation logic keys off of the application type: Data Modeler, RediSearch and Redis Core.

		String appType = mAppGenDoc.getValueByName("app_type");
		switch (appType)
		{
			case "Data Modeler":
				saveAppViewDataSource(aGraphDS, "dm");
				saveDataModelerHTML();
				break;
			case "RedisGraph":
				saveAppViewDataSource(aGraphDS, "rg");
				saveRedisGraphHTML(aGraphDS);
				break;
			default:
				errMsg = String.format("Unsupported application type '%s' - cannot generate application.", appType);
				appLogger.error(errMsg);
				throw new DSException(errMsg);
		}
		storeAppDocument();

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}
}
