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

package com.redis.app.redis_app_studio.servlet;

import com.redis.app.redis_app_studio.shared.*;
import com.redis.ds.ds_graph.GraphDS;
import com.redis.ds.ds_redis.graph.RedisGraphs;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.data.*;
import com.redis.foundation.ds.DSException;
import com.redis.foundation.io.DataDocLogger;
import com.redis.foundation.std.FCException;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Graph visualization servlet responsible for rendering a graph
 * based on the current data graph instance.
 */
@SuppressWarnings("FieldCanBeLocal")
public class GraphVisualizationServlet extends HttpServlet
{
	private final String HTML_CONTENT_TYPE = "text/html";
	private final String FILE_CONTENT_TYPE = "application/octet-stream";
	private final String FILE_DOWNLOAD_NAME = "GraphVisualization.html";
	private final String APPLICATION_PROPERTIES_PREFIX = "dm";
	private final String CLASS_NAME = "GraphVisualizationServlet";

	/**
	 * Write an HTML error message to the user's web browser.
	 *
	 * @param aResponse Servlet response instance (used for output stream).
	 * @param aMessage Message to show.
	 * @throws IOException Thrown when an I/O error is detected.
	 */
	private void errorMessage(HttpServletResponse aResponse, String aMessage)
		throws IOException
	{
		PrintWriter pwOut = aResponse.getWriter();
		aResponse.setContentType(HTML_CONTENT_TYPE);
		pwOut.printf("<html>%n");
		pwOut.printf("<head>%n");
		pwOut.printf("<style type=\"text/css\">%n");
		pwOut.printf("p {border: medium dodgerblue solid;}%n");
		pwOut.printf("</style>%n");
		pwOut.printf("<title>Graph Visualization Error</title>%n");
		pwOut.printf("</head>%n");
		pwOut.printf("<body>%n");
		pwOut.printf("<p>%s</p>%n", aMessage);
		pwOut.printf("</body>%n");
		pwOut.printf("</html>%n");
		pwOut.flush();
	}

	@Override
	public void doGet(HttpServletRequest aRequest, HttpServletResponse aResponse)
		throws ServletException, IOException
	{
		String htmlGraph;
		DataGraph dataGraph;
		boolean isDataModeler;
		DataGraph dataGraphSR;
		AppSession appSession;
		AppResource appResource;

// Get handles to Request and Session related object instances and initialize session.

		SessionContext sessionContext = new SessionContext(CLASS_NAME, aRequest);
		AppCtx appCtx = sessionContext.establishAppCtx(APPLICATION_PROPERTIES_PREFIX);

// Get a handle to the application logger instance.

		Logger appLogger = appCtx.getLogger(this, "doGet");
		appLogger.trace(appCtx.LOGMSG_TRACE_ENTER);

// Create/establish our application session.

		DataDoc urlParamsDoc = sessionContext.requestToDataDoc(aRequest);
//		DataDocLogger dataDocLogger = new DataDocLogger(appLogger);
//		dataDocLogger.writeSimple(urlParamsDoc);
		Optional<DataItem> optDataItem = urlParamsDoc.getItemByNameOptional("is_modeler");
		isDataModeler = optDataItem.map(DataItem::isValueTrue).orElse(false);
		if (isDataModeler)
			appSession = new AppSession.Builder().context(sessionContext).targetMemory().build();
		else
			appSession = new AppSession.Builder().context(sessionContext).targetRedisGraph().build();
		try
		{
			appResource = appSession.restore();
			dataGraphSR = appResource.getResultDataGraph();
		}
		catch (Exception e)
		{
			throw new ServletException(e.getMessage());
		}
		GraphDS graphDS = appResource.getGraphDS();

// Let's build our options document and visualize the graph back as an HTML stream.

		optDataItem = urlParamsDoc.getItemByNameOptional("is_download");
		boolean isDownloadOperation = optDataItem.map(DataItem::isValueTrue).orElse(false);
		optDataItem = urlParamsDoc.getItemByNameOptional("is_matched");
		boolean isGraphMatched = optDataItem.map(DataItem::isValueTrue).orElse(false);
		DataDoc graphOptionsDoc = graphDS.createOptionsDefault();
		for (DataItem dataItem : urlParamsDoc.getItems())
			graphOptionsDoc.setValueByName(dataItem.getName(), dataItem.getValue());
		if (isDownloadOperation)
		{
			graphOptionsDoc.setValueByName("ui_width", "1200px");
			graphOptionsDoc.setValueByName("ui_height", "1000px");
		}

		try
		{
			if (isDataModeler)
				dataGraph = graphDS.createDataGraph(graphDS.getName());
			else
			{
				RedisGraphs redisGraph = appResource.getRedisGraph();
				dataGraph = redisGraph.queryAll(appResource.getTitle());
				appLogger.debug(String.format("%s: RedisGraph data graph has %d vertexes and %d edges.", dataGraph.getName(), dataGraph.getVertexDocSet().size(), dataGraph.getEdgeSet().size()));
			}
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			if ((isGraphMatched) && (dataGraphSR != null))
			{
				htmlGraph = graphDS.visualizeToString(dataGraph, dataGraphSR, graphOptionsDoc);
				appLogger.debug(String.format("%s: Visualizing data graph (%d v, %d e) with matches against (%d v %d e).", dataGraph.getName(),
											  dataGraph.getVertexDocSet().size(), dataGraph.getEdgeSet().size(),
											  dataGraphSR.getVertexDocSet().size(), dataGraphSR.getEdgeSet().size()));
			}
			else
			{
				htmlGraph = graphDS.visualizeToString(dataGraph, graphOptionsDoc);
				appLogger.debug(String.format("%s: Visualizing data graph (%d v, %d e) without matches.", dataGraph.getName(),
											  dataGraph.getVertexDocSet().size(), dataGraph.getEdgeSet().size()));
			}
			if (isDownloadOperation)
			{
				aResponse.setContentType(FILE_CONTENT_TYPE);
				aResponse.setContentLength(htmlGraph.length());
				String headerKey = "Content-Disposition";
				String headerValue = String.format("attachment; filename=\"%s\"", FILE_DOWNLOAD_NAME);
				aResponse.setHeader(headerKey, headerValue);
				OutputStream outStream = aResponse.getOutputStream();
				byte[] byteArrray = htmlGraph.getBytes(StandardCharsets.UTF_8);
				outStream.write(byteArrray, 0, byteArrray.length);
				outStream.close();
			}
			else
			{
				PrintWriter pwOut = aResponse.getWriter();
				aResponse.setContentType(HTML_CONTENT_TYPE);
				pwOut.printf("%s%n", htmlGraph);
				pwOut.flush();
				stopWatch.stop();
			}
			int edgeCount = graphDS.getEdgeGridDS().getDataGrid().rowCount();
			int vertexCount = graphDS.getVertexGridDS().getDataGrid().rowCount();
			appLogger.debug(String.format("'%s': %d vertexes and %d edges visualized in %d milliseconds.", dataGraph.getName(),
										  vertexCount, edgeCount, stopWatch.getTime()));
		}
		catch (FCException e)
		{
			appLogger.error(e.getMessage());
			errorMessage(aResponse, e.getMessage());
		}

		appLogger.trace(appCtx.LOGMSG_TRACE_DEPART);
	}
}
