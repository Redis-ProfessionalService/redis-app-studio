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

import com.isomorphic.datasource.DSRequest;
import com.isomorphic.rpc.RPCManager;
import com.isomorphic.servlet.RequestContext;
import com.isomorphic.servlet.ServletTools;
import com.redis.ds.ds_content.ContentType;
import com.redis.ds.ds_grid.GridDS;
import com.redis.ds.ds_redis.RedisDS;
import com.redis.foundation.app.AppCtx;
import com.redis.foundation.app.CfgMgr;
import com.redis.foundation.data.Data;
import com.redis.foundation.data.DataDoc;
import com.redis.foundation.data.DataGrid;
import com.redis.foundation.data.DataItem;
import com.redis.foundation.ds.DSCriteria;
import com.redis.foundation.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

/**
 * The SessionContext manages the lifecycle of a Servlet session.
 */
public class SessionContext
{
	public final String ACCOUNT_NAME_UNKNOWN = "Unknown";
	public final String ACCOUNT_PASSWORD_EMPTY = StringUtils.EMPTY;

	private AppCtx mAppCtx;
	private CfgMgr mCfgMgr;
	private HttpSession mSession;
	private final String mClassName;
	private final String mSessionMessage;
	private final HttpServletRequest mServletRequest;

	/**
	 * Constructor that establishes a session context from the Servlet Request.
	 *
	 * @param aRequest Servlet Request
	 */
	public SessionContext(String aClassName, HttpServletRequest aRequest)
	{
		mClassName = aClassName;
		mServletRequest = aRequest;

// A Servlet session should already be established by the SmartClient framework, so
// this we should see restored when this is called.

		mSession = mServletRequest.getSession(false);
		if (mSession == null)
		{
			mSession = mServletRequest.getSession(true);
			mSessionMessage = String.format("Session created: %s", mSession.getId());
		}
		else
			mSessionMessage = String.format("Session restored: %s", mSession.getId());
	}

	/**
	 * Invalidates the current session - typically used when a user logs out
	 * or a new application is generated.
	 */
	public void resetSession()
	{
		if (mSession != null)
			mSession.invalidate();
	}

	private AppCtx createAppCtx()
	{
		String cfgPathName = System.getProperty("user.dir");
		String insPathName = System.getProperty("catalina.home");
		String logPathName = String.format("%s%clog", insPathName, File.separatorChar);
		String dsPathName = String.format("..%c..%cds", File.separatorChar, File.separatorChar);
		String rdbPathName = String.format("..%c..%crdb", File.separatorChar, File.separatorChar);
		String datPathName = String.format("..%c..%cdata", File.separatorChar, File.separatorChar);

		SCLogger scLogger = new SCLogger();
		ServletContext servletContext = mSession.getServletContext();
		String realPathName = servletContext.getRealPath("/");
		if (StringUtils.isNotEmpty(realPathName))
		{
			File realFile = new File(realPathName);
			try
			{
				String rfcPathName = realFile.getCanonicalPath();
				if (StrUtl.endsWithChar(rfcPathName, File.separatorChar))
					rfcPathName = StringUtils.chop(rfcPathName);
				cfgPathName = String.format("%s%cWEB-INF%cclasses", rfcPathName,
											File.separatorChar, File.separatorChar);
				dsPathName = String.format("%s%cds", rfcPathName, File.separatorChar);
				rdbPathName = String.format("%s%crdb", rfcPathName, File.separatorChar);
				datPathName = String.format("%s%cdata", rfcPathName, File.separatorChar);
			}
			catch (IOException e)
			{
				scLogger.error("createAppCtx Exception (dsPathName): " + e.getMessage());
			}
		}
		scLogger.debug(String.format("insPath = %s, cfgPath = %s, logPath = %s, dsPath = %s, datPath = %s, rdbPath = %s",
									  insPathName, cfgPathName, logPathName, dsPathName, datPathName, rdbPathName));
		AppCtx appCtx = new AppCtx(insPathName, cfgPathName, logPathName, dsPathName, rdbPathName, datPathName);
		try
		{
			appCtx.loadPropertyFiles();
		}
		catch (Exception e)
		{
			scLogger.error("AppMgr Exception (loadPropertyFiles): " + e.getMessage());
		}

		return appCtx;
	}

	/**
	 * Establishes an Application Context from a previous session (e.g. cookie with JSESSIONID
	 * set) or creates a new one for the session.
	 *
	 * @param anAppPrefix Application prefix string (used for property name lookups)
	 *
	 * @return Application Context instance
	 */
	public AppCtx establishAppCtx(String anAppPrefix)
	{

//	Establish or create our Application Context - "web.xml" 60 minute default timeout.

		mAppCtx = (AppCtx) mSession.getAttribute(Constants.SESSION_PROPERTY_APPCTX);
		if (mAppCtx == null)
		{
			mAppCtx = createAppCtx();
			mSession.setAttribute(Constants.SESSION_PROPERTY_APPCTX, mAppCtx);
		}

		Logger appLogger = mAppCtx.getLogger(this, "initialize");

		appLogger.trace(mAppCtx.LOGMSG_TRACE_ENTER);
		appLogger.info(String.format("%s: %s", mClassName, mSessionMessage));

// Create our Configuration Manager for property file lookups.

		if (StringUtils.isEmpty(anAppPrefix))
		{
			appLogger.error(String.format("Application prefix string is null - using default of '%s'", Constants.APPLICATION_PREFIX_DEFAULT));
			mCfgMgr = new CfgMgr(mAppCtx, Constants.APPLICATION_PREFIX_DEFAULT);
		}
		else
		{
			appLogger.debug(String.format("%s: Application prefix is '%s'", mClassName, anAppPrefix));
			mCfgMgr = new CfgMgr(mAppCtx, anAppPrefix);
		}

// Assign Servlet Request properties to our Application Context.

		mAppCtx.addProperty(Constants.APPCTX_PROPERTY_SESSION_ID, mSession.getId());

		String remoteUser = mServletRequest.getRemoteUser();
		if (StringUtils.isNotEmpty(remoteUser))
		{
			int offset = remoteUser.indexOf(StrUtl.CHAR_BACKSLASH);
			if (offset == -1)
				mAppCtx.addProperty(Constants.APPCTX_PROPERTY_REMOTE_USER, remoteUser);
			else
				mAppCtx.addProperty(Constants.APPCTX_PROPERTY_REMOTE_USER, remoteUser.substring(offset + 1));
		}

		String remoteAddress = mServletRequest.getRemoteAddr();
		if (StringUtils.isNotEmpty(remoteAddress))
			mAppCtx.addProperty(Constants.APPCTX_PROPERTY_REMOTE_ADDR, remoteAddress);
		String referURL = mServletRequest.getHeader("Referer");
		if (StringUtils.isNotEmpty(referURL))
			mAppCtx.addProperty(Constants.APPCTX_PROPERTY_REFERER_URL, referURL);
		String pathURL = mServletRequest.getPathInfo();
		if (StringUtils.isNotEmpty(pathURL))
			mAppCtx.addProperty(Constants.APPCTX_PROPERTY_PATH_URL, pathURL);
		String httpURL = mServletRequest.getRequestURL().toString();
		if (StringUtils.isNotEmpty(httpURL))
			mAppCtx.addProperty(Constants.APPCTX_PROPERTY_HTTP_URL, httpURL);

		appLogger.trace(mAppCtx.LOGMSG_TRACE_DEPART);

		return mAppCtx;
	}

	public String constructURIFromRequest(HttpServletRequest aRequest)
	{
		return aRequest.getScheme() + "://" + aRequest.getServerName() +
				("http".equals(aRequest.getScheme()) && aRequest.getServerPort() == 80 ||
						 "https".equals(aRequest.getScheme()) && aRequest.getServerPort() == 443 ? "" : ":" + aRequest.getServerPort() ) +
				aRequest.getRequestURI() + (aRequest.getQueryString() != null ? "?" + aRequest.getQueryString() : "");
	}

	private String encryptAccountPassword(String aPassword)
	{
		// ToDo: Develop a simple rotation encryption scheme that a JavaScript client could support.

		return aPassword;
	}

	public void saveCookies(DSRequest aRequest, String anAccountName, String anAccountPassword)
	{
		if (aRequest != null)
		{
			RPCManager rpcManager = aRequest.getRPCManager();
			RequestContext requestContext = rpcManager.getContext();
			if (StringUtils.isNotEmpty(anAccountName))
			{
				ServletTools.setCookie(requestContext, Constants.COOKIE_ACCOUNT_NAME, anAccountName, -1);
				if (StringUtils.isNotEmpty(anAccountPassword))
					ServletTools.setCookie(requestContext, Constants.COOKIE_ACCOUNT_PASSWORD, encryptAccountPassword(anAccountPassword), -1);
			}
		}
	}

// You must call establishAppCtx() before using this method.

	public String getAccountName(DSCriteria aDSCriteria)
	{
		String accountName = mCfgMgr.getString("account_name");
		if (StringUtils.isEmpty(accountName))
		{
			accountName = mAppCtx.getString(Constants.APPCTX_PROPERTY_REMOTE_USER);
			if ((StringUtils.isEmpty(accountName)) && (aDSCriteria != null))
				accountName = aDSCriteria.getFeature(Constants.FEATURE_ACCOUNT_NAME);
		}

		if (StringUtils.isEmpty(accountName))
			accountName = ACCOUNT_NAME_UNKNOWN;

		return accountName;
	}

// You must call establishAppCtx() before using this method.

	public String getAccountPassword(DSCriteria aDSCriteria)
	{
		String accountPassword = mCfgMgr.getString("account_password");
		if (StringUtils.isEmpty(accountPassword))
		{
			if (aDSCriteria != null)
				accountPassword = aDSCriteria.getFeature(Constants.FEATURE_ACCOUNT_PASSWORD);
		}

		if (StringUtils.isEmpty(accountPassword))
			accountPassword = ACCOUNT_PASSWORD_EMPTY;

		return accountPassword;
	}

// Accessor methods.

	public AppCtx getAppCtx()
	{
		return mAppCtx;
	}
	public CfgMgr getCfgMgr()
	{
		return mCfgMgr;
	}

	public String getId()
	{
		return mSession.getId();
	}

	public String getApplicationPrefix()
	{
		return mCfgMgr.getPrefix();
	}

	public RedisDS getRedisDS()
	{
		return (RedisDS) mAppCtx.getProperty(Constants.APPCTX_PROPERTY_DS_REDIS);
	}

	public GridDS getGridDS()
	{
		return (GridDS) mAppCtx.getProperty(Constants.APPCTX_PROPERTY_DS_GRID);
	}

	public ContentType getContentType()
	{
		return (ContentType) mAppCtx.getProperty(Constants.APPCTX_PROPERTY_CONTENT_TYPE);
	}

// Utility methods.

	@SuppressWarnings({"unchecked","rawtypes"})
	public DataDoc requestToDataDoc(HttpServletRequest aRequest)
	{
		String paramName;
		String[] paramValues;

		DataDoc dataDoc = new DataDoc("URI Parameters");
		Enumeration paramNames = aRequest.getParameterNames();
		if (paramNames != null)
		{
			while (paramNames.hasMoreElements())
			{
				paramName = (String) paramNames.nextElement();
				paramValues = aRequest.getParameterValues(paramName);
				if ((paramValues != null) && (paramValues.length > 0))
					dataDoc.add(new DataItem.Builder().name(paramName).values(paramValues).build());
			}
		}

		return dataDoc;
	}

	public DataGrid cookiesToDataGrid(HttpServletRequest aRequest)
	{
		DataGrid cookieDataGrid = new DataGrid("Servlet Cookies");
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_name").title("Cookie Name").build());
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_value").title("Cookie Value").build());
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_comment").title("Cookie Comment").build());
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_domain").title("Cookie Domain").build());
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_version").title("Cookie Version").build());
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_max_age").title("Cookie Max Age").build());
		cookieDataGrid.addCol(new DataItem.Builder().name("cookie_path_uri").title("Cookie Path URI").build());
		cookieDataGrid.addCol(new DataItem.Builder().type(Data.Type.Boolean).name("cookie_is_secure").title("Cookie Is Secure").build());

		Cookie[] cookieList = aRequest.getCookies();
		if (cookieList != null)
		{
			for (Cookie cookie : cookieList)
			{
				cookieDataGrid.newRow();
				cookieDataGrid.setValueByName("cookie_name", cookie.getName());
				cookieDataGrid.setValueByName("cookie_value", cookie.getValue());
				cookieDataGrid.setValueByName("cookie_domain", cookie.getDomain());
				cookieDataGrid.setValueByName("cookie_path_uri", cookie.getPath());
				cookieDataGrid.setValueByName("cookie_comment", cookie.getComment());
				cookieDataGrid.setValueByName("cookie_max_age", String.format("%d", cookie.getMaxAge()));
				cookieDataGrid.setValueByName("cookie_version", String.format("%d", cookie.getVersion()));
				cookieDataGrid.setValueByName("cookie_is_secure", cookie.getSecure());
				cookieDataGrid.addRow();
			}
		}

		return cookieDataGrid;
	}
}
