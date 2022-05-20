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

package ${maven_group_id}.${maven_artifact_id};

import com.redis.foundation.app.AppCtx;
import com.redis.foundation.std.FCException;
import org.slf4j.Logger;

public class Main
{
    private static final String APPLICATION_IDENTITY = "${maven_group_id}.${maven_artifact_id}.Main";

    public static void main(String[] anArgs)
    {
        AppCtx appCtx = new AppCtx();

        appCtx.setAbortHandlerEnabledFlag(true);
        appCtx.addTask(new ConsoleTask());
        try
        {
            appCtx.init(anArgs);
            Logger appLogger = appCtx.getLogger(APPLICATION_IDENTITY);
            appCtx.writeIdentity(appLogger);
            appCtx.execute();
        }
        catch (FCException e)
        {
            System.err.printf("%nAppCtx Error: %s%n", e.getMessage());
        }
        finally
        {
            appCtx.shutdown();
        }
    }
}

