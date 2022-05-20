
Redis App Studio README
=======================

Application Focus List
----------------------

01) Move "GitHub->dev-env->apache-tomcat-9.0.34" to under 'tomcat'.  This will
    impact "bootstrap.sh".

App Studio Test Coverage
------------------------
 04/16 (RedisJson.html) - All grid navigation and toolbar operations were validated.
 04/19 (RedisCore.html) - All grid navigation and toolbar operations were validated.
 04/21 (RediSearch-Hash.html) - All grid navigation and toolbar operations were validated.
 04/23 (RediSearch-Json.html) - All grid navigation and toolbar operations were validated.
 04/30 (DataModeler.html) - All grid navigation and toolbar operations were validated.
 04/30 (DataGraph.html) - All grid navigation and toolbar operations were validated.
 05/07 (RedisGraph.html) - Good enough for demonstrations - notes capture remaining gaps.

RedisCore
---------
- Mission accomplished.

RedisJSON
---------
- Mission accomplished.

RediSearch
----------
- Date searching works with absolute dates (nothing relative).
- You did not exercise the index rebuild feature off the schema field
  due to the end of May deadline.
- Jedis 4.2.1 does not support the Suggest database object, so you needed
  to implement a GridDS alternative for the feature.
  + An alternative would be to exectue suggestions as searches using
    TEXT NOSTEM fields with a wildcard like: FT.SEARCH 'loans'
    "'@sub_date:[0 16540560000.0] @borrower_name:'Jo*'" RETURN 1
    'JsonId as ID'
- You could not get the term Highlighting feature to work when you used
  schema field aliasing, so you implemented a rudimentary one when you
  generate an SC payload response object.  All these failed (they worked
  when you were using JRediSearch:
  > FT.SEARCH RS:RS:SI:DD:MN:human_resources_employees_records_1k business HIGHLIGHT FIELDS 7 full_name position_title office_location industry_focus city state region TAGS "<b>" "</b>" LIMIT 0 1
  > FT.SEARCH RS:RS:SI:DD:MN:human_resources_employees_records_1k business HIGHLIGHT FIELDS 7 full_name_stemmed position_title_stemmed office_location_stemmed industry_focus_stemmed city_stemmed state_stemmed region_stemmed TAGS "<b>" "</b>" LIMIT 0 1
  > FT.SEARCH RS:RS:SI:DD:MN:human_resources_employees_records_1k business HIGHLIGHT FIELDS 7 full_name_text position_title_text office_location_text industry_focus_text city_text state_text region_text TAGS "<b>" "</b>" LIMIT 0 1
  > FT.SEARCH RS:RS:SI:DD:MN:human_resources_employees_records_1k business HIGHLIGHT FIELDS 7 full_name_stemmed position_title_stemmed office_location_stemmed industry_focus_stemmed city_stemmed state_stemmed region_stemmed  LIMIT 0 1

RedisGraph
----------
- Your design maintains and in-memory graph and a database graph with your logic
  keeping the in-sync in the AppViewGridDS class.  You might transition to 100%
  of the database, but that will impact your RedisGraphs.queryAll() method
  because it only loads nodes that have relationships linked to them.
- Your use of isGraphLabel="true" with the vertext schema is inconsistent and when
  you tried to focus on just "common_name" with the feature, it broke the graph
  logic.  You can get this consistent if you started with 'ds_graph', then
  'redis_graph' and finally 'redis_app_studio'.
- You wired up the node and relationship schema logic on the server, but the updates
  do not work properly yet and you need to recognize isFullTextSearch and
  isPropertySearch features in the UI
- Need to enhance the framework and UI to support the new RedisGraph 2.8 features
  discussed here: https://redis.com/blog/redisgraph-2-8-is-generally-available/
- In "ApplicationGenerator.java" at line 144, you have "Role" column header
  assigned using hacked code that needs to be generalized via a schema
  definition document.

RedisTimeSeries
---------------
- This will follow the approach you did for RedisGraph with the use of a
  parent row (containing labels) and expansion row containing the time
  series data.
  + Remember how RedisInsight (1.x) redendered its charts
- You will need to create a new data source package 'TimeSeriesDS' that
  handles the raw data load, data doc of labels and possibly and in-memory
  queries (e.g. GridDS).  Look at 'GraphDS' to see if some of those ideas
  can be borrowed.

App Studio Colors & Icons
-------------------------
   Standard web color names here: https://htmlcolorcodes.com/color-names/
 Free image icons available here: https://www.iconninja.com/

 A grid of data can be transitioned into a tile presentation if you have
 supporting images for each row as shown in this demonstration.

  https://www.smartclient.com/smartclient/showcase/?id=tilingFilterFS

SmartClient & TypeScript
------------------------
 You will need to copy the TypeScript definition file to your IntelliJ project
 for customization on that project.

 $ cp $SC_HOME/smartclientSDK/isomorphic/system/development/smartclient.d.ts .

 Unlike the SmartClient 12.1 release, you did not need to manually modify this
 file to remove warning messages.

Tomcat Release Support
----------------------
 SmartClient will not support Apache Tomcat 10.x releases.  You must stay with
 'apache-tomcat-9.0.34' for now.  After some research, you discovered it was
 because of code-breaking changes to "javax.servlet.*" packages - those need
 to be renamed due to licensing issues.  SmartClient embeds those calls into
 it's server side Java code.

IntelliJ Debugger Settings
--------------------------
 After you upgraded to the JDK 17 release, you noticed that IntelliJ reported
 the following error message whenever a debug session was invoked.

 "Java HotSpot(TM) 64-Bit Server VM warning: Sharing is only supported for
 boot loader classes because bootstrap classpath has been appended"

 https://stackoverflow.com/questions/54205486/how-to-avoid-sharing-is-only-supported-for-boot-loader-classes-because-bootstra

 To fix this, you selected "IntelliJIDEA->Preferences..." and executed a
 search for "async" and that led you to:

 - "Build, Execution, Deployment -> Debugger -> Async Stack Traces"
 - Deselect "Instrumenting agent (requires debugger restart)" and
   that corrected the issue.

