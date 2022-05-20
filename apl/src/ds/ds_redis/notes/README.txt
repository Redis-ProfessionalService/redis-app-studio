
Redis Development Notes
=======================

Redis Time Series
-----------------
- You confirmed that sample counts for range queries with aggregations do
  not work and you added workaround logic for that situation.
  TS.RANGE "ASRT:RT:TS:DD:MN:nasdaq:ebay:price_open" 0 1649072437860 AGGREGATION COUNT 25
- The performance of adding a DataGrid is poor and you will need to add
  support for pipelinining when you start the App Studio Tiime Series
  project.

RedisGraph Notes
----------------
 Cypher Examples:
  GRAPH.QUERY 'RG:RG:GP:DD:MN:Internet Movie Database' 'MATCH (n)-[r]-() RETURN n,r'
   Returns all nodes and relationships
  GRAPH.QUERY 'ASRG:RG:GP:DD:MN:IMDb Graph' "CREATE (:Principal {common_id:'nm12345',common_name:'Al Cole',common_summary:'VP of PS',principal_birth_year:1963,principal_professions:'Developer',principal_is_male:true,principal_height:72,principal_eye_color:'Blue'})"
   Creates a new unconnected node
  GRAPH.QUERY 'ASRG:RG:GP:DD:MN:IMDb Graph' 'MATCH (n) RETURN n'
   Returns just nodes

RedisJSON Notes
---------------
- Once the unitifed framework is stable, revisit the update method with the goal
  of handling deep updates in the JSON.
  + You need to enhance your update() method to update all fields in the schema
    are updated based on the JSON path expression.  This will ensure that the
    full JSON object remains in the database after it is updated.
    > JSON.SET "TEST" $ '{"one":1, "two": "string"}'
    > JSON.GET "TEST"
      {"one":1,"two":"string"}
    > JSON.SET "TEST" '$.two' '"updated"'
    > JSON.GET "TEST"
      {"one":1,"two":"updated"}
    NOTE: You created logic to handle selective JSON path field updates,
          but the JRedisJSON framework has a Path.of("jsonPath") method
          that does not support the syntax properly.  Retest this when
          the library is updated
- Useful 'redis-cli' Commands
  $ ./redis-cli --raw
  > JSON.GET "KeyName" INDENT "\t" NEWLINE "\n" SPACE " "
  > ZRANGE "KeyName" 0 -1 WITHSCORES

Redis (All Modules) Docker Image
--------------------------------

 #!/bin/bash
 #
 # https://hub.docker.com/r/redislabs/redismod
 # https://docs.docker.com/engine/reference/commandline/cli/
 # https://www.tecmint.com/run-docker-container-in-background-detached-mode/
 #
 echo "Starting the Redis Modules container in the background ..."
 docker run -d --rm -p 6379:6379 --name redismod redislabs/redismod:preview
 #docker run -d --rm -p 6379:6379 --name redismod redislabs/redismod:latest
 #docker run -d --rm -p 6379:6379 --name redismod redislabs/redismod:edge
 echo "Use 'redis-cli shutdown' to stop the service"
 exit 0

IntelliJ Debugger Settings
--------------------------

After you upgraded to the JDK 17 release, you noticed that IntelliJ reported
the following error message whenever a debug session was invoked.

"Java HotSpot(TM) 64-Bit Server VM warning: Sharing is only supported for
boot loader classes because bootstrap classpath has been appended"

https://stackoverflow.com/questions/54205486/how-to-avoid-sharing-is-only-supported-for-boot-loader-classes-because-bootstra

To fix this, you selected "IntelliJIDEA->Preferences..." and executed a
search for "async" and led you to:
"Build, Execution, Deployment -> Debugger -> Async Stack Traces"
Deselect "Instrumenting agent (requires debugger restart)" and
that corrected the issue.

