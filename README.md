# Redis App Studio

Redis App Studio is a suite of demonstration applications designed to showcase the features of the core Redis database and its modules. It consists of 7 distinct applications including: Application Launcher, Data Modeler, Redis Core, RediSearch (Hash) Redis JSON, RediSearch (JSON) and Redis Graph.  It was developed using the Java and TypeScript/JavaScript programming languages.

## Redis App Studio Framework

Consists of foundation classes that are utilized throughout the application stack, data source classes which provide CRUD+S operations for the Redis database and its modules and the UI application classes (client/server) that drive the presentation layer of the Redis App Studio application suite.

### Foundation Classes

Collection of foundational classes for application and data management.

- **com.redis.foundation.app** - Application management classes (sessions, threads, logging, properties).
- **com.redis.foundation.crypt** - Data hiding utility classes (encryption/decryption).
- **com.redis.foundation.data** - Data object classes (item, document, grid, graph).
- **com.redis.foundation.ds** - Data source management classes (criteria).
- **com.redis.foundation.io** - Data object storage and retrieval classes (XML, JSON, CSV).
- **com.redis.foundation.mail** - Email generation classes (SMTP).
- **com.redis.foundation.std** - Standard utility classes (strings, numerics, files, message hashing).

### Data Source Content Classes

Collection of data source content parsing classes.

- **Content** - The Content class captures the constants, enumerated types and utility methods for the content text extraction package.
- **ContentClean** - The ContentClean class provides utility methods for cleaning data streams.
- **ContentParser** - The ContentParser class is responsible for parsing textual content from a file.
- **ContentType** - The ContentType class is responsible for detecting matching document types based on an application provided CSV file.

### Data Source Grid Classes

Collection of data source grid management classes.

- **GridCriteria** - This is a helper class for the Grid data source and should not be invoked by applications.
- **GridDS** - The grid data source manages a row x column grid of DataItem cells in memory.

### Data Source JSON Classes

Collection of data source JSON management classes.

- **JsonDS** - The JSON data source manages a row x column grid of DataItem cells in memory using JSON path expressions to populate them.

### Data Source Graph Classes

Collection of data source graph management classes.

- **GraphDS** - The graph data source manages an in-memory graph of vertexes and edges using an underlying GridDS to handle CRUD+S methods, storage/retrieval of the data sets via CSV files and the visualization of the current graph.

### Data Source Redis Classes

Collection of data source Redis management classes - this covers Redis core data structures, RediSearch, RedisJSON, RedisGraph and RedisTimeSeries modules.

- **com.redis.ds.ds_redis** - The RedisDS class is defined in this package and it serves as the primary connection object for Redis database communication.
- **com.redis.ds.ds_redis.core** - The RedisCore class is responsible for accessing the core Redis commands via the Jedis programmatic library. It designed to simplify the use of core Foundation class objects like items, documents and grids.
- **com.redis.ds.ds_redis.graph** - The RedisGraphs class is responsible for accessing the RedisGraph module commands via the Jedis programmatic library. It designed to simplify the use of core Foundation class objects like items, documents, grids and graphs.
- **com.redis.ds.ds_redis.json** - The RedisJson class is responsible for accessing the RedisJSON module commands via the Jedis programmatic library. It designed to simplify the use of core Foundation class objects like items, documents and grids.
- **com.redis.ds.ds_redis.search** - The RedisSearch class is responsible for accessing the RediSearch module commands via the Jedis programmatic library. It designed to simplify the use of core Foundation class objects like items, documents and grids.
- **com.redis.ds.ds_redis.shared** - The shared package contains shared utility classes for Redis key and field name management.
- **com.redis.ds.ds_redis.time_series** - The RedisTimeseries class is responsible for accessing the RedisTimeSeries module commands via the Jedis programmatic library. It designed to simplify the use of core Foundation class objects like items, documents and grids.

### Redis App Studio UI Classes

Collection of Redis App Studio UI framework server-side classes.

- **com.redis.app.redis_app_studio.dm** - Collection of server-side UI classes for the App Studio Data Modeler application.
- **com.redis.app.redis_app_studio.rc** - Collection of server-side UI classes for the App Studio RedisCore application.
- **com.redis.app.redis_app_studio.rg** - Collection of server-side UI classes for the App Studio RedisGraph application.
- **com.redis.app.redis_app_studio.rj** - Collection of server-side UI classes for the App Studio RedisJSON application.
- **com.redis.app.redis_app_studio.rsh** - Collection of server-side UI classes for the App Studio RediSearch for Hashes application.
- **com.redis.app.redis_app_studio.rsj** - Collection of server-side UI classes for the App Studio RediSearch for JSON Documents application.
- **com.redis.app.redis_app_studio.servlet** - Collection server-side UI of classes to handle App Studio graph visualization features.
- **com.redis.app.redis_app_studio.shared** - Collection of server-side UI classes shared by all App Studio applications.

