Example of Report data provider component env variables:

CRADLE_INSTANCE_NAME=instance1

CASSANDRA_DATA_CENTER=kos
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=demo
CASSANDRA_USERNAME=guest
CASSANDRA_PASSWORD=guest
CASSANDRA_QUERY_TIMEOUT=30000 - defined in milliseconds

HTTP_PORT=8080
HTTP_HOST=localhost
HTTP_RESPONSE_TIMEOUT=60000 - defined in milliseconds
SERVER_CACHE_TIMEOUT=60000 - defined in milliseconds; sets the cache invalidation timeout for non-batched events
CLIENT_CACHE_TIMEOUT=60 - defined in seconds; sets max-age value in http cache control header

EVENT_CACHE_SIZE=100000 - sets in-memory cache limit (item count)
MESSAGE_CACHE_SIZE=100000 - sets in-memory cache limit (item count)
THREAD_POOL_SIZE=100 - sets the thread pool size of IO coroutine dispatcher
