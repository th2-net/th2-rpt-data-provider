# Report data provider 1.5

# Overview
This component serves as a backend for report-viewer. It will connect to the configured cassandra database via crale api and expose the data stored in there as REST resources.


# API
`http://localhost:8080/` - a test page showing the configured cassandra keyspace and host



`http://localhost:8080/messageStreams` - returns a list of message stream names



`http://localhost:8080/event/{id}` - returns a single event with the specified id

Event object example: 
```
{
  "type": "event",
  "eventId": "e21de910-fd30-11ea-8896-d7538a286e60",
  "batchId": null,
  "isBatched": false,
  "eventName": "Send 'OrderMassCancelRequest' message",
  "eventType": "sendMessage",
  "endTimestamp": {
    "nano": 209824000,
    "epochSecond": 1600819698
  },
  "startTimestamp": {
    "nano": 209190000,
    "epochSecond": 1600819698
  },
  "parentEventId": "e196df51-fd30-11ea-9da8-ffa990115db7",
  "successful": true,
  "attachedMessageIds": [],
  "body": {} // contains a custom json object
}
```


`http://localhost:8080/message/{id}` - returns a sigle message with the specified id

Message object example: 
```
{
  "type": "message",
  "messageId": "fix01:first:1600854429908302153",
  "timestamp": {
    "nano": 334000000,
    "epochSecond": 1600894596
  },
  "direction": "IN",
  "sessionId": "fix01",
  "messageType": "OrderMassCancelReport",
  "body": {}, // parsed data
  "bodyBase64": "" // base64-encoded binary data
}
```



`http://localhost:8080/search/events` - returns an array of event metadata that matches the filter. Accepts following query parameters:
- `attachedMessageId` - text - Filters the events that are linked to the specified message id.
- `timestampFrom` - number, unix timestamp in milliseconds - Sets the lower limit of the time window. **Required**.
- `timestampTo` - number, unix timestamp in milliseconds - Sets the upper limit of the time window. **Required**.
- `name` - text, accepts multiple values - Will match the events which name contains one of the given substrings. Case-insensitive.
- `type` - text, accepts multiple values - Will match the events which type contains one of the given substrings. Case-insensitive.
- `flat` - boolean - If `true`, returns the result as a flat list of event ids. If `false`, returns them as a list of event metadata object trees. Metadata tree will contain parent event objects as long as at least one of their direct or indirect children matches the filter. So, the resulting tree will preserve the hierarchy without the irrelevant branches.

Event metadata object example:
```
{
    "eventId": "e21de910-fd30-11ea-8896-d7538a286e60",
    "eventName": "Send 'OrderMassCancelRequest' message",
    "eventType": "sendMessage",
    "successful": true,
    "startTimestamp": {
        "nano": 209190000,
        "epochSecond": 1600819698
    },
    "childList": [], // may contain an array of the simillar metadata objects
    "filtered": true // is set to 'false' if an event does not match the given filter
}
```



`http://localhost:8080/search/messages` - returns an array of message ids that match the filter. Accepts following query parameters:
- `attachedEventId` - text - Filters the messages that are linked to the specified event id.
- `timestampFrom` - number, unix timestamp in milliseconds - Sets the lower limit of the time window. **Required**.
- `timestampTo` - number, unix timestamp in milliseconds - Sets the upper limit of the time window. **Required**.
- `stream` - text, accepts multiple values - Sets the stream ids to search in. Case-sensitive. **Required**.
- `messageType` - text, accepts multiple values - Will match the messages by their full type name. Case-sensitive. Is very slow at the moment.
- `limit` - number - Sets the maximum amount of messages to return. Can be used for pagination. Defaults to `100`.
- `timelineDirection` - `next`/`previous` - Sets the lookup direction. Can be used for pagination. Defaults to `next`.
- `messageId` - text - Sets the message id to start the lookup from. Can be used for pagination.



# Configuration
Example of Report data provider component environment variables:

```
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
```
