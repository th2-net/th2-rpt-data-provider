# Report data provider

# Overview
This component serves as a backend for rpt-viewer. It will connect to the cassandra database via cradle api and expose the data stored in there as REST resources.

# API

### REST

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

`http://localhost:8080/events` - returns list of events with the specified ids (at a time you can request no more
 `eventSearchChunkSize`)
- `ids` - text, one or more event ids **Required**

```
Example:
    http://localhost:8082/events/?ids=first_id&ids=second_id
``` 


`http://localhost:8080/message/{id}` - returns a single message with the specified id

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
  "attachedEventIds": []
}
```

### SSE

#### Filters API

Filters are formed as follows:
- `filters={filter name}` - you must register the filter by specifying its name.  
- `{filter name}-{parameter}={parameter value}` - each filter parameter

****Types of filter `parameter`****:
- `value / values` - text, number, boolean, etc. - one / many values to match against a filter.
- `negative` - boolean. - Negates the result of the filter. To retrieve data that does NOT match the specified filter. Defaults to `false`.
- `conjunct` - boolean. - Used if the filter takes several values. If `true` then the element must match ALL given `values`. If `false` then element must match ANY of `values`. *Not used in filters with one possible value*. Defaults to `false`.

```
As example:
/search/sse/events/?startTimestamp=1605872487277&filters=name&filters=type&name-values=Checkpoint&type-values=session&type-negative=true
```

`http://localhost:8080/filters/sse-messages` - get all names of sse message filters

`http://localhost:8080/filters/sse-events` - get all names of sse event filters

`http://localhost:8080/filters/sse-messages/{filter name}` - get filter info

`http://localhost:8080/filters/sse-events/{filter name}` - get filter info

`http://localhost:8080/match/event/{id}` - return boolean value. Check that event with the specified id is matched by filter

`http://localhost:8080/match/message/{id}` - return boolean value. Check that message with the specified id is matched by filter
   

```Filter info example
  {
    name: "type", // non-nullable
    hint: "matches messages by one of the specified types" // nullable
    parameters: [
      {
        name: "negative", // non-nullable string
        type: "boolean", // possible values are "number", "boolean", "string", "string[]"
        defaultValue: false, // nullable, should match the type
        hint: null // nullable string
      },
      {
        name: "values",
        type: "string[]",
        defaultValue: null,
        hint: "NewOrderSingle, ..."
      },
    ]
  } 
```


##### SSE requests API
`http://localhost:8080/search/sse/events` - create a sse channel of event metadata that matches the filter. Accepts following query parameters:
- `startTimestamp` - number, unix timestamp in milliseconds - Sets the search starting point. **One of the 'startTimestamp' or 'resumeFromId' must not be null** 
- `resumeFromId` - text, last event id. In order to continue the execution of an interrupted sse request, you need to send exactly the same request with an indication of the element ID, from which to resume data transfer. Defaults to `null`. **One of the 'startTimestamp' or 'resumeFromId' must not be null**

- `parentEvent` - text - Will match events with the specified parent element.
- `searchDirection` - `next`/`previous` - Sets the lookup direction. Can be used for pagination. Defaults to `next`.
- `resultCountLimit` - number - Sets the maximum amount of events to return. Defaults to `null (unlimited)`.
- `endTimestamp` - number, unix timestamp in milliseconds - Sets the timestamp to which the search will be performed, starting with `startTimestamp`. When `searchDirection` is `previous`, `endTimestamp` must be less then `startTimestamp`. Defaults to `null` (the search is carried out endlessly into the past or the future).
- `limitForParent` - number - How many children for each parent do we want to request. Default `not limited`.
- `metadataOnly` - boolean - Receive only metadata (`true`) or entire event (`false`) (without `attachedMessageIds`). Default `true`.
- `attachedMessages`- boolean - If the `metadataOnly` is `false` additionally load `attachedMessageIds`. Default `false`.


Event metadata object example (in sse):
```
{
    "type": "eventTreeNode",
    "eventId": "e21de910-fd30-11ea-8896-d7538a286e60",
    "parentId": "e21de910-gc89-11ea-8345-d7538a286e60", 
    "eventName": "Send 'OrderMassCancelRequest' message",
    "eventType": "sendMessage",
    "successful": true,
    "startTimestamp": {
        "nano": 209190000,
        "epochSecond": 1600819698
    },
}
```

[FILTERS](#filters-api):
- `attachedMessageId` - Filters the events that are linked to the specified message id. Parameters: `values` - text, `negative` - boolean, `conjunct` - boolean.  
- `name` - Will match the events which name contains one of the given substrings. Parameters: `values` - text, accepts multiple values, case-insensitive, `negative` - boolean, `conjunct` - boolean.  
- `type` - Will match the events which type contains one of the given substrings. Parameters: `values` - text, accepts multiple values, case-insensitive, `negative` - boolean, `conjunct` - boolean.  
- `body` - Will match the events which body contains one of the given substrings. Parameters: `values` - text, accepts multiple values, case-insensitive, `negative` - boolean, `conjunct` - boolean.  
- `status` - Will match the events which status equals that specified. Parameters: `value` - string, one of `failed` or `passed`. `negative` - boolean.


`http://localhost:8080/search/sse/messages` - create a sse channel of messages that matches the filter. Accepts following query parameters:
- `startTimestamp` - number, unix timestamp in milliseconds - Sets the search starting point. **One of the 'startTimestamp' or 'resumeFromId' must not be null**
- `resumeFromId` - text, last message id. In order to continue the execution of an interrupted sse request, you need to send exactly the same request with an indication of the element ID, from which to resume data transfer. Defaults to `null`. **One of the 'startTimestamp' or 'resumeFromId' must not be null**

- `stream` - text, accepts multiple values - Sets the stream ids to search in. Case-sensitive. **Required**. 
- `searchDirection` - `next`/`previous` - Sets the lookup direction. Can be used for pagination. Defaults to `next`.
- `resultCountLimit` - number - Sets the maximum amount of messages to return. Defaults to `null (unlimited)`.
- `endTimestamp` - number, unix timestamp in milliseconds - Sets the timestamp to which the search will be performed, starting with `startTimestamp`. When `searchDirection` is `previous`, `endTimestamp` must be less then `startTimestamp`. Defaults to `null` (the search is carried out endlessly into the past or the future).
- `messageId` - text, accepts multiple values - List of message IDs to restore search. If given, it has the highest priority and ignores `stream` (uses streams from ids), `startTimestamp` and `resumeFromId`. Defaults to `null`
- `attachedEvents`- boolean - If `true`, additionally load `attachedEventIds`. Default `false`.
- `lookupLimitDays` - number - The number of days that will be viewed on the first request to get the one closest to the specified timestamp. Default `null` - not limited to the past and up to the present moment to the future.

[FILTERS](#filters-api):

- `attachedEventIds` - Filters the messages that are linked to the specified event id. Parameters: `values` - text, accepts multiple values, `negative` - boolean, `conjunct` - boolean. 
- `type` - Will match the messages by their full type name. Parameters: `values` - text, accepts multiple values, case-insensitive, `negative` - boolean, `conjunct` - boolean.
- `body` - Will match the messages by their parsed body. Parameters: `values` - text, accepts multiple values, case-insensitive, `negative` - boolean, `conjunct` - boolean.
- `bodyBinary` - Will match the messages by their binary body. Parameters: `values` - text, accepts multiple values, case-insensitive, `negative` - boolean, `conjunct` - boolean.

Elements in channel match the format sse: 
```
event: 'event' / 'message' | 'close' | 'error' | 'keep_alive'
data: 'Event metadata object' / 'message' | 'Empty data' | 'HTTP Error code' | 'Empty data'
id: event / message id | null | null | null
```


# Configuration
schema component description example (rpt-data-provider.yml):
```
apiVersion: th2.exactpro.com/v1
kind: Th2CoreBox
metadata:
  name: rpt-data-provider
spec:
  image-name: ghcr.io/th2-net/th2-rpt-data-provider
  image-version: 2.2.5 // change this line if you want to use a newer version
  type: th2-rpt-data-provider
  custom-config:
    hostname: localhost
    port: 8080
    responseTimeout: 60000 // maximum request processing time in milliseconds
    
    eventCacheSize: 1000 // internal event cache size
    messageCacheSize: 1000 // internal message cache size
    serverCacheTimeout: 60000 // cached event lifetime in milliseconds
    
    ioDispatcherThreadPoolSize: 10 // thread pool size for blocking database calls
    codecResponseTimeout: 6000 // if a codec doesn't respond in time, requested message is returned with a 'null' body
    checkRequestsAliveDelay: 2000 // response channel check interval in milliseconds
    
    enableCaching: true // enables proxy and client cache (Cache-control response headers)
    notModifiedObjectsLifetime: 3600 // max-age in seconds
    rarelyModifiedObjects: 500 // max-age in seconds
              
    sseEventSearchStep: 200 // step size in seconds when requesting events 
    keepAliveTimeout: 5000 // timeout in milliseconds. keep_alive sending frequency
    cradleDispatcherPoolSize: 1 // number of threads in the cradle dispatcher
      
    messageExtractorOutputBatchBuffer: 1       // buffer size of message search pipeline
    messageConverterOutputBatchBuffer: 1
    messageDecoderOutputBatchBuffer: 1
    messageUnpackerOutputMessageBuffer: 100
    messageFilterOutputMessageBuffer: 100
    messageMergerOutputMessageBuffer: 10
   
    codecPendingBatchLimit: 16              // the total number of messages sent to the codec batches in parallel for all pipelines
    codecCallbackThreadPool: 4              // thread pool for parsing messages received from codecs
    codecRequestThreadPool: 1               // thread pool for sending message to codecs
    grpcWriterMessageBuffer: 10            // buffer before send grpc response

    sendEmptyDelay: 100 // frequency of sending empty messages

    eventSearchChunkSize: 64 // the size of event chunks during sse search and the maximum size of the batch of messages upon request getEvents

    sendPipelineStatus: false // enable profiling

    grpcThreadPoolSize: 20 // thread pool size for grpc requests

    useStrictMode: false // if true throw an exception when bad messages are received from the codec otherwise return messages with null body and type

    serverType: HTTP // provider server type. Allows 'HTTP' and 'GRPC' (case sensetive).

    codecUsePinAttributes: true // send raw message to specified codec (true) or send to all codecs (false) 

  pins: // pins are used to communicate with codec components to parse message data
    - name: to_codec
      connection-type: mq
      attributes:
        - to_codec
        - raw
        - publish
    - name: from_codec
      connection-type: mq
      attributes:
        - from_codec
        - parsed
        - subscribe
  extended-settings:
    chart-cfg:
      ref: schema-stable
      path: custom-component
    service:
      enabled: false
      nodePort: '31275'
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError -Ddatastax-java-driver.advanced.connection.init-query-timeout=\"5000 milliseconds\""
    resources:
      limits:
        memory: 2000Mi
        cpu: 600m
      requests:
        memory: 300Mi
        cpu: 50m

```
