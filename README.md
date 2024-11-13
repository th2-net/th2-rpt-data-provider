# Report data provider (5.15.0)

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
apiVersion: th2.exactpro.com/v2
kind: Th2CoreBox
metadata:
  name: rpt-data-provider
spec:
  imageName: ghcr.io/th2-net/th2-rpt-data-provider
  imageVersion: 5.7.0 // change this line if you want to use a newer version
  type: th2-rpt-data-provider
  customConfig:
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
    
    messageIdsLookupLimitDays: 7            // lookup limit value for seacing next and previous message ids.  
   
    codecPendingBatchLimit: 16              // the total number of messages sent to the codec batches in parallel for all pipelines
    codecCallbackThreadPool: 4              // thread pool for parsing messages received from codecs
    codecRequestThreadPool: 1               // thread pool for sending message to codecs
    grpcWriterMessageBuffer: 10            // buffer before send grpc response

    sendEmptyDelay: 100 // frequency of sending empty messages

    eventSearchChunkSize: 64 // the size of event chunks during sse search and the maximum size of the batch of messages upon request getEvents

    grpcThreadPoolSize: 20 // thread pool size for grpc requests

    useStrictMode: false // if true throw an exception when bad messages are received from the codec otherwise return messages with null body and type

    serverType: HTTP // provider server type. Allows 'HTTP' and 'GRPC' (case sensetive).

    codecUsePinAttributes: true // send raw message to specified codec (true) or send to all codecs (false)
    
    eventSearchTimeOffset: 5000 // sets the offset in milliseconds on search events. (startTimestamp - eventSearchTimeOffset) and (endTimestamp + eventSearchTimeOffset)     

    searchBySessionGroup: true // if true data-provider uses the session alias to group cache and translates http / gRPC requests by session alias to group th2 storage request 
    aliasToGroupCacheSize: 1000 // the size of cache for the mapping between session alias and group.

    useTransportMode: true // if true data-provider uses th2 transport protocol to interact with thw codecs

  pins: // pins are used to communicate with codec components to parse message data
    mq:
      subscribers:
      - name: from_codec
        attributes:
        - from_codec
        - transport-group
        - subscribe
        linkTo:
          - box: codec-fix
            pin: out_codec_decode
      publishers:
      - name: to_codec_fix
        attributes:
        - to_codec
        - transport-group
        - publish
        filters:
          - metadata:
              - fieldName: "session_group"
                expectedValue: "fix*"
                operation: WILDCARD
        
  extendedSettings:
    chartCfg:
      ref: schema-stable
      path: custom-component
    service:
      enabled: false
      nodePort: '31275'
    envVariables:
      JAVA_TOOL_OPTIONS: > 
        -XX:+ExitOnOutOfMemoryError
        -Ddatastax-java-driver.advanced.connection.init-query-timeout="5000 milliseconds"
        -XX:+UseContainerSupport 
        -XX:MaxRAMPercentage=85
    resources:
      limits:
        memory: 2000Mi
        cpu: 600m
      requests:
        memory: 300Mi
        cpu: 50m

```

# Release notes

## 5.15.0
* Migrate to native grouped message request
* Updated th2 gradle plugin: `0.1.4` (th2-bom:4.8.0)

## 5.14.1
* Updated cradle api: `5.4.4-dev`

## 5.14.0
* Reduced required memory for executing sse event request with `limitForParent` parameter
* Updated th2 gradle plugin: `0.1.3` (th2-bom:4.8.0)

## 5.13.1
* Fixed the problem data provider can't handle `messageIds` request with `messageId` but without `startTimestamp` arguments

## 5.13.0
* Provided ability to limit `messageIds` request by `lookupLimitDays` argument or `messageIdsLookupLimitDays` option
* Updated:
  * th2 gradle plugin: `0.1.1`
  * common: `5.14.0-dev`
  * gradle api: `5.4.2-dev`
  * common-utils: `2.3.0-dev`
  * ktor-bom: `2.3.12`
  * ehcache: `3.10.8`
  * kotlin-logging: `5.1.4`

## 5.12.0
* Migrate to th2 gradle plugin `0.0.8`
* Updated common: `5.12.0-dev`
* Updated jaxb-runtime: `2.3.9`
  * '2.3.9' version has 'EDL 1.0' license instead of 'CDDL GPL 1.1' in the '2.3.1'

## 5.11.0
+ Migrate to th2 gradle plugin `0.0.6`
+ Updated bom: `4.6.1`
+ Updated common: `5.11.0-dev`
+ Updated common-utils: `2.2.3-dev`
+ Updated cradle api: `5.3.0-dev`
+ Updated grpc-data-provider: `0.2.0-dev`

## 5.10.0
+ Updated cradle api: `5.2.0-dev`
+ Updated common: `5.8.0-dev`
+ Updated common-utils: `2.2.2-dev`
+ Disabled unsupported [Cassandra driver metrics](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/metrics/)

## 5.9.4

+ Fix problem with incorrect events order when requesting search in previous direction

## 5.9.3
+ Enabled [Cassandra driver metrics](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/metrics/)

## 5.9.2
+ Fix problem with accumulating all batches in memory when provider loads messages by group

## 5.9.1
+ Migrated to the cradle version with fixed load pages where `removed` field is null problem.
+ Updated cradle: `5.1.4-dev`

## 5.9.0
+ Updated ktor: `2.3.3`

## 5.8.0
+ Updated bom: `4.5.0-dev`
+ Updated common: `5.4.0-dev`
+ Updated common-utils: `2.2.0-dev`
+ Updated kotlin: `1.8.22`

## 5.7.1
+ Fixed request groped message on single page [EPOCH, null] problem 
+ Updated cradle: `5.1.2-dev`

## 5.7.0

### Feature
+ Added optional session alias to group cache to translate cradle queries from `messages` to `grouped_messages` tables.
  User can enable this feature by `searchBySessionGroup` option and set cache size in entries by `aliasToGroupCacheSize` option
+ Added mode to interact with codec by th2 transport protocol 

### Updated
+ gradle
  + owasp `8.3.1`
 
+ th2
  + cradle `5.1.0`
  + bom `4.4.0`
  + common `5.3.0`

### Added
+ gradle
  + gradle-git-properties `2.4.1` (gradle plugin for git metadata genration)