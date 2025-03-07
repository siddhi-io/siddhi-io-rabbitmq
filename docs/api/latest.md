# API Docs - v3.0.8

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## Sink

### rabbitmq *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The rabbitmq sink pushes the events into a rabbitmq broker using the AMQP protocol</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="rabbitmq", uri="<STRING>", heartbeat="<INT>", exchange.name="<STRING>", exchange.type="<STRING>", exchange.durable.enabled="<BOOL>", exchange.autodelete.enabled="<BOOL>", delivery.mode="<INT>", content.type="<STRING>", content.encoding="<STRING>", priority="<INT>", correlation.id="<STRING>", reply.to="<STRING>", expiration="<STRING>", message.id="<STRING>", timestamp="<STRING>", type="<STRING>", user.id="<STRING>", app.id="<STRING>", routing.key="<STRING>", headers="<STRING>", tls.enabled="<BOOL>", tls.truststore.path="<STRING>", tls.truststore.password="<STRING>", tls.truststore.type="<STRING>", tls.version="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The URI that used to connect to an AMQP server. If no URI is specified, an error is logged in the CLI.e.g.,<br><code>amqp://guest:guest</code>, <code>amqp://guest:guest@localhost:5672</code> </p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">heartbeat</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The period of time (in seconds) after which the peer TCP connection should be considered unreachable (down) by RabbitMQ and client libraries.</p></td>
        <td style="vertical-align: top">60</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the exchange that decides what to do with a message it sends.If the <code>exchange.name</code> already exists in the RabbitMQ server, then the system uses that <code>exchange.name</code> instead of redeclaring.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The type of the exchange.name. The exchange types available are <code>direct</code>, <code>fanout</code>, <code>topic</code> and <code>headers</code>. For a detailed description of each type, see [RabbitMQ - AMQP Concepts](https://www.rabbitmq.com/tutorials/amqp-concepts.html) </p></td>
        <td style="vertical-align: top">direct</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.durable.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this is set to <code>true</code>, the exchange remains declared even if the broker restarts.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.autodelete.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this is set to <code>true</code>, the exchange is automatically deleted when it is not used anymore. </p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">delivery.mode</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This determines whether the connection should be persistent or not. The value must be either <code>1</code> or <code>2</code>.If the delivery.mode = 1, then the connection is not persistent. If the delivery.mode = 2, then the connection is persistent.</p></td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">content.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The message content type. This should be the <code>MIME</code> content type.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">content.encoding</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The message content encoding. The value should be <code>MIME</code> content encoding.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">priority</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Specify a value within the range 0 to 9 in this parameter to indicate the message priority.</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">correlation.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The message correlated to the current message. e.g., The request to which this message is a reply. When a request arrives, a message describing the task is pushed to the queue by the front end server. After that the frontend server blocks to wait for a response message with the same correlation ID. A pool of worker machines listen on queue, and one of them picks up the task, performs it, and returns the result as message. Once a message with right correlation ID arrives, thefront end server continues to return the response to the caller. </p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">reply.to</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is an anonymous exclusive callback queue. When the RabbitMQ receives a message with the <code>reply.to</code> property, it sends the response to the mentioned queue. This is commonly used to name a reply queue (or any other identifier that helps a consumer application to direct its response).</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">expiration</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The expiration time after which the message is deleted. The value of the expiration field describes the TTL (Time To Live) period in milliseconds.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">message.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The message identifier. If applications need to identify messages, it is recommended that they use this attribute instead of putting it into the message payload.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">timestamp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Timestamp of the moment when the message was sent. If you do not specify a value for this parameter, the system automatically generates the current date and time as the timestamp value. The format of the timestamp value is <code>dd/mm/yyyy</code>.</p></td>
        <td style="vertical-align: top">current timestamp</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The type of the message. e.g., The type of the event or the command represented by the message.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">user.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The user ID specified here is verified by RabbitMQ against theuser name of the actual connection. This is an optional parameter.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">app.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The identifier of the application that produced the message.</p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">routing.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The key based on which the excahnge determines how to route the message to the queue. The routing key is similar to an address for the message.</p></td>
        <td style="vertical-align: top">empty</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">headers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The headers of the message. The attributes used for routing are taken from the this paremeter. A message is considered matching if the value of the header equals the value specified upon binding. </p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies whether an encrypted communication channel should be established or not. When this parameter is set to <code>true</code>, the <code>tls.truststore.path</code> and <code>tls.truststore.password</code> parameters are initialized.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.truststore.path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file path to the location of the truststore of the client that sends the RabbitMQ events via the <code>AMQP</code> protocol. A custom client-truststore can be specified if required. If a custom truststore is not specified, then the system uses the default client-trustore in the <code>${carbon.home}/resources/security</code> directory.</p></td>
        <td style="vertical-align: top">${carbon.home}/resources/security/client-truststore.jks</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.truststore.password</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The password for the client-truststore. A custom password can be specified if required. If no custom password is specified, then the system uses <code>wso2carbon</code> as the default password.</p></td>
        <td style="vertical-align: top">wso2carbon</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.truststore.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The type of the truststore.</p></td>
        <td style="vertical-align: top">JKS</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.version</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The version of the tls/ssl.</p></td>
        <td style="vertical-align: top">SSL</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestExecutionPlan') 
define stream FooStream (symbol string, price float, volume long); 
@info(name = 'query1')
@sink(type ='rabbitmq',
uri = 'amqp://guest:guest@localhost:5672',
exchange.name = 'direct',
routing.key= 'direct',
@map(type='xml'))
Define stream BarStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query publishes events to the <code>direct</code> exchange with the <code>direct</code> exchange type and the <code>directTest</code> routing key.</p>
<p></p>
## Source

### rabbitmq *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The rabbitmq source receives the events from the rabbitmq broker via the AMQP protocol. </p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="rabbitmq", uri="<STRING>", heartbeat="<INT>", exchange.name="<STRING>", exchange.type="<STRING>", exchange.durable.enabled="<BOOL>", exchange.autodelete.enabled="<BOOL>", routing.key="<STRING>", headers="<STRING>", queue.name="<STRING>", queue.durable.enabled="<BOOL>", queue.exclusive.enabled="<BOOL>", queue.autodelete.enabled="<BOOL>", tls.enabled="<BOOL>", tls.truststore.path="<STRING>", tls.truststore.password="<STRING>", tls.truststore.type="<STRING>", tls.version="<STRING>", auto.ack="<BOOL>", consumer.threadpool.size="<INT>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The URI that is used to connect to an AMQP server. If no URI is specified,an error is logged in the CLI.e.g.,<br><code>amqp://guest:guest</code>,<br><code>amqp://guest:guest@localhost:5672</code></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">heartbeat</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The period of time (in seconds) after which the peer TCP connection should be considered unreachable (down) by RabbitMQ and client libraries.</p></td>
        <td style="vertical-align: top">60</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the exchange that decides what to do with a message it receives.If the <code>exchange.name</code> already exists in the RabbitMQ server, then the system uses that <code>exchange.name</code> instead of redeclaring.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The type of the exchange name. The exchange types available are <code>direct</code>, <code>fanout</code>, <code>topic</code> and <code>headers</code>. For a detailed description of each type, see [RabbitMQ - AMQP Concepts](https://www.rabbitmq.com/tutorials/amqp-concepts.html). </p></td>
        <td style="vertical-align: top">direct</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.durable.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this is set to <code>true</code>, the exchange remains declared even if the broker restarts.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exchange.autodelete.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this is set to <code>true</code>, the exchange is automatically deleted when it is not used anymore. </p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">routing.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The key based on which the exchange determines how to route the message to queues. The routing key is like an address for the message. The routing.key must be initialized when the value for the <code>exchange.type</code> parameter is <code>direct</code> or <code>topic</code>.</p></td>
        <td style="vertical-align: top">empty</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">headers</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The headers of the message. The attributes used for routing are taken from the this paremeter. A message is considered matching if the value of the header equals the value specified upon binding. </p></td>
        <td style="vertical-align: top">null</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">queue.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">A queue is a buffer that stores messages. If the queue name already exists in the RabbitMQ server, then the system usees that queue name instead of redeclaring it. If no value is specified for this parameter, the system uses the unique queue name that is automatically generated by the RabbitMQ server. </p></td>
        <td style="vertical-align: top">system generated queue name</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">queue.durable.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to <code>true</code>, the queue remains declared even if the broker restarts</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">queue.exclusive.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to <code>true</code>, the queue is exclusive for the current connection. If it is set to <code>false</code>, it is also consumable by other connections. </p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">queue.autodelete.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to <code>true</code>, the queue is automatically deleted when it is not used anymore.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter specifies whether an encrypted communication channel should be established or not. When this parameter is set to <code>true</code>, the <code>tls.truststore.path</code> and <code>tls.truststore.password</code> parameters are initialized.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.truststore.path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file path to the location of the truststore of the client that receives the RabbitMQ events via the <code>AMQP</code> protocol. A custom client-truststore can be specified if required. If a custom truststore is not specified, then the system uses the default client-trustore in the <code>${carbon.home}/resources/security</code> directory.</p></td>
        <td style="vertical-align: top">${carbon.home}/resources/security/client-truststore.jks</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.truststore.password</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The password for the client-truststore. A custom password can be specified if required. If no custom password is specified, then the system uses <code>wso2carbon</code> as the default password.</p></td>
        <td style="vertical-align: top">wso2carbon</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.truststore.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The type of the truststore.</p></td>
        <td style="vertical-align: top">JKS</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tls.version</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The version of the tls/ssl.</p></td>
        <td style="vertical-align: top">SSL</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">auto.ack</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to <code>false</code>, the server should expect explicit messages acknowledgements once delivered</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">consumer.threadpool.size</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of consumer threads to be registered</p></td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@App:name('TestExecutionPlan') 
define stream FooStream (symbol string, price float, volume long); 
@info(name = 'query1') 
@source(type ='rabbitmq',
uri = 'amqp://guest:guest@localhost:5672',
exchange.name = 'direct',
routing.key= 'direct',
@map(type='xml'))
Define stream BarStream (symbol string, price float, volume long);
from FooStream select symbol, price, volume insert into BarStream;

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This query receives events from the <code>direct</code> exchange with the <code>direct</code>exchange type, and the <code>directTest</code> routing key.</p>
<p></p>
