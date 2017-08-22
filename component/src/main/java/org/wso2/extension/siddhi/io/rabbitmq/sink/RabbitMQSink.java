/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.rabbitmq.sink;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.file.util.Constants;
import org.wso2.extension.siddhi.io.rabbitmq.util.RabbitMQConstants;
import org.wso2.extension.siddhi.io.rabbitmq.util.RabbitMQSinkUtil;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * {@code RabbitmqSink } Handle the Rabbitmq publishing tasks.
 */
@Extension(
        name = "rabbitmq",
        namespace = "sink",
        description = "The rabbitmq sink pushes the events into a rabbitmq broker using the AMQP protocol",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "The uri that used to connects to an AMQP server. This is a mandatory " +
                                "parameter and if it is not specified, an error is logged in the CLI " +
                                "e.g., " +
                                "`amqp://guest:guest`, " +
                                "`amqp://guest:guest@localhost:5672` ",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "heartbeat",
                        description = "It defines after what period of time the peer TCP connection should " +
                                "be considered unreachable (down) by RabbitMQ and client libraries.",
                        type = {DataType.INT},
                        optional = true, defaultValue = "60"),
                @Parameter(
                        name = "exchange.name",
                        description = "The name of the exchange, which decide what to do with a message it receives." +
                                "If the exchange.name is already in the RabbitMQ server, then the system use " +
                                "that exchange.name instead of redeclaring.",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(
                        name = "exchange.type",
                        description = "The type of the exchange.name. There are four different exchange types are " +
                                "available: `direct`, `fanout`, `topic` and `headers`. ",
                        type = {DataType.STRING},
                        dynamic = true,
                        optional = true, defaultValue = "direct"),
                @Parameter(
                        name = "exchange.durable.enabled",
                        description = "Decide whether the exchange should remain declared even if the broker " +
                                "restarts. ",
                        type = {DataType.BOOL},
                        dynamic = true,
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "exchange.autodelete.enabled",
                        description = "Decide whether to keep the exchange even if it is not used anymore. ",
                        type = {DataType.BOOL},
                        dynamic = true,
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "delivery.mode",
                        description = "Decide whether the connection should be persistent or not. delivery.mode's" +
                                "value should be either 1 or 2. " +
                                "If the delivery.mode = 1, then the connection is not persistent. " +
                                "If the delivery.mode = 2, then the connection is persistent",
                        type = {DataType.INT},
                        optional = true, defaultValue = "1"),
                @Parameter(
                        name = "content.type",
                        description = "Message content type, value should be MIME content type.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "content.encoding",
                        description = "Message content encoding, value should be MIME content encoding.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "priority",
                        description = "Message priority, value should be within the range 0 to 9",
                        type = {DataType.INT},
                        dynamic = true,
                        optional = true, defaultValue = "0"),
                @Parameter(
                        name = "correlation.id",
                        description = "Message correlated to this one, e.g. what request this message is a " +
                                "reply to. A request comes in, message describing the task is pushed to queue " +
                                "by the frontend server. After that the frontend server blocks to wait for " +
                                "response message with the same correlation id. A pool of worker machines are " +
                                "listening on queue and one of them picks up the task, performs it and returns " +
                                "the result as message. Once a message with right correlation id comes in, " +
                                "frontend server continues to return the response to the caller. ",
                        type = {DataType.STRING},
                        dynamic = true,
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "reply.to",
                        description = "This is an anonymous exclusive callback Queue. When the RabbitMQ receives " +
                                "a message with the property 'reply.to', it will send the response to the " +
                                "mentioned queue. Commonly used to name a reply queue (or any other identifier " +
                                "that helps a consumer application to direct its response).",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "expiration",
                        description = "Expiration time after which the message will be deleted. The value of the " +
                                "expiration field describes the TTL period in milliseconds.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "message.id",
                        description = "Message identifier. If applications need to identify messages, " +
                                "it is recommended that they use this attribute instead of putting it into " +
                                "the message payload. ",
                        type = {DataType.STRING},
                        dynamic = true,
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "timestamp",
                        description = "Timestamp of the moment when message was sent. If the timestamp is empty, " +
                                "then the system will automatically generate the current date and time as the " +
                                "timestamp value. The format of the timestamp is that dd/mm/yyyy.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "current timestamp"),
                @Parameter(
                        name = "type",
                        description = "Message type, e.g. what type of event or command this message represents.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "user.id",
                        description = "Optional user ID. Verified by RabbitMQ against the actual connection " +
                                "username. ",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "app.id",
                        description = "Identifier of the application that produced the message.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "routing.key",
                        description = "The key that the exchange looks at to decide how to route the message to " +
                                "queues. The routing key is like an address for the message. ",
                        type = {DataType.STRING},
                        dynamic = true,
                        optional = true, defaultValue = "empty"),
                @Parameter(
                        name = "headers",
                        description = "Headers of the message. The attributes used for routing are taken " +
                                "from the headers attribute. A message is considered matching if the value of " +
                                "the header equals the value specified upon binding ",
                        type = {DataType.STRING},
                        dynamic = true,
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "tls.enabled",
                        description = "Used to establish an encrypted communication channel. The parameters " +
                                "`tls.truststore.path` and `tls.truststore.password` should be initialised " +
                                "when the parameter tls.enable = true: ",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "tls.truststore.path",
                        description = "The file path to the location of the truststore of the client that sends " +
                                "the RabbitMQ events through 'AMQP' protocol. A custom client-truststore can be " +
                                "specified if required. If custom truststore is not specified then the system " +
                                "uses the default client-trustore in the " +
                                "`${carbon.home}/resources/security` directory.",
                        type = {DataType.STRING},
                        optional = true,  defaultValue = "${carbon.home}/resources/security/client-truststore.jks"),
                @Parameter(
                        name = "tls.truststore.password",
                        description = "The password for the client-truststore. A custom password can be specified " +
                                "if required. If no custom password is specified, then the system uses " +
                                "'wso2carbon' as the default password.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "wso2carbon"),
                @Parameter(
                        name = "tls.truststore.type",
                        description = "The type of the truststore.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "JKS"),
                @Parameter(
                        name = "tls.version",
                        description = "The version of the tls/ssl.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "SSL")
        },
        examples = {
                @Example(
                        description = "The following query will publish events to 'direct' exchange with " +
                                "exchange type `direct` and routing key `directTest`",
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@sink(type ='rabbitmq', " +
                                "uri = 'amqp://guest:guest@localhost:5672', " +
                                "exchange.name = 'direct', " +
                                "routing.key= 'direct', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n")}
)
public class RabbitMQSink extends Sink {
    private static final Logger log = Logger.getLogger(RabbitMQSink.class);

    private String publisherURI;
    private Connection connection = null;
    private Channel channel = null;
    private int heartbeat;
    private Option exchangeNameOption;
    private Option exchangeTypeOption;
    private Option exchangeDurableAsStringOption;
    private Option routingKeyOption;
    private Option headerOption;
    private int deliveryMode;
    private Option exchangeAutoDeleteAsStringOption;
    private String contentType;
    private String contentEncoding;
    private Option messageIdOption;
    private String timestampString;
    private String replyTo;
    private String expiration;
    private Option priorityOption;
    private Option correlationIdOption;
    private String userId;
    private String appId;
    private String type;
    private boolean tlsEnabled;
    private String tlsTruststoreLocation;
    private String tlsTruststoreType;
    private String tlsVersion;
    private String tlsTruststorePassword;
    private StreamDefinition streamDefinition;
    private FileInputStream fileInputStream = null;

    @Override
    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                        ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.publisherURI = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_SERVER_URI);
        this.heartbeat = Integer.parseInt(optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_HEARTBEAT,
                RabbitMQConstants.DEFAULT_HEARTBEAT));
        this.exchangeNameOption = optionHolder.validateAndGetOption(RabbitMQConstants.RABBITMQ_EXCHANGE_NAME);
        this.exchangeTypeOption = optionHolder.getOrCreateOption
                (RabbitMQConstants.RABBITMQ_EXCHANGE_TYPE, RabbitMQConstants.DEFAULT_EXCHANGE_TYPE);

        this.tlsTruststoreLocation = optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_LOCATION,
                        RabbitMQSinkUtil.getTrustStorePath(configReader));
        this.tlsTruststorePassword = optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_PASSWORD,
                        RabbitMQSinkUtil.getTrustStorePassword(configReader));
        this.tlsTruststoreType = optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_TYPE,
                        RabbitMQConstants.DEFAULT_TLS_TRUSTSTORE_TYPE);
        this.tlsVersion = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_CONNECTION_TLS_VERSION,
                RabbitMQConstants.DEFAULT_TLS_VERSION);
        this.exchangeDurableAsStringOption = optionHolder.getOrCreateOption
                (RabbitMQConstants.RABBITMQ_EXCHANGE_DURABLE, RabbitMQConstants.DEFAULT_EXCHANGE_DURABLE);

        this.exchangeAutoDeleteAsStringOption = optionHolder.getOrCreateOption
                (RabbitMQConstants.RABBITMQ_EXCHANGE_AUTO_DELETE,
                        RabbitMQConstants.DEFAULT_EXCHANGE_AUTODELETE);

        this.deliveryMode = Integer.parseInt(optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_DELIVERY_MODE,
                        RabbitMQConstants.DEFAULT_DELIVERY_MODE));
        this.contentType = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_CONTENT_TYPE,
                RabbitMQConstants.NULL);
        this.contentEncoding = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_CONTENT_ENCODING,
                RabbitMQConstants.NULL);
        this.priorityOption = optionHolder.getOrCreateOption(RabbitMQConstants.RABBITMQ_PRIORITY,
                RabbitMQConstants.DEFAULT_PRIORITY);
        this.correlationIdOption = optionHolder.getOrCreateOption(RabbitMQConstants.RABBITMQ_CORRELATION_ID,
                RabbitMQConstants.NULL);
        this.messageIdOption = optionHolder.getOrCreateOption(RabbitMQConstants.RABBITMQ_MESSAGE_ID,
                RabbitMQConstants.NULL);
        this.appId = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_APP_ID,
                RabbitMQConstants.NULL);
        this.timestampString = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_TIMESTAMP,
                RabbitMQConstants.NULL);
        this.replyTo = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_REPLY_TO,
                RabbitMQConstants.NULL);
        this.expiration = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_EXPIRATION,
                RabbitMQConstants.NULL);
        this.userId = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_USER_ID,
                RabbitMQConstants.NULL);
        this.type = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_TYPE,
                RabbitMQConstants.NULL);
        this.tlsEnabled = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                    (RabbitMQConstants.RABBITMQ_CONNECTION_TLS_ENABLED,
                            RabbitMQConstants.DEFAULT_EXCHANGE_TLS_ENABLED));

        this.routingKeyOption = optionHolder.getOrCreateOption
                (RabbitMQConstants.RABBITMQ_ROUTINGKEY, RabbitMQConstants.EMPTY_STRING);

        this.headerOption = optionHolder.getOrCreateOption(RabbitMQConstants.RABBITMQ_HEADERS,
                RabbitMQConstants.NULL);

        if (!RabbitMQConstants.EXCHANGE_TYPE_FANOUT.equals(exchangeTypeOption.getValue())
                && !RabbitMQConstants.EXCHANGE_TYPE_DIRECT.equals(exchangeTypeOption.getValue())
                && !RabbitMQConstants.EXCHANGE_TYPE_TOPIC.equals(exchangeTypeOption.getValue())
                && !RabbitMQConstants.EXCHANGE_TYPE_HEADERS.equals(exchangeTypeOption.getValue())) {
            throw new SiddhiAppCreationException("Check the exchange type in " + this.streamDefinition + ". " +
                    "There is no exchange type named as " + exchangeTypeOption.getValue() + " in RabbitMQ");
        }

    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        try {

            ConnectionFactory factory = new ConnectionFactory();
            URI uri = URI.create(publisherURI);
            factory.setUri(uri);
            factory.setRequestedHeartbeat(heartbeat);

            if (tlsEnabled) {
                //TODO default truststore location should be defined as wso2 truststore location.
                if (tlsTruststoreLocation.isEmpty()) {
                    factory.useSslProtocol();
                } else {
                    try {
                        char[] trustStorePassword = tlsTruststorePassword.toCharArray();
                        KeyStore keyStore = KeyStore.getInstance(tlsTruststoreType);
                        fileInputStream = new FileInputStream(tlsTruststoreLocation);
                        keyStore.load(fileInputStream, trustStorePassword);

                        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance
                                (TrustManagerFactory.getDefaultAlgorithm());
                        trustManagerFactory.init(keyStore);

                        SSLContext context = SSLContext.getInstance(tlsVersion);
                        context.init(null, trustManagerFactory.getTrustManagers(), null);
                        factory.useSslProtocol(context);
                    } catch (FileNotFoundException e) {
                        throw new SiddhiAppCreationException("The trustStore File path " +
                                "" + RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_LOCATION + " = " +
                                "" + tlsTruststoreLocation + " defined in " + streamDefinition + " is incorrect." +
                                " Specify TrustStore location correctly.", e);
                    } catch (CertificateException e) {
                        throw new SiddhiAppCreationException("TrustStore is not specified in " + streamDefinition, e);
                    } catch (NoSuchAlgorithmException e) {
                        throw new SiddhiAppCreationException("Algorithm " +
                                "" + RabbitMQConstants.RABBITMQ_CONNECTION_TLS_VERSION + " = " + tlsVersion + " " +
                                "defined in " + streamDefinition + "is not available in " +
                                "TrustManagerFactory class.", e);
                    } catch (KeyStoreException e) {
                        throw new SiddhiAppCreationException("The trustStore type " +
                                "" + RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_TYPE + "= " +
                                "" + tlsTruststoreType + " defined in " + streamDefinition + " is incorrect." +
                                " Specify TrustStore type correctly.", e);
                    } catch (IOException e) {
                        throw new SiddhiAppCreationException("The trustStore type " +
                                "" + RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_PASSWORD + " = " +
                                "" + tlsTruststorePassword + " defined in " + streamDefinition + " is incorrect." +
                                " Specify TrustStore password correctly.", e);
                    } finally {
                        if (fileInputStream != null) {
                            fileInputStream.close();
                        }
                    }
                }
            }

            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException e) {
            throw new ConnectionUnavailableException(
                    "Failed to connect with the Rabbitmq server. Check the " +
                            "" + RabbitMQConstants.RABBITMQ_SERVER_URI + " = " + publisherURI + " defined in " +
                            "" + streamDefinition, e);
        } catch (NoSuchAlgorithmException e) {
            throw new SiddhiAppCreationException(
                    "No such algorithm in the " + RabbitMQConstants.RABBITMQ_SERVER_URI + " = "
                            + publisherURI + " defined in " + streamDefinition, e);
        } catch (URISyntaxException e) {
            throw new SiddhiAppCreationException(
                    "There is an invalid syntax in the " + RabbitMQConstants.RABBITMQ_SERVER_URI + " = "
                            + publisherURI + " defined in " + streamDefinition, e);
        } catch (TimeoutException e) {
            throw new SiddhiAppCreationException(
                    "Timeout while connectiong with the RabbitMQ server", e);
        } catch (KeyManagementException e) {
            throw new SiddhiAppCreationException(
                    "There is an error in key management in the " + RabbitMQConstants.RABBITMQ_SERVER_URI + " = "
                            + publisherURI + " defined in " + streamDefinition, e);
        }
    }

        @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException {
        try {
            byte[] byteArray;
            if (payload instanceof byte[]) {
                byteArray = (byte[]) payload;
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append(payload.toString()).append("\n");
                byteArray = sb.toString().getBytes(Constants.UTF_8);
            }
            String exchangeName = exchangeNameOption.getValue(dynamicOptions);
            String exchangeType = exchangeTypeOption.getValue(dynamicOptions);
            String headers = headerOption.getValue(dynamicOptions);
            String messageId = messageIdOption.getValue(dynamicOptions);
            int priority = Integer.parseInt(priorityOption.getValue(dynamicOptions));
            String correlationId = correlationIdOption.getValue(dynamicOptions);
            Date timestamp;
            if (timestampString == null) {
                timestamp = new Date();
            } else {
                SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
                timestamp = formatter.parse(timestampString);
            }

            BasicProperties props = new BasicProperties();
            Map<String, Object> map = null;
            if (headers != null) {
                map = RabbitMQSinkUtil.getHeaders(headers);
            }
            String routingKey = routingKeyOption.getValue(dynamicOptions);
            props = props.builder().
                    deliveryMode(deliveryMode).
                    contentType(contentType).
                    contentEncoding(contentEncoding).
                    messageId(messageId).
                    replyTo(replyTo).
                    expiration(expiration).
                    priority(priority).
                    correlationId(correlationId).
                    userId(userId).
                    appId(appId).
                    type(type).
                    timestamp(timestamp).
                    headers(map).
                    build();

            boolean exchangeAutoDelete = Boolean.parseBoolean(exchangeDurableAsStringOption.getValue(dynamicOptions));
            boolean exchangeDurable = Boolean.parseBoolean(exchangeAutoDeleteAsStringOption.getValue(dynamicOptions));
            /*
              In the following method, system checked whether the exchange.name is already existed or not.
              If the exchange.name is not existed, then the system declare the exchange.name
             */
            try {
                channel.exchangeDeclarePassive(exchangeName);
            } catch (Exception e) {
                channel = connection.createChannel();
                RabbitMQSinkUtil.declareExchange(connection, channel, exchangeName, exchangeType,
                        exchangeDurable, exchangeAutoDelete);
            }

            channel.basicPublish(exchangeName, routingKey, props, byteArray);
        } catch (ParseException e) {
           throw new SiddhiAppCreationException("Invalid timestamp format defined in " + timestampString + " . " +
                   "Please include as dd/MM/yyyy in " + streamDefinition, e);
        } catch (UnsupportedEncodingException e) {
            throw new SiddhiAppCreationException("Received payload does not support UTF-8 encoding. Hence " +
                    "dropping the event", e);
        } catch (IOException e) {
            log.error("Error in sending the message to the " + RabbitMQConstants.RABBITMQ_EXCHANGE_NAME +
                    " = " + exchangeNameOption.getValue() + " in RabbitMQ broker at " + streamDefinition, e);
        } catch (TimeoutException e) {
            throw new SiddhiAppCreationException(
                    "Timeout while publishing the events to " + exchangeNameOption.getValue() + " in " +
                            "RabbitMQ server", e);
        }
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{RabbitMQConstants.RABBITMQ_EXCHANGE_NAME, RabbitMQConstants.RABBITMQ_EXCHANGE_TYPE,
                RabbitMQConstants.RABBITMQ_ROUTINGKEY, RabbitMQConstants.RABBITMQ_EXCHANGE_DURABLE,
                RabbitMQConstants.RABBITMQ_EXCHANGE_AUTO_DELETE, RabbitMQConstants.RABBITMQ_MESSAGE_ID,
                RabbitMQConstants.RABBITMQ_CORRELATION_ID, RabbitMQConstants.RABBITMQ_PRIORITY};
    }

    @Override
    public void disconnect() {
        if (connection != null) {
            try {
                channel.close();
                connection.close();
                if (log.isDebugEnabled()) {
                    log.debug("Server connector for uri = " + publisherURI + " is disconnected in " +
                            "" + streamDefinition + ".");
                }
            } catch (TimeoutException e) {
                log.error("Timeout while disconnecting the uri = " + publisherURI + " in " +
                        "" + streamDefinition + ".");
            } catch (IOException e) {
                log.error("Error in disconnecting the uri = " + publisherURI + " in " +
                    "" + streamDefinition + ".");
            }
        }
    }

    @Override
    public void destroy() {
    }

}
