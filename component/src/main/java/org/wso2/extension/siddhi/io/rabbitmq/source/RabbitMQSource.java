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

package org.wso2.extension.siddhi.io.rabbitmq.source;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.io.rabbitmq.util.RabbitMQConstants;
import org.wso2.extension.siddhi.io.rabbitmq.util.RabbitMQSinkUtil;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * RabbitMQ Source implementation.
 */

@Extension(
        name = "rabbitmq",
        namespace = "source",
        description = "The rabbitmq source receives the events from the rabbitmq broker via the AMQP protocol. ",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "The URI that is used to connect to an AMQP server. If no URI is specified," +
                                "an error is logged in the CLI." +
                                "e.g.,\n" +
                                "`amqp://guest:guest`,\n" +
                                "`amqp://guest:guest@localhost:5672`",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "heartbeat",
                        description = "The period of time (in seconds) after which the peer TCP connection should " +
                                "be considered unreachable (down) by RabbitMQ and client libraries.",
                        type = {DataType.INT},
                        optional = true, defaultValue = "60"),
                @Parameter(
                        name = "exchange.name",
                        description = "The name of the exchange that decides what to do with a message it receives." +
                                "If the `exchange.name` already exists in the RabbitMQ server, then the system uses " +
                                "that `exchange.name` instead of redeclaring.",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "exchange.type",
                        description = "The type of the exchange name. The exchange types available are " +
                                "`direct`, `fanout`, `topic` and `headers`. For a detailed description of each " +
                                "type, see " +
                                "[RabbitMQ - AMQP Concepts](https://www.rabbitmq.com/tutorials/amqp-concepts.html). ",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "direct"),
                @Parameter(
                        name = "exchange.durable.enabled",
                        description = "If this is set to `true`, the exchange remains declared even if the broker" +
                                " restarts.",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "exchange.autodelete.enabled",
                        description = "If this is set to `true`, the exchange is automatically deleted when it is " +
                                "not used anymore. ",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "routing.key",
                        description = "The key based on which the exchange determines how to route the message to " +
                                "queues. The routing key is like an address for the message. The routing.key must " +
                                "be initialized when the value for the `exchange.type` parameter is `direct` " +
                                "or `topic`.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "empty"),
                @Parameter(
                        name = "headers",
                        description = "The headers of the message. The attributes used for routing are taken " +
                                "from the this paremeter. A message is considered matching if the value of " +
                                "the header equals the value specified upon binding. ",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "null"),
                @Parameter(
                        name = "queue.name",
                        description = "A queue is a buffer that stores messages. If the queue name already exists " +
                                "in the RabbitMQ server, then the system usees that queue name instead of " +
                                "redeclaring it. If no value is specified for this parameter, the system uses the " +
                                "unique queue name that is automatically generated by the RabbitMQ server. ",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "system generated queue name"),
                @Parameter(
                        name = "queue.durable.enabled",
                        description = "If this parameter is set to `true`, the queue remains declared even if the " +
                                "broker restarts",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "queue.exclusive.enabled",
                        description = "If this parameter is set to `true`, the queue is exclusive for the current " +
                                "connection. If it is set to `false`, it is also consumable by other connections. ",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "queue.autodelete.enabled",
                        description = "If this parameter is set to `true`, the queue is automatically deleted when " +
                                "it is not used anymore.",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "tls.enabled",
                        description = "This parameter specifies whether an encrypted communication channel should " +
                        "be established or not. When this parameter is set to `true`, the " +
                        "`tls.truststore.path` and `tls.truststore.password` parameters are initialized.",
                        type = {DataType.BOOL},
                        optional = true, defaultValue = "false"),
                @Parameter(
                        name = "tls.truststore.path",
                        description = "The file path to the location of the truststore of the client that receives " +
                        "the RabbitMQ events via the `AMQP` protocol. A custom client-truststore can be " +
                        "specified if required. If a custom truststore is not specified, then the system " +
                        "uses the default client-trustore in the `${carbon.home}/resources/security` " +
                        "directory.",
                        type = {DataType.STRING},
                        optional = true,  defaultValue = "${carbon.home}/resources/security/client-truststore.jks"),
                @Parameter(
                        name = "tls.truststore.password",
                        description = "The password for the client-truststore. A custom password can be specified " +
                                "if required. If no custom password is specified, then the system uses " +
                                "`wso2carbon` as the default password.",
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
                        syntax = "@App:name('TestExecutionPlan') \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n" +
                                "@info(name = 'query1') \n" +
                                "@source(type ='rabbitmq',\n" +
                                "uri = 'amqp://guest:guest@localhost:5672',\n" +
                                "exchange.name = 'direct',\n" +
                                "routing.key= 'direct',\n" +
                                "@map(type='xml'))\n" +
                                "Define stream BarStream (symbol string, price float, volume long);\n" +
                                "from FooStream select symbol, price, volume insert into BarStream;\n",
                        description = "This query receives events from the `direct` exchange with the `direct`" +
                                "exchange type, and the `directTest` routing key.")
        })
public class RabbitMQSource extends Source {
    private static final Logger log = Logger.getLogger(RabbitMQSource.class);
    private SourceEventListener sourceEventListener;
    private Connection connection = null;
    private int heartbeat;
    private String queueName;
    private boolean queueExclusive;
    private boolean queueDurable;
    private boolean queueAutodelete;
    private String listenerUri;
    private String tlsTruststoreLocation;
    private String tlsTruststorePassword;
    private String tlsTruststoreType;
    private String tlsVersion;
    private boolean tlsEnabled;
    private String exchangeName;
    private String exchangeType;
    private boolean exchangeDurable;
    private boolean exchangeAutoDelete;
    private String routingKey;
    private Map<String, Object> map = null;
    private FileInputStream fileInputStream = null;
    private RabbitMQConsumer rabbitMQConsumer;
    private String siddhiAppName;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] strings,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        this.sourceEventListener = sourceEventListener;
        this.listenerUri = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_SERVER_URI);
        this.heartbeat = Integer.parseInt(optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_HEARTBEAT,
                RabbitMQConstants.DEFAULT_HEARTBEAT));
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
        this.tlsEnabled = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_CONNECTION_TLS_ENABLED,
                        RabbitMQConstants.DEFAULT_EXCHANGE_TLS_ENABLED));
        this.queueName = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_QUEUENAME,
                RabbitMQConstants.EMPTY_STRING);
        if (!queueName.isEmpty()) {
            this.queueExclusive = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                    (RabbitMQConstants.RABBITMQ_QUEUE_EXCLUSIVE,
                            RabbitMQConstants.DEFAULT_QUEUE_EXCLUSIVE));
            this.queueDurable = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                    (RabbitMQConstants.RABBITMQ_QUEUE_DURABLE,
                            RabbitMQConstants.DEFAULT_QUEUE_DURABLE));
            this.queueAutodelete = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                    (RabbitMQConstants.RABBITMQ_QUEUE_AUTO_DELETE,
                            RabbitMQConstants.DEFAULT_QUEUE_AUTO_DELETE));
        }
        this.exchangeName = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_EXCHANGE_NAME);
        this.exchangeType = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_EXCHANGE_TYPE,
                RabbitMQConstants.DEFAULT_EXCHANGE_TYPE);
        this.exchangeDurable = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_EXCHANGE_DURABLE, RabbitMQConstants.DEFAULT_EXCHANGE_DURABLE));
        this.exchangeAutoDelete = Boolean.parseBoolean(optionHolder.validateAndGetStaticValue
                (RabbitMQConstants.RABBITMQ_EXCHANGE_AUTO_DELETE,
                        RabbitMQConstants.DEFAULT_EXCHANGE_AUTODELETE));

        routingKey = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_ROUTINGKEY,
                RabbitMQConstants.EMPTY_STRING);

        String headers = optionHolder.validateAndGetStaticValue(RabbitMQConstants.RABBITMQ_HEADERS,
                RabbitMQConstants.NULL);

        if (headers != null) {
            try {
                map = RabbitMQSinkUtil.getHeaders(headers);
            } catch (IOException e) {
                throw new SiddhiAppCreationException("Invalid header format. Please include as " +
                        "'key1:value1','key2:value2',..");
            }
        }
        this.sourceEventListener = sourceEventListener;
        if (!RabbitMQConstants.EXCHANGE_TYPE_FANOUT.equals(exchangeType)
                && !RabbitMQConstants.EXCHANGE_TYPE_DIRECT.equals(exchangeType)
                && !RabbitMQConstants.EXCHANGE_TYPE_TOPIC.equals(exchangeType)
                && !RabbitMQConstants.EXCHANGE_TYPE_HEADERS.equals(exchangeType)) {
            throw new SiddhiAppCreationException("Check the exchange type in " + this.sourceEventListener + ". " +
                    "There is no exchange type named as " + exchangeType + " in RabbitMQ");
        }

    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            URI uri = URI.create(listenerUri);
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
                        throw new SiddhiAppCreationException("The trustStore File path tls.truststore.path = " +
                                "" + tlsTruststoreLocation + " defined in " + sourceEventListener + " is incorrect." +
                                " Specify TrustStore location correctly.", e);
                    } catch (CertificateException e) {
                        throw new SiddhiAppCreationException("TrustStore is not specified in "
                                + sourceEventListener, e);
                    } catch (NoSuchAlgorithmException e) {
                        throw new SiddhiAppCreationException("Algorithm tls.version = " + tlsVersion + " defined in " +
                                "" + sourceEventListener + "is not available in TrustManagerFactory class.", e);
                    } catch (KeyStoreException e) {
                        throw new SiddhiAppCreationException("The trustStore type tls.truststore.type = " +
                                "" + tlsTruststoreType + " defined in " + sourceEventListener + " is incorrect." +
                                " Specify TrustStore type correctly.", e);
                    } catch (IOException e) {
                        throw new SiddhiAppCreationException("The trustStore type tls.truststore.password = " +
                                "" + tlsTruststorePassword + " defined in " + sourceEventListener + " is incorrect." +
                                " Specify TrustStore password correctly.", e);
                    } finally {
                        if (fileInputStream != null) {
                            fileInputStream.close();
                        }
                    }

                }
            }
            connection = factory.newConnection();
            rabbitMQConsumer = new RabbitMQConsumer();
            rabbitMQConsumer.consume(connection, exchangeName, exchangeType, exchangeDurable,
                    exchangeAutoDelete, queueName, queueExclusive, queueDurable, queueAutodelete, routingKey, map,
                    sourceEventListener, connectionCallback);
        } catch (IOException e) {
            throw new ConnectionUnavailableException(
                    "Failed to connect with the Rabbitmq server. Check the " +
                            "" + RabbitMQConstants.RABBITMQ_SERVER_URI + " = " + listenerUri + " defined in " +
                            "" + sourceEventListener, e);
        } catch (NoSuchAlgorithmException e) {
            throw new SiddhiAppCreationException(
                    "No such algorithm in the " + RabbitMQConstants.RABBITMQ_SERVER_URI + " = "
                            + listenerUri + " defined in " + sourceEventListener, e);
        } catch (URISyntaxException e) {
            throw new SiddhiAppCreationException(
                    "There is an invalid syntax in the " + RabbitMQConstants.RABBITMQ_SERVER_URI + " = "
                            + listenerUri + " defined in " + sourceEventListener, e);
        } catch (TimeoutException e) {
            throw new SiddhiAppCreationException(
                    "Timeout while connectiong with the RabbitMQ server", e);
        } catch (KeyManagementException e) {
            throw new SiddhiAppCreationException(
                    "There is an error in key management in the " + RabbitMQConstants.RABBITMQ_SERVER_URI + " = "
                            + listenerUri + " defined in " + sourceEventListener, e);
        } catch (Exception e) {
            throw new SiddhiAppCreationException("Error in receiving the message from the RabbitMQ broker " +
                    "in " + sourceEventListener, e);
        }
    }

        @Override
    public void disconnect() {
        if (connection != null) {
            try {
                if (connection.isOpen()) {
                    rabbitMQConsumer.closeChannel();
                    connection.close();
                    if (log.isDebugEnabled()) {
                        log.debug("Server connector for uri = " + listenerUri + " is disconnected in " +
                                "" + sourceEventListener + ".");
                    }
                }
            } catch (TimeoutException e) {
                log.error("Timeout while disconnecting the uri = " + listenerUri + " in " +
                        "" + sourceEventListener + ".");
            } catch (IOException e) {
                log.error("Error in disconnecting the uri = " + listenerUri + " in " +
                        "" + sourceEventListener + ".");
            } catch (Exception e) {
                log.error("Error occurred while closing the RabbitMQ consumer for the queue: "
                        + queueName + ". Respective Siddhi App name : " + siddhiAppName + " and stream ID : " +
                        sourceEventListener.getStreamDefinition().getId(), e);
            }
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public void pause() {
       rabbitMQConsumer.pause();
    }

    @Override
    public void resume() {
        rabbitMQConsumer.resume();
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> map) {

    }
}
