/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.extension.analytics.receiver.rabbitmq.internal.util;

import com.rabbitmq.client.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * This is the listener which directly interacts with the external RabbitMQ broker and making connection with RabbitMQ
 * broker and listen messages from Broker.
 */
public class RabbitMQAdapterListener implements Runnable {
    private static final Log log = LogFactory.getLog(RabbitMQAdapterListener.class);
    private ConnectionFactory connectionFactory;
    private Connection connection = null;
    private RabbitMQBrokerConnectionConfiguration rabbitmqBrokerConnectionConfiguration;
    private int tenantId;
    private boolean connectionSucceeded = false;
    private Channel channel = null;
    private QueueingConsumer queueingConsumer;
    private String queueName, routeKey, exchangeName;
    private String exchangeType;
    private String consumerTagString;
    private String adapterName;
    private final int STATE_STOPPED = 0;
    private final int STATE_STARTED;
    private int workerState;
    private int retryInterval = RabbitMQEventAdapterConstants.DEFAULT_RETRY_INTERVAL;
    private int retryCountMax = RabbitMQEventAdapterConstants.DEFAULT_RETRY_COUNT;
    private InputEventAdapterListener eventAdapterListener = null;

    public RabbitMQAdapterListener(RabbitMQBrokerConnectionConfiguration rabbitmqBrokerConnectionConfiguration,
                                   InputEventAdapterConfiguration eventAdapterConfiguration,
                                   InputEventAdapterListener inputEventAdapterListener) {

        connectionFactory = new ConnectionFactory();
        this.rabbitmqBrokerConnectionConfiguration = rabbitmqBrokerConnectionConfiguration;
        this.queueName = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_NAME);
        this.exchangeName = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_NAME);
        this.exchangeType = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE);
        this.routeKey = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY);
        this.consumerTagString = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.CONSUMER_TAG);
        this.adapterName = eventAdapterConfiguration.getName();
        this.eventAdapterListener = inputEventAdapterListener;
        this.tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
        workerState = STATE_STOPPED;
        STATE_STARTED = 1;
        if (routeKey == null) {
            routeKey = queueName;
        }
        if (!eventAdapterConfiguration.getProperties().
                get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED).equals("false")) {
            try {
                boolean sslEnabled = Boolean.parseBoolean(eventAdapterConfiguration.getProperties().
                        get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED));
                if (sslEnabled) {
                    String keyStoreLocation = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION);
                    String keyStoreType = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE);
                    String keyStorePassword = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD);
                    String trustStoreLocation = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION);
                    String trustStoreType = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE);
                    String trustStorePassword = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD);
                    String sslVersion = eventAdapterConfiguration.getProperties().
                            get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_VERSION);

                    if (StringUtils.isEmpty(keyStoreLocation) || StringUtils.isEmpty(keyStoreType) ||
                            StringUtils.isEmpty(keyStorePassword) || StringUtils.isEmpty(trustStoreLocation) ||
                            StringUtils.isEmpty(trustStoreType) || StringUtils.isEmpty(trustStorePassword)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Truststore and keystore information is not provided");
                        }
                        if (StringUtils.isNotEmpty(sslVersion)) {
                            connectionFactory.useSslProtocol(sslVersion);
                        } else {
                            log.info("Proceeding with default SSL configuration");
                            connectionFactory.useSslProtocol();
                        }
                    } else {
                        char[] keyPassphrase = keyStorePassword.toCharArray();
                        KeyStore ks = KeyStore.getInstance(keyStoreType);
                        ks.load(new FileInputStream(keyStoreLocation), keyPassphrase);

                        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(ks, keyPassphrase);

                        char[] trustPassphrase = trustStorePassword.toCharArray();
                        KeyStore tks = KeyStore.getInstance(trustStoreType);
                        tks.load(new FileInputStream(trustStoreLocation), trustPassphrase);

                        TrustManagerFactory tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        tmf.init(tks);

                        SSLContext context = SSLContext.getInstance(sslVersion);
                        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                        connectionFactory.useSslProtocol(context);
                    }
                }
            } catch (IOException e) {
                handleException("TrustStore or KeyStore File path is incorrect. Specify KeyStore location or " +
                        "TrustStore location Correctly.", e);
            } catch (CertificateException e) {
                handleException("TrustStore or keyStore is not specified. So Security certificate" +
                        " Exception happened.  ", e);
            } catch (NoSuchAlgorithmException e) {
                handleException("Algorithm is not available in KeyManagerFactory class.", e);
            } catch (UnrecoverableKeyException e) {
                handleException("Unable to recover Key", e);
            } catch (KeyStoreException e) {
                handleException("Error in KeyStore or TrustStore Type", e);
            } catch (KeyManagementException e) {
                handleException("Error in Key Management", e);
            }
        }

        if (!StringUtils.isEmpty(eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.
                RABBITMQ_FACTORY_HEARTBEAT))) {
            try {
                int heartbeatValue = Integer.parseInt(eventAdapterConfiguration.getProperties().
                        get(RabbitMQEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT));
                connectionFactory.setRequestedHeartbeat(heartbeatValue);
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading heartbeat value. Proceeding with default");
            }
        }
        connectionFactory.setHost(rabbitmqBrokerConnectionConfiguration.getHostName());
        try {
            int port = Integer.parseInt(rabbitmqBrokerConnectionConfiguration.getPort());
            if (port > 0) {
                connectionFactory.setPort(port);
            }
        } catch (NumberFormatException e) {
            handleException("Number format error in port number", e);
        }
        connectionFactory.setUsername(rabbitmqBrokerConnectionConfiguration.getUsername());
        connectionFactory.setPassword(rabbitmqBrokerConnectionConfiguration.getPassword());
        if (!StringUtils.isEmpty(eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.
                RABBITMQ_SERVER_VIRTUAL_HOST))) {
            connectionFactory.setVirtualHost(eventAdapterConfiguration.getProperties().
                    get(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST));
        }
        if (!StringUtils.isEmpty(eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.
                RABBITMQ_CONNECTION_RETRY_COUNT))) {
            try {
                retryCountMax = Integer.parseInt(eventAdapterConfiguration.getProperties().
                        get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT));
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading retry count value. Proceeding with default value (3)", e);
            }
        }
        if (!StringUtils.isEmpty(eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.
                RABBITMQ_CONNECTION_RETRY_INTERVAL))) {
            try {
                retryInterval = Integer.parseInt(eventAdapterConfiguration.getProperties().
                        get(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL));
            } catch (NumberFormatException e) {
                log.warn("Number format error in reading retry interval value. Proceeding with default value" +
                        " (30000ms)", e);
            }
        }
    }

    /**
     * Create a queue consumer using the properties from receiver configuration
     */
    private void initListener() {
        if (log.isDebugEnabled()) {
            log.debug("Initializing RabbitMQ consumer for Event Adaptor" + adapterName);
        }
        try {
            connection = getConnection();
            channel = openChannel();
            //declaring queue
            RabbitMQUtils.declareQueue(connection, queueName, rabbitmqBrokerConnectionConfiguration.getDurable(),
                    rabbitmqBrokerConnectionConfiguration.getExclusive(), rabbitmqBrokerConnectionConfiguration.
                            getAutoDelete());
            //declaring exchange
            RabbitMQUtils.declareExchange(connection, exchangeName, exchangeType, rabbitmqBrokerConnectionConfiguration.
                    getExchangeDurable());
            channel.queueBind(queueName, exchangeName, routeKey);
            if (log.isDebugEnabled()) {
                log.debug("Bind queue '" + queueName + "' to exchange '" + exchangeName +
                        "' with route key '" + routeKey + "'");
            }
            queueingConsumer = new QueueingConsumer(channel);
            if (consumerTagString != null) {
                channel.basicConsume(queueName, rabbitmqBrokerConnectionConfiguration.getAutoAck(), consumerTagString,
                        queueingConsumer);
                if (log.isDebugEnabled()) {
                    log.debug("Start consuming queue '" + queueName + "' with consumer tag '" + consumerTagString +
                            "' for receiver " + adapterName);
                }
            } else {
                consumerTagString = channel.basicConsume(queueName, rabbitmqBrokerConnectionConfiguration.getAutoAck(),
                        queueingConsumer);
                if (log.isDebugEnabled()) {
                    log.debug("Start consuming queue '" + queueName + "' with consumer tag '" + consumerTagString +
                            "' for receiver" + adapterName);
                }
            }
        } catch (IOException e) {
            openChannel();
        }
    }

    /**
     * Used to start message consuming messages. This method is called in startup and when
     * connection is re-connected. This method will request for the connection and create
     * channel, queues, exchanges and bind queues to exchanges before consuming messages
     *
     * @throws ShutdownSignalException
     */
    public void startListener() throws ShutdownSignalException {
        try {
            connection = getConnection();
            while (connection.isOpen()) {
                try {
                    if (!channel.isOpen()) {
                        channel = queueingConsumer.getChannel();
                    }
                    channel.txSelect();
                } catch (IOException e) {
                    handleException("Error while starting transaction", e);
                }
                boolean successful = false;
                QueueingConsumer.Delivery delivery = null;

                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for next delivery from queue for receiver ");
                    }
                    delivery = queueingConsumer.nextDelivery();
                } catch (InterruptedException e) {
                    handleException("Interrupt when consuming messages", e);
                }
                try {
                    if (delivery != null) {
                        AMQP.BasicProperties properties = delivery.getProperties();
                        String msgText = new String(delivery.getBody(), properties.getContentEncoding());
                        PrivilegedCarbonContext.startTenantFlow();
                        PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantId(tenantId);
                        eventAdapterListener.onEvent(msgText);
                        successful = true;
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Queue delivery item is null for receiver " + adapterName);
                        }
                    }
                } catch (UnsupportedEncodingException e) {
                    handleException("Unsupported Encoding method", e);
                } finally {
                    PrivilegedCarbonContext.endTenantFlow();
                    if (successful) {
                        try {
                            if (!rabbitmqBrokerConnectionConfiguration.getAutoAck()) {
                                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                                channel.txCommit();
                            }
                        } catch (IOException e) {
                            handleException("Error while committing transaction", e);
                        }
                    } else {
                        try {
                            if (!rabbitmqBrokerConnectionConfiguration.getAutoAck()) {
                                channel.txRollback();
                            }
                        } catch (IOException e) {
                            handleException("Error while trying to roll back transaction", e);
                        }
                    }
                }
            }
        } catch (IOException e) {
            openChannel();
        }
    }

    /**
     * Close the connection and channel
     *
     * @param adapterName Name of the Adapter
     */
    public void stopListener(String adapterName) {

        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
                log.info("RabbitMQ connection closed for receiver " + adapterName);
            } catch (IOException e) {
                handleException("Error while closing RabbitMQ connection with the event adapter" + adapterName, e);
            } finally {
                connection = null;
            }
        }
        workerState = STATE_STOPPED;
        connectionSucceeded = true;
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                handleException("Error while closing RabbitMQ channel with the event adapter " + adapterName, e);
            } finally {
                channel = null;
            }
        }
    }

    /**
     * Making connection to the rabbitMQ broker
     *
     * @throws IOException
     */
    private Connection makeConnection() throws IOException {
        Connection connection = null;
        try {
            connection = RabbitMQUtils.createConnection(connectionFactory);
            log.info("Successfully connected to RabbitMQ Broker " + adapterName);
        } catch (IOException e) {
            handleException("Error creating connection to RabbitMQ Broker. Reattempting to connect.", e);
            int retryC = 0;
            while ((connection == null) && (workerState == STATE_STARTED) && ((retryCountMax == -1) || (retryC < retryCountMax))) {
                retryC++;
                log.info("Attempting to create connection to RabbitMQ Broker" + adapterName +
                        " in " + retryInterval + " ms");
                try {
                    Thread.sleep(retryInterval);
                    connection = RabbitMQUtils.createConnection(connectionFactory);
                    log.info("Successfully connected to RabbitMQ Broker" + adapterName);
                } catch (InterruptedException e1) {
                    handleException("Thread has been interrupted while trying to reconnect to RabbitMQ Broker " +
                            adapterName, e1);
                }
            }
            if (connection == null) {
                handleException("Could not connect to RabbitMQ Broker" + adapterName + "Error while creating " +
                        "connection", e);
            }
        }
        return connection;
    }

    /**
     * Check connection is available or not and calling method to create connection
     *
     * @throws IOException
     */
    private Connection getConnection() throws IOException {
        if (connection == null) {
            connection = makeConnection();
            connectionSucceeded = true;
        }
        return connection;
    }

    /**
     * Start the thread of adapter
     */
    public void createConnection() {
        new Thread(this).start();
    }

    /**
     * Initialize the Adapter
     */
    @Override
    public void run() {
        while (!connectionSucceeded) {
            try {
                workerState = STATE_STARTED;
                initListener();
                while (workerState == STATE_STARTED) {
                    try {
                        startListener();
                    } catch (ShutdownSignalException sse) {
                        if (!sse.isInitiatedByApplication()) {
                            log.error("RabbitMQ Listener of the receiver" + adapterName + "was disconnected", sse);
                            waitForConnection();
                        }
                    }
                }
                connectionSucceeded = true;
                if (log.isDebugEnabled()) {
                    log.debug("RabbitMQ Connection successful in " + adapterName);
                }
            } finally {
                stopListener(adapterName);
                workerState = STATE_STOPPED;
            }
        }
    }

    /**
     * Handle the exception by throwing the exception
     *
     * @param msg Error message of the exception.
     * @param e   Exception Object
     */
    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new RabbitMQException(msg, e);
    }

    /**
     * Handle the exception by throwing the exception
     *
     * @param msg Error message of the exception.
     */
    private void handleException(String msg) {
        log.error(msg);
        throw new RabbitMQException(msg);
    }

    /**
     * Wait For Connection if any connection lost happened
     */
    private void waitForConnection() {
        int retryCount = 0;
        while (!connection.isOpen() && (workerState == STATE_STARTED)
                && ((retryCountMax == -1) || (retryCount < retryCountMax))) {
            retryCount++;
            log.info("Attempting to reconnect to RabbitMQ Broker for the receiver " + adapterName + " in" +
                    retryInterval + " ms");
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                handleException("Error while trying to reconnect to RabbitMQ Broker for the receiver " + adapterName, e);
            }
        }
        if (connection.isOpen()) {
            log.info("Successfully reconnected to RabbitMQ Broker for the receiver " + adapterName);
            initListener();
        } else {
            handleException("Could not reconnect to the RabbitMQ Broker for the receiver " + adapterName + ". " +
                    "Connection is closed.");
        }
    }

    /**
     * Open the Channel
     */
    private Channel openChannel() {
        try {
            if (channel == null || !channel.isOpen()) {
                channel = connection.createChannel();
                if (log.isDebugEnabled()) {
                    log.debug("Channel is not open. Creating a new channel for receiver " + adapterName);
                }
            }
        } catch (IOException e) {
            handleException("Error in creating Channel", e);
        }
        return channel;
    }
}
