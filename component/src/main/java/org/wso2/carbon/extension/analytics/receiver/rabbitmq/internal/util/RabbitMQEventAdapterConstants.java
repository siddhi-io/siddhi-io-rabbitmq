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

/**
 * This class represents RabbitMQ specific Constants.
 */
public final class RabbitMQEventAdapterConstants {
    public static final String RESOURCES = "Resources";
    /**
     * Name of the receiver.
     */
    public static final String ADAPTER_TYPE_RABBITMQ = "rabbitmq";
    /**
     * Host name of the Rabbitmq server.
     */
    public static final String RABBITMQ_SERVER_HOST_NAME = "hostname";
    /**
     * Hint for the host name of the Rabbitmq server which shows in management console.
     */
    public static final String RABBITMQ_SERVER_HOST_NAME_HINT = "hostname.hint";
    /**
     * Port of the server.
     */
    public static final String RABBITMQ_SERVER_PORT = "port";
    /**
     * Hint for the port of the Rabbitmq server which shows in management console.
     */
    public static final String RABBITMQ_SERVER_PORT_HINT = "port.hint";
    /**
     * Username of the broker.
     */
    public static final String RABBITMQ_SERVER_USERNAME = "userName";
    /**
     * Hint for the username of the broker which shows in management console.
     */
    public static final String RABBITMQ_SERVER_USERNAME_HINT = "userName.hint";
    /**
     * Password of the broker.
     */
    public static final String RABBITMQ_SERVER_PASSWORD = "password";
    /**
     * Hint for the password of the broker which shows in management console.
     */
    public static final String RABBITMQ_SERVER_PASSWORD_HINT = "password.hint";
    /**
     * A queue is a buffer that stores messages. Name of the queue.
     */
    public static final String RABBITMQ_QUEUE_NAME = "queue.Name";
    /**
     * Hint for the queue name of the broker which shows in management console.
     */
    public static final String RABBITMQ_QUEUE_NAME_HINT = "queue.Name.hint";
    /**
     * The exchange know exactly what to do with a message it receives.
     */
    public static final String RABBITMQ_EXCHANGE_NAME = "exchange.Name";
    /**
     * Hint for the exchange name of the broker which shows in management console.
     */
    public static final String RABBITMQ_EXCHANGE_NAME_HINT = "exchange.Name.hint";
    /**
     * Whether the queue should remain declared even if the broker restarts.
     */
    public static final String RABBITMQ_QUEUE_DURABLE = "queue.Durable";
    /**
     * Hint for the durable of the broker which shows in management console.
     */
    public static final String RABBITMQ_QUEUE_DURABLE_HINT = "queue.Durable.hint";
    /**
     * Whether the queue should be exclusive or should be consumable by other connections.
     */
    public static final String RABBITMQ_QUEUE_EXCLUSIVE = "queue.Exclusive";
    /**
     * Hint for the exclusive of the broker which shows in management console.
     */
    public static final String RABBITMQ_QUEUE_EXCLUSIVE_HINT = "queue.Exclusive.hint";
    /**
     * Whether to keep the queue even if it is not being consumed anymore.
     */
    public static final String RABBITMQ_QUEUE_AUTO_DELETE = "queue.Autodelete";
    /**
     * Hint for the auto delete property of the broker which shows in management console.
     */
    public static final String RABBITMQ_QUEUE_AUTO_DELETE_HINT = "queue.AutoDelete.hint";
    /**
     * Whether to send back an acknowledgement.
     */
    public static final String RABBITMQ_QUEUE_AUTO_ACK = "queue.Autoack";
    /**
     * Hint for the auto ack property of the broker which shows in management console.
     */
    public static final String RABBITMQ_QUEUE_AUTO_ACK_HINT = "queue.AutoAck.hint";
    /**
     * The routing key is like an address for the message.
     */
    public static final String RABBITMQ_QUEUE_ROUTING_KEY = "queue.RoutingKey";
    /**
     * Hint for the routing key property of the broker which shows in management console.
     */
    public static final String RABBITMQ_QUEUE_ROUTING_KEY_HINT = "queue.RoutingKey.hint";
    /**
     * The type of the exchange.
     */
    public static final String RABBITMQ_EXCHANGE_TYPE = "exchange.Type";
    /**
     * Hint for the exchange type property of the broker which shows in management console.
     */
    public static final String RABBITMQ_EXCHANGE_TYPE_HINT = "exchange.Type.hint";
    /**
     * Whether the exchange should remain declared even if the broker restarts.
     */
    public static final String RABBITMQ_EXCHANGE_DURABLE = "exchange.Durable";
    /**
     * Hint for the exchange durable property of the broker which shows in management console.
     */
    public static final String RABBITMQ_EXCHANGE_DURABLE_HINT = "exchange.durable.hint";
    /**
     * Whether to keep the queue even if it is not used anymore.
     */
    public static final String RABBITMQ_EXCHANGE_AUTO_DELETE = "exchange.Autodelete";
    /**
     * Hint for the exchange auto delete property of the broker which shows in management console.
     */
    public static final String RABBITMQ_EXCHANGE_AUTO_DELETE_HINT = "exchange.AutoDelete.hint";
    /**
     * Retry count to connect.
     */
    public static final String RABBITMQ_CONNECTION_RETRY_COUNT = "connection.RetryCount";
    /**
     * Hint for the retry count property of the broker which shows in management console.
     */
    public static final String RABBITMQ_CONNECTION_RETRY_COUNT_HINT = "connection.RetryCount.hint";
    /**
     * Retry interval to connect.
     */
    public static final String RABBITMQ_CONNECTION_RETRY_INTERVAL = "connection.RetryInterval";
    /**
     * Hint for the retry interval property of the broker which shows in management console.
     */
    public static final String RABBITMQ_CONNECTION_RETRY_INTERVAL_HINT = "connection.RetryInterval.hint";
    /**
     * A Virtual host provide a way to segregate applications using the same RabbitMQ instance.
     */
    public static final String RABBITMQ_SERVER_VIRTUAL_HOST = "server.VirtualHost";
    /**
     * Hint for the virtual host property of the broker which shows in management console.
     */
    public static final String RABBITMQ_SERVER_VIRTUAL_HOST_HINT = "server.VirtualHost.hint";
    /**
     * Ensure that the application layer promptly finds out about disrupted connections.
     */
    public static final String RABBITMQ_FACTORY_HEARTBEAT = "factory.Heartbeat";
    /**
     * Hint for the virtual host property of the broker which shows in management console.
     */
    public static final String RABBITMQ_FACTORY_HEARTBEAT_HINT = "factory.Heartbeat.hint";
    /**
     * Used simply to establish an encrypted communication channel.
     */
    public static final String RABBITMQ_CONNECTION_SSL_ENABLED = "connection.sslEnabled";
    /**
     * Hint for the ssl property of the broker which shows in management console.
     */
    public static final String RABBITMQ_CONNECTION_SSL_ENABLED_HINT = "connection.ssl.hint";
    /**
     * Location of key store.
     */
    public static final String RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION = "connection.ssl.keystore.Location";
    /**
     * Type of the key store.
     */
    public static final String RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE = "connection.ssl.keystore.Type";
    /**
     * Password of the key store.
     */
    public static final String RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD = "connection.ssl.keystore.Password";
    /**
     * Location of the trust store.
     */
    public static final String RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION = "connection.ssl.truststore.Location";
    /**
     * Type of the trust store.
     */
    public static final String RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE = "connection.ssl.truststore.Type";
    /**
     * Password of the trust store.
     */
    public static final String RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD = "connection.ssl.truststore.Password";
    /**
     * Version of the SSL.
     */
    public static final String RABBITMQ_CONNECTION_SSL_VERSION = "connection.ssl.Version";
    /**
     * The consumer tag is local to a channel, so two clients can use the same consumer tags.
     * If this field is empty the server will generate a unique tag.
     */
    public static final String CONSUMER_TAG = "consumer.tag";
    /**
     * Hint for the consumer tag property of the broker which shows in management console.
     */
    public static final String CONSUMER_TAG_HINT = "consumer.tag.hint";
    /**
     * Default retry interval for connection.
     */
    public static final int DEFAULT_RETRY_INTERVAL = 30000;
    /**
     * Default retry count for connection.
     */
    public static final int DEFAULT_RETRY_COUNT = 3;
    /**
     * Default exchange type for connection.
     */
    public static final String DEFAULT_EXCHANGE_TYPE = "direct";
}