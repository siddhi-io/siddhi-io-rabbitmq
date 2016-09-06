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

public final class RabbitMQEventAdapterConstants {

    private RabbitMQEventAdapterConstants() {
    }

    public static final String ADAPTER_TYPE_RABBITMQ = "rabbitmq";
    public static final String RABBITMQ_SERVER_HOST_NAME = "hostname";
    public static final String RABBITMQ_SERVER_HOST_NAME_HINT = "hostname.hint";
    public static final String RABBITMQ_SERVER_PORT = "port";
    public static final String RABBITMQ_SERVER_PORT_HINT = "port.hint";
    public static final String RABBITMQ_SERVER_USERNAME = "userName";
    public static final String RABBITMQ_SERVER_USERNAME_HINT = "userName.hint";
    public static final String RABBITMQ_SERVER_PASSWORD = "password";
    public static final String RABBITMQ_SERVER_PASSWORD_HINT = "password.hint";
    public static final String RABBITMQ_QUEUE_NAME = "queue.Name";
    public static final String RABBITMQ_QUEUE_NAME_HINT = "queue.Name.hint";
    public static final String RABBITMQ_EXCHANGE_NAME = "exchange.Name";
    public static final String RABBITMQ_EXCHANGE_NAME_HINT = "exchange.Name.hint";
    public static final String RABBITMQ_QUEUE_DURABLE = "queue.Durable";
    public static final String RABBITMQ_QUEUE_DURABLE_HINT = "queue.Durable.hint";
    public static final String RABBITMQ_QUEUE_EXCLUSIVE = "queue.Exclusive";
    public static final String RABBITMQ_QUEUE_EXCLUSIVE_HINT = "queue.Exclusive.hint";
    public static final String RABBITMQ_QUEUE_AUTO_DELETE = "queue.Autodelete";
    public static final String RABBITMQ_QUEUE_AUTO_DELETE_HINT = "queue.AutoDelete.hint";
    public static final String RABBITMQ_QUEUE_AUTO_ACK = "queue.Autoack";
    public static final String RABBITMQ_QUEUE_AUTO_ACK_HINT = "queue.AutoAck.hint";
    public static final String RABBITMQ_QUEUE_ROUTING_KEY = "queue.RoutingKey";
    public static final String RABBITMQ_QUEUE_ROUTING_KEY_HINT = "queue.RoutingKey.hint";
    public static final String RABBITMQ_EXCHANGE_TYPE = "exchange.Type";
    public static final String RABBITMQ_EXCHANGE_TYPE_HINT = "exchange.Type.hint";
    public static final String RABBITMQ_EXCHANGE_DURABLE = "exchange.Durable";
    public static final String RABBITMQ_EXCHANGE_DURABLE_HINT = "exchange.durable.hint";
    public static final String RABBITMQ_EXCHANGE_AUTO_DELETE = "exchange.Autodelete";
    public static final String RABBITMQ_EXCHANGE_AUTO_DELETE_HINT = "exchange.AutoDelete.hint";
    public static final String RABBITMQ_CONNECTION_RETRY_COUNT = "connection.RetryCount";
    public static final String RABBITMQ_CONNECTION_RETRY_COUNT_HINT = "connection.RetryCount.hint";
    public static final String RABBITMQ_CONNECTION_RETRY_INTERVAL = "connection.RetryInterval";
    public static final String RABBITMQ_CONNECTION_RETRY_INTERVAL_HINT = "connection.RetryInterval.hint";
    public static final String RABBITMQ_SERVER_VIRTUAL_HOST = "server.VirtualHost";
    public static final String RABBITMQ_SERVER_VIRTUAL_HOST_HINT = "server.VirtualHost.hint";
    public static final String RABBITMQ_FACTORY_HEARTBEAT = "factory.Heartbeat";
    public static final String RABBITMQ_FACTORY_HEARTBEAT_HINT = "factory.Heartbeat.hint";
    public static final String RABBITMQ_CONNECTION_SSL_ENABLED = "connection.sslEnabled";
    public static final String RABBITMQ_CONNECTION_SSL_ENABLED_HINT = "connection.ssl.hint";
    public static final String RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION = "connection.ssl.keystore.Location";
    public static final String RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE = "connection.ssl.keystore.Type";
    public static final String RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD = "connection.ssl.keystore.Password";
    public static final String RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION = "connection.ssl.truststore.Location";
    public static final String RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE = "connection.ssl.truststore.Type";
    public static final String RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD = "connection.ssl.truststore.Password";
    public static final String RABBITMQ_CONNECTION_SSL_VERSION = "connection.ssl.Version";
    public static final String CONSUMER_TAG = "consumer.tag";
    public static final String CONSUMER_TAG_HINT = "consumer.tag.hint";
    public static final int DEFAULT_RETRY_INTERVAL = 30000;
    public static final int DEFAULT_RETRY_COUNT = 3;
}