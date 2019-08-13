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

package io.siddhi.extension.io.rabbitmq.util;

/**
 * This class represents RabbitMQ specific Constants.
 */

public class RabbitMQConstants {
    private RabbitMQConstants() {
    }

    public static final String RABBITMQ_SERVER_URI = "uri";
    public static final String EMPTY_STRING = "";
    public static final String NULL = null;
    public static final String RABBITMQ_HEARTBEAT = "heartbeat";
    public static final String RABBITMQ_EXCHANGE_NAME = "exchange.name";
    public static final String RABBITMQ_EXCHANGE_TYPE = "exchange.type";
    public static final String RABBITMQ_ROUTINGKEY = "routing.key";
    public static final String RABBITMQ_EXCHANGE_DURABLE = "exchange.durable.enabled";
    public static final String RABBITMQ_EXCHANGE_AUTO_DELETE = "exchange.autodelete.enabled";
    public static final String RABBITMQ_CONNECTION_TLS_ENABLED = "tls.enabled";
    public static final String RABBITMQ_CONNECTION_TLS_TRUSTSTORE_LOCATION = "tls.truststore.path";
    //public static final String TRUSTSTORE_FILE_VALUE = "${carbon.home}/resources/security/client-truststore.jks";
    public static final String RABBITMQ_CONNECTION_TLS_TRUSTSTORE_TYPE = "tls.truststore.Type";
    public static final String RABBITMQ_CONNECTION_TLS_VERSION = "tls.version";
    public static final String RABBITMQ_CONNECTION_TLS_TRUSTSTORE_PASSWORD = "tls.truststore.password";
    public static final String TRUSTSTORE_PASSWORD_VALUE = "wso2carbon";
    public static final String RABBITMQ_DELIVERY_MODE = "delivery.mode";
    public static final String RABBITMQ_CONTENT_TYPE = "content.type";
    public static final String RABBITMQ_CONTENT_ENCODING = "content.encoding";
    public static final String RABBITMQ_PRIORITY = "priority";
    public static final String RABBITMQ_CORRELATION_ID = "correlation.id";
    public static final String RABBITMQ_REPLY_TO = "reply.to";
    public static final String RABBITMQ_EXPIRATION = "expiration";
    public static final String RABBITMQ_TIMESTAMP = "timestamp";
    public static final String RABBITMQ_MESSAGE_ID = "message.id";
    public static final String RABBITMQ_USER_ID = "user.id";
    public static final String RABBITMQ_APP_ID = "app.id";
    public static final String RABBITMQ_TYPE = "type";
    public static final String DEFAULT_PRIORITY = "0";
    public static final String DEFAULT_HEARTBEAT = "60";
    public static final String DEFAULT_DELIVERY_MODE = "1";
    public static final String RABBITMQ_HEADERS = "headers";
    public static final String DEFAULT_EXCHANGE_TYPE = "direct";
    public static final String DEFAULT_EXCHANGE_DURABLE = "false";
    public static final String DEFAULT_EXCHANGE_AUTODELETE = "false";
    public static final String DEFAULT_EXCHANGE_TLS_ENABLED = "false";
    public static final String HEADER_SPLITTER = "','";
    public static final String HEADER_NAME_VALUE_SPLITTER = ":";
    public static final String DEFAULT_TLS_TRUSTSTORE_TYPE = "JKS";
    public static final String DEFAULT_TLS_VERSION = "SSL";
    public static final String EXCHANGE_TYPE_DIRECT = "direct";
    public static final String EXCHANGE_TYPE_FANOUT = "fanout";
    public static final String EXCHANGE_TYPE_TOPIC = "topic";
    public static final String EXCHANGE_TYPE_HEADERS = "headers";
    // variables for source
    public static final String RABBITMQ_QUEUENAME = "queue.name";
    public static final String RABBITMQ_QUEUE_DURABLE = "queue.durable.enabled";
    public static final String RABBITMQ_QUEUE_AUTO_DELETE = "queue.autodelete.enabled";
    public static final String RABBITMQ_QUEUE_EXCLUSIVE = "queue.exclusive.enabled";
    public static final String DEFAULT_QUEUE_DURABLE = "false";
    public static final String DEFAULT_QUEUE_AUTO_DELETE = "false";
    public static final String DEFAULT_QUEUE_EXCLUSIVE = "false";
}
