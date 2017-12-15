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

package org.wso2.extension.siddhi.io.rabbitmq.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.util.config.ConfigReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * {@code RabbitMQSinkUtil } responsible of the all configuration reading and input formatting of rabbitmq broker.
 */

public class RabbitMQSinkUtil {
    private static final Logger log = Logger.getLogger(RabbitMQSinkUtil.class);


    public static void declareExchange(Connection connection, Channel channel, String exchangeName,
                                       String exchangeType, boolean exchangeDurable,
                                       boolean exchangeAutodelete) throws IOException, TimeoutException {
        try {
            channel.exchangeDeclare(exchangeName,
                    exchangeType,
                    exchangeDurable,
                    exchangeAutodelete,
                    null);

        } catch (IOException e) {
            log.error("Error occurred while declaring the exchange - " + removeCRLFCharacters(exchangeName) + ".", e);
        }
    }

    public static void declareQueue(Connection connection, Channel channel, String queueName, boolean queueDurable,
                                    boolean queueAutodelete, boolean queueExclusive)
            throws IOException, TimeoutException {
        try {
            channel.queueDeclare(queueName, queueDurable, queueExclusive, queueAutodelete, null);
        } catch (IOException e) {
            log.error("Error occurred while declaring the queue " + removeCRLFCharacters(queueName) + ".", e);
        }
    }

    public static String getTrustStorePath(ConfigReader sinkConfigReader) {
        // return sinkConfigReader.readConfig(RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_LOCATION,
        //RabbitMQConstants.TRUSTSTORE_FILE_VALUE);
        return sinkConfigReader.readConfig(RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_LOCATION,
                RabbitMQConstants.EMPTY_STRING);
    }

    public static String getTrustStorePassword(ConfigReader sinkConfigReader) {
        return sinkConfigReader.readConfig(RabbitMQConstants.RABBITMQ_CONNECTION_TLS_TRUSTSTORE_PASSWORD,
                RabbitMQConstants.TRUSTSTORE_PASSWORD_VALUE);
    }

    public static Map<String, Object> getHeaders(String headers) throws IOException {
        headers = headers.trim();
        headers = headers.substring(1, headers.length() - 1);
        Map<String, Object> map = new HashMap<String, Object>();
        if (!headers.isEmpty()) {
            String[] spam = headers.split(RabbitMQConstants.HEADER_SPLITTER);
            for (String aSpam : spam) {
                String[] header = aSpam.split(RabbitMQConstants.HEADER_NAME_VALUE_SPLITTER, 2);
                if (header.length > 1) {
                    map.put(header[0], header[1]);
                } else {
                    throw new IOException(
                            "Invalid header format. Please include as 'key1:value1','key2:value2',..");
                }
            }
        }
        return map;
    }

    private static String removeCRLFCharacters(String str) {
        if (str != null) {
            str = str.replace('\n', '_').replace('\r', '_');
        }
        return str;
    }
}

