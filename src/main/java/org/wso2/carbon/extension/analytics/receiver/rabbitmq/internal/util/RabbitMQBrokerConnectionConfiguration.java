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

import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;

public class RabbitMQBrokerConnectionConfiguration {

    private String username = null;
    private String password = null;
    private String hostName;
    private String port;
    private String exchangeDurable;
    private boolean autoAck;
    private String durable;
    private String exclusive;
    private String autoDelete;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getExclusive() {
        return exclusive;
    }

    public void setExclusive(String exclusive) {
        this.exclusive = exclusive;
    }

    public String getExchangeDurable() {
        return exchangeDurable;
    }

    public void setExchangeDurable(String exchangeDurable) {
        this.exchangeDurable = exchangeDurable;
    }

    public String getDurable() {
        return durable;
    }

    public void setDurable(String durable) {
        this.durable = durable;
    }

    public boolean getAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public String getAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(String autoDelete) {
        this.autoDelete = autoDelete;
    }

    public RabbitMQBrokerConnectionConfiguration(InputEventAdapterConfiguration eventAdapterConfiguration) {

        this.username = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_USERNAME);
        this.port = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PORT);
        this.password = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PASSWORD);
        this.hostName = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME);
        this.exclusive = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE);
        this.durable = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_DURABLE);
        this.autoDelete = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE);
        this.exchangeDurable = eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE);

        if (eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK) != null) {
            this.autoAck = Boolean.parseBoolean(eventAdapterConfiguration.getProperties().get(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK));
        }
    }
}