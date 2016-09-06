/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.wso2.carbon.extension.analytics.receiver.rabbitmq;

import org.wso2.carbon.event.input.adapter.core.InputEventAdapter;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterConfiguration;
import org.wso2.carbon.event.input.adapter.core.InputEventAdapterListener;
import org.wso2.carbon.event.input.adapter.core.exception.TestConnectionNotSupportedException;
import org.wso2.carbon.extension.analytics.receiver.rabbitmq.internal.util.RabbitMQAdapterListener;
import org.wso2.carbon.extension.analytics.receiver.rabbitmq.internal.util.RabbitMQBrokerConnectionConfiguration;

import java.util.Map;
import java.util.UUID;

/**
 * Input RabbitMQEventAdapter will be used to receive events with AMQP protocol using specified broker and queue.
 */
public class RabbitMQEventAdapter implements InputEventAdapter {

    private final InputEventAdapterConfiguration eventAdapterConfiguration;
    private final String id = UUID.randomUUID().toString();
    private RabbitMQAdapterListener rabbitmqAdapterListener;

    public RabbitMQEventAdapter(InputEventAdapterConfiguration eventAdapterConfiguration,
                                Map<String, String> globalProperties) {
        this.eventAdapterConfiguration = eventAdapterConfiguration;
    }

    /**
     * This method is called when initiating event receiver bundle.
     * Relevant code segments which are needed when loading OSGI bundle can be included in this method.
     *
     * @param eventAdapterListener Receiving input
     */
    @Override
    public void init(InputEventAdapterListener eventAdapterListener) {
        RabbitMQBrokerConnectionConfiguration rabbitmqBrokerConnectionConfiguration;
            rabbitmqBrokerConnectionConfiguration = new RabbitMQBrokerConnectionConfiguration(eventAdapterConfiguration);
            rabbitmqAdapterListener = new RabbitMQAdapterListener(rabbitmqBrokerConnectionConfiguration, eventAdapterConfiguration,
                    eventAdapterListener);
    }

    /**
     * This method checks whether the receiving server is available.
     *
     * @throws org.wso2.carbon.event.input.adapter.core.exception.TestConnectionNotSupportedException
     */
    @Override
    public void testConnect() throws TestConnectionNotSupportedException {
        throw new TestConnectionNotSupportedException("not-supported");
    }

    /**
     * This method will be called after calling the init() method.
     * Intention is to connect to a receiving end
     * and if it is not available "ConnectionUnavailableException" will be thrown.
     */
    @Override
    public void connect() {
        rabbitmqAdapterListener.createConnection();
    }

    /**
     * This method can be called when it is needed to disconnect from the connected receiving server.
     */
    @Override
    public void disconnect() {
        if (rabbitmqAdapterListener != null) {
            rabbitmqAdapterListener.stopListener(eventAdapterConfiguration.getName());
        }
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that has to be done when removing the receiver can be done over here.
     */
    @Override
    public void destroy() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RabbitMQEventAdapter)) return false;

        RabbitMQEventAdapter that = (RabbitMQEventAdapter) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Returns a boolean output stating whether an event is duplicated in a cluster or not.
     * This can be used in clustered deployment.
     *
     * @return Boolean value
     */
    @Override
    public boolean isEventDuplicatedInCluster() {
        return false;
    }

    /**
     * Checks whether events get accumulated at the adapter and clients connect to it to collect events.
     *
     * @return Boolean value
     */
    @Override
    public boolean isPolling() {
        return true;
    }

}