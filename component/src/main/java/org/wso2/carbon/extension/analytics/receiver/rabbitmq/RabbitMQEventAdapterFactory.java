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

package org.wso2.carbon.extension.analytics.receiver.rabbitmq;

import org.wso2.carbon.event.input.adapter.core.*;
import org.wso2.carbon.extension.analytics.receiver.rabbitmq.internal.util.RabbitMQEventAdapterConstants;

import java.util.*;

/**
 * This class represents the properties we need to get from the UI configuration for define Receiver.
 */
public class RabbitMQEventAdapterFactory extends InputEventAdapterFactory {
    private ResourceBundle resourceBundle = ResourceBundle.getBundle("Resources", Locale.getDefault());

    /**
     * This method returns the receiver type as a String.
     *
     * @return Type of the Adapter
     */
    @Override
    public String getType() {
        return RabbitMQEventAdapterConstants.ADAPTER_TYPE_RABBITMQ;
    }

    /**
     * Specify supported message formats for the created receiver type.
     *
     * @return Supported Message Formats
     */
    @Override
    public List<String> getSupportedMessageFormats() {
        List<String> supportInputMessageTypes = new ArrayList<>();
        supportInputMessageTypes.add(MessageType.XML);
        supportInputMessageTypes.add(MessageType.JSON);
        supportInputMessageTypes.add(MessageType.TEXT);
        return supportInputMessageTypes;
    }

    /**
     * Here the properties have to be defined for the receiver.
     * When defining properties you can implement to configure property values from the management console.
     *
     * @return List of Properties
     */
    @Override
    public List<Property> getPropertyList() {
        ArrayList<Property> propertyList = new ArrayList<>();
        // Server Host Name
        Property hostNameProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME);
        hostNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME));
        hostNameProperty.setRequired(true);
        hostNameProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_HOST_NAME_HINT));
        propertyList.add(hostNameProperty);

        // Server Server Port
        Property serverPortProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PORT);
        serverPortProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PORT));
        serverPortProperty.setRequired(true);
        serverPortProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PORT_HINT));
        propertyList.add(serverPortProperty);

        // Server User name
        Property userNameProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_USERNAME);
        userNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_USERNAME));
        userNameProperty.setRequired(true);
        //userNameProperty.setDefaultValue("guest");
        userNameProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_USERNAME_HINT));
        propertyList.add(userNameProperty);

        // Server Password
        Property passwordProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PASSWORD);
        passwordProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PASSWORD));
        passwordProperty.setRequired(true);
        passwordProperty.setSecured(true);
        //  passwordProperty.setDefaultValue("guest");
        passwordProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_PASSWORD_HINT));
        propertyList.add(passwordProperty);

        // Server Queue Name
        Property queueNameProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_NAME);
        queueNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_NAME));
        queueNameProperty.setRequired(true);
        queueNameProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_NAME_HINT));
        propertyList.add(queueNameProperty);

        // Server Exchange Name
        Property exchangeNameProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_NAME);
        exchangeNameProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_NAME));
        exchangeNameProperty.setRequired(true);
        exchangeNameProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_NAME_HINT));
        propertyList.add(exchangeNameProperty);

        //Queue Durable
        Property queueDurableProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_DURABLE);
        queueDurableProperty.setRequired(false);
        queueDurableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_DURABLE));
        queueDurableProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_DURABLE_HINT));
        queueDurableProperty.setOptions(new String[]{"true", "false"});
        queueDurableProperty.setDefaultValue("true");
        propertyList.add(queueDurableProperty);

        //Queue Exclusive
        Property queueExclusiveProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE);
        queueExclusiveProperty.setRequired(false);
        queueExclusiveProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE));
        queueExclusiveProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_EXCLUSIVE_HINT));
        queueExclusiveProperty.setOptions(new String[]{"true", "false"});
        queueExclusiveProperty.setDefaultValue("false");
        propertyList.add(queueExclusiveProperty);

        //Queue Auto Delete
        Property queueAutoDeleteProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE);
        queueAutoDeleteProperty.setRequired(false);
        queueAutoDeleteProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE));
        queueAutoDeleteProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_DELETE_HINT));
        queueAutoDeleteProperty.setOptions(new String[]{"true", "false"});
        queueAutoDeleteProperty.setDefaultValue("false");
        propertyList.add(queueAutoDeleteProperty);

        //Queue Auto Ack
        Property queueAutoAckProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK);
        queueAutoAckProperty.setRequired(false);
        queueAutoAckProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK));
        queueAutoAckProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_AUTO_ACK_HINT));
        queueAutoAckProperty.setOptions(new String[]{"true", "false"});
        queueAutoAckProperty.setDefaultValue("false");
        propertyList.add(queueAutoAckProperty);

        // Queue Routing Key
        Property routingKeyProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY);
        routingKeyProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY));
        routingKeyProperty.setRequired(false);
        routingKeyProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_QUEUE_ROUTING_KEY_HINT));
        propertyList.add(routingKeyProperty);

        //Consumer Tag
        Property consumerDurableProperty = new Property(RabbitMQEventAdapterConstants.CONSUMER_TAG);
        consumerDurableProperty.setRequired(false);
        consumerDurableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.CONSUMER_TAG));
        consumerDurableProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.CONSUMER_TAG_HINT));
        propertyList.add(consumerDurableProperty);

        // Exchange Type
        Property exchangeTypeProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE);
        exchangeTypeProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE));
        exchangeTypeProperty.setRequired(false);
        exchangeTypeProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_TYPE_HINT));
        propertyList.add(exchangeTypeProperty);

        //Exchange Durable
        Property exchangeDurableProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE);
        exchangeDurableProperty.setRequired(false);
        exchangeDurableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE));
        exchangeDurableProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_DURABLE_HINT));
        exchangeDurableProperty.setOptions(new String[]{"true", "false"});
        exchangeDurableProperty.setDefaultValue("true");
        propertyList.add(exchangeDurableProperty);

        //Exchange Auto Delete
        Property exchangeAutoDeleteProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_AUTO_DELETE);
        exchangeAutoDeleteProperty.setRequired(false);
        exchangeAutoDeleteProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_AUTO_DELETE));
        exchangeAutoDeleteProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_EXCHANGE_AUTO_DELETE_HINT));
        exchangeAutoDeleteProperty.setOptions(new String[]{"true", "false"});
        exchangeAutoDeleteProperty.setDefaultValue("false");
        propertyList.add(exchangeAutoDeleteProperty);

        // Retry Count
        Property retryCountProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT);
        retryCountProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT));
        retryCountProperty.setRequired(false);
        retryCountProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_COUNT_HINT));
        propertyList.add(retryCountProperty);

        // Retry Interval
        Property retryIntervalProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL);
        retryIntervalProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL));
        retryIntervalProperty.setRequired(false);
        retryIntervalProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_RETRY_INTERVAL_HINT));
        propertyList.add(retryIntervalProperty);

        // Virtual Host
        Property virtualHostProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST);
        virtualHostProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST));
        virtualHostProperty.setRequired(false);
        virtualHostProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_SERVER_VIRTUAL_HOST_HINT));
        propertyList.add(virtualHostProperty);

        // Factory Heart beat
        Property heartBeatProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT);
        heartBeatProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT));
        heartBeatProperty.setRequired(false);
        heartBeatProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_FACTORY_HEARTBEAT_HINT));
        propertyList.add(heartBeatProperty);

        // SSL Enable
        Property sslEnableProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED);
        sslEnableProperty.setRequired(false);
        sslEnableProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED));
        sslEnableProperty.setHint(resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_ENABLED_HINT));
        sslEnableProperty.setOptions(new String[]{"true", "false"});
        sslEnableProperty.setDefaultValue("false");
        propertyList.add(sslEnableProperty);

        // SSL Keystore Location
        Property keystoreLocationProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION);
        keystoreLocationProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_LOCATION));
        keystoreLocationProperty.setRequired(false);
        propertyList.add(keystoreLocationProperty);

        // SSL Keystore Type
        Property keystoreTypeProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE);
        keystoreTypeProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_TYPE));
        keystoreTypeProperty.setRequired(false);
        propertyList.add(keystoreTypeProperty);

        // SSL Keystore Password
        Property keystorePasswordProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD);
        keystorePasswordProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_KEYSTORE_PASSWORD));
        keystorePasswordProperty.setRequired(false);
        propertyList.add(keystorePasswordProperty);

        // SSL Truststore Location
        Property truststoreLocationProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION);
        truststoreLocationProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_LOCATION));
        truststoreLocationProperty.setRequired(false);
        propertyList.add(truststoreLocationProperty);

        // SSL Truststore Type
        Property truststoreTypeProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE);
        truststoreTypeProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_TYPE));
        truststoreTypeProperty.setRequired(false);
        propertyList.add(truststoreTypeProperty);

        // SSL Truststore Password
        Property truststorePasswordProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD);
        truststorePasswordProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_TRUSTSTORE_PASSWORD));
        truststorePasswordProperty.setRequired(false);
        propertyList.add(truststorePasswordProperty);

        // SSL Version
        Property versionProperty = new Property(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_VERSION);
        versionProperty.setDisplayName(
                resourceBundle.getString(RabbitMQEventAdapterConstants.RABBITMQ_CONNECTION_SSL_VERSION));
        versionProperty.setRequired(false);
        propertyList.add(versionProperty);
        return propertyList;
    }

    /**
     * Specify any hints to be displayed in the management console.
     *
     * @return Usage Tips
     */
    @Override
    public String getUsageTips() {
        return null;
    }

    /**
     * This method creates the receiver by specifying event adapter configuration
     * and global properties which are common to every adapter type.
     *
     * @param inputEventAdapterConfiguration Configuration of Adapter
     * @param map                            Global properties from the map
     * @return InputEventAdapter
     */
    @Override
    public InputEventAdapter createEventAdapter(InputEventAdapterConfiguration inputEventAdapterConfiguration, Map<String, String> map) {
        return new RabbitMQEventAdapter(inputEventAdapterConfiguration, map);
    }
}