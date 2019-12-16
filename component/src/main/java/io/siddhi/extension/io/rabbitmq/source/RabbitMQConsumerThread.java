/*
 *  Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.rabbitmq.source;

import com.rabbitmq.client.Channel;
import io.siddhi.core.stream.input.source.SourceEventListener;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;

/**
 * RabbitMQ consumer thread implementation.
 */
public class RabbitMQConsumerThread implements Runnable {

    private static final Logger log = Logger.getLogger(RabbitMQConsumerThread.class);
    private final BlockingQueue<RabbitMQMessage> blockingQueue;
    private SourceEventListener sourceEventListener;
    private boolean autoAck;
    private Channel channel;
    private String listenerUri;
    private String exchangeName;

    RabbitMQConsumerThread(BlockingQueue<RabbitMQMessage> blockingQueue, Channel channel,
                           SourceEventListener sourceEventListener, boolean autoAck, String listenerUri,
                           String exchangeName) {
        this.blockingQueue = blockingQueue;
        this.channel = channel;
        this.sourceEventListener = sourceEventListener;
        this.autoAck = autoAck;
        this.listenerUri = listenerUri;
        this.exchangeName = exchangeName;
    }

    @Override
    public void run() {
        while (true) {
            try {
                RabbitMQMessage rabbitMQMessage = blockingQueue.take();
                sourceEventListener.onEvent(rabbitMQMessage.getBody(), null);
                if (!autoAck) {
                    channel.basicAck(rabbitMQMessage.getEnvelope().getDeliveryTag(), false);
                }
            } catch (InterruptedException e) {
                log.error("Error when reading message from blocking queue at RabbitMQ receiver " +
                        listenerUri + " and exchange name " + exchangeName, e);
            } catch (Exception e) {
                log.error("Error in processing the message received from RabbitMQ broker in "
                        + listenerUri + " and exchange name " + exchangeName, e);
            }
        }
    }
}
