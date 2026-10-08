/*
 *  Copyright (c) 2026 WSO2 LLC. (http://www.wso2.com)
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.io.rabbitmq.source;

import com.rabbitmq.client.ShutdownSignalException;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.extension.io.rabbitmq.util.UnitTestAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RabbitMQConsumerShutdownListenerTest {

    @Test
    public void applicationInitiatedShutdownDoesNotLogAnError() throws Exception {
        UnitTestAppender appender = new UnitTestAppender("shutdownListenerAppender", null);
        Logger logger = (Logger) LogManager.getRootLogger();
        Level originalLevel = logger.getLevel();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        try {
            CountingCallback callback = new CountingCallback(null);
            RabbitMQConsumer consumer = consumerWith(callback);
            consumer.new RabbitMQShutdownListener()
                    .shutdownCompleted(new ShutdownSignalException(false, true, null, null));

            AssertJUnit.assertNull(appender.getMessages());
            AssertJUnit.assertEquals(0, callback.errors.get());
        } finally {
            logger.removeAppender(appender);
            appender.stop();
            logger.setLevel(originalLevel);
        }
    }

    @Test
    public void brokerInitiatedShutdownIsReported() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CountingCallback callback = new CountingCallback(latch);
        RabbitMQConsumer consumer = consumerWith(callback);
        consumer.new RabbitMQShutdownListener()
                .shutdownCompleted(new ShutdownSignalException(false, false, null, null));

        AssertJUnit.assertTrue(latch.await(5, TimeUnit.SECONDS));
        AssertJUnit.assertEquals(1, callback.errors.get());
    }

    private static RabbitMQConsumer consumerWith(Source.ConnectionCallback callback) throws Exception {
        RabbitMQConsumer consumer = new RabbitMQConsumer();
        Field field = RabbitMQConsumer.class.getDeclaredField("connectionCallback");
        field.setAccessible(true);
        field.set(consumer, callback);
        return consumer;
    }

    private static class CountingCallback extends Source.ConnectionCallback {

        private final AtomicInteger errors = new AtomicInteger();
        private final CountDownLatch latch;

        CountingCallback(CountDownLatch latch) {
            new RabbitMQSource().super();
            this.latch = latch;
        }

        @Override
        public void onError(ConnectionUnavailableException e) {
            errors.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }
}
