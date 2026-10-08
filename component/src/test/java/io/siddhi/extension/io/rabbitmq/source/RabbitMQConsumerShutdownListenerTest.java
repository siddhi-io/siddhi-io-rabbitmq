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
import io.siddhi.extension.io.rabbitmq.util.UnitTestAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class RabbitMQConsumerShutdownListenerTest {

    @Test
    public void applicationInitiatedShutdownDoesNotLogAnError() {
        UnitTestAppender appender = new UnitTestAppender("shutdownListenerAppender", null);
        Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.ALL);
        logger.addAppender(appender);
        appender.start();

        try {
            RabbitMQConsumer.RabbitMQShutdownListener listener = new RabbitMQConsumer()
                    .new RabbitMQShutdownListener();
            listener.shutdownCompleted(new ShutdownSignalException(false, true, null, null));

            AssertJUnit.assertNull(appender.getMessages());
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }
    }
}
