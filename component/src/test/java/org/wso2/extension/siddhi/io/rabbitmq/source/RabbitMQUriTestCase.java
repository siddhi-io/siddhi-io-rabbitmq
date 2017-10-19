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

package org.wso2.extension.siddhi.io.rabbitmq.source;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.io.rabbitmq.util.UnitTestAppender;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

public class RabbitMQUriTestCase {
    private static Logger log = Logger.getLogger(RabbitMQUriTestCase.class);

    @Test (expectedExceptions = SiddhiAppValidationException.class)
    public void rabbitmqWithoutUriTest() {
            log.info("---------------------------------------------------------------------------------------------");
            log.info("RabbitMQ Source test without URI");
            log.info("---------------------------------------------------------------------------------------------");
            SiddhiManager siddhiManager = new SiddhiManager();
            siddhiManager.createSiddhiAppRuntime(
                            "@App:name('TestExecutionPlan') " +
                                    "define stream FooStream1 (symbol string, price float, volume long); " +
                                    "@info(name = 'query1') " +
                                    "@source(type='rabbitmq', " +
                                    "exchange.name = 'testUri', " +
                                    "@map(type='xml'))" +
                                    "Define stream BarStream1 (symbol string, price float, volume long);" +
                                    "from FooStream1 select symbol, price, volume insert into BarStream1;");
    }

    @Test
    public void rabbitmqInvalidUriTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Source.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest?guest@172.17.0.2?5672', " +
                                "exchange.name = 'invaliduriTest', routing.key= 'invalid', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        AssertJUnit.assertTrue(appender.messages.contains("Failed to connect with the Rabbitmq server"));
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqInvalidUriCredentialsTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI credentials");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Source.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://admin:admin@172.17.0.2:5672', " +
                                "exchange.name = 'invaliduriTest', routing.key= 'invalid', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        AssertJUnit.assertTrue(appender.messages.contains("Failed to connect with the Rabbitmq server"));
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqInvalidUriHostnameTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI hostname");
        log.info("---------------------------------------------------------------------------------------------");
        log = Logger.getLogger(Source.class);
        UnitTestAppender appender = new UnitTestAppender();
        log.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@host:5672', " +
                                "exchange.name = 'invaliduriTest', routing.key= 'invalid', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        AssertJUnit.assertTrue(appender.messages.contains("Failed to connect with the Rabbitmq server"));
        siddhiAppRuntime.shutdown();
    }

    @Test (expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqInvalidUriHostnameTest1() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://172.17.0.2^5672', " +
                                "exchange.name = 'invaliduriTest', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
