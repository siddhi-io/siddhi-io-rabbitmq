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

package io.siddhi.extension.io.rabbitmq.source;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

;

public class RabbitMQTlsTestCase {
    private static final Logger log = Logger.getLogger(RabbitMQTlsTestCase.class);
    private AtomicInteger eventCount1 = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;
    private List<String> receivedEventNameList;

    @BeforeMethod
    public void init() {
        eventCount1.set(0);
    }

    @Test
    public void rabbitmqTlsEnabledTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with tls.enabled = true");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(3);
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest1',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "', " +
                                "tls.version= 'TLSv1.2', tls.truststore.password = 'wso2carbon', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount1.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                        "exchange.name = 'tlstest1',  tls.enabled = 'true', " +
                        "tls.truststore.Type = 'JKS', " +
                        "tls.truststore.path = '" + truststorePath + "', " +
                        "tls.version= 'TLSv1.2', tls.truststore.password = 'wso2carbon', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        List<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        List<String> expected = new ArrayList<>(2);
        expected.add("WSO2");
        expected.add("IBM");
        expected.add("WSO2");
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount1, timeout);
        AssertJUnit.assertEquals(expected, receivedEventNameList);
        AssertJUnit.assertEquals(3, eventCount1.get());
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidPasswordTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with tls.enabled = true");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "',  "
                                + "tls.truststore.password = 'wrongPassword'," +
                                "tls.version= 'TLSv1.2', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidPasswordSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with tls.enabled = true");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@sink(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "', " +
                                "tls.truststore.password = 'wrongPassword'," +
                                "tls.version= 'TLSv1.2', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidPathSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with invalid truststore path");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-trustsore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@sink(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "', " +
                                "tls.version= 'TLSv1.2', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidPathTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid truststore path");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-trusttore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "',  " +
                                "tls.version= 'TLSv1.2', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidTruststoreTypeTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid truststore type");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JK', " +
                                "tls.truststore.path = '" + truststorePath + "',  " +
                                "tls.version= 'TLSv1.2', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidTruststoreTypeSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with with invalid truststore type");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@sink(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JK', " +
                                "tls.truststore.path = '" + truststorePath + "', " +
                                "tls.version= 'TLSv1.2', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidTruststoreSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with with invalid truststore version");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@sink(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "', " +
                                "tls.version= 'TLSv', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTlsEnabledwithInvalidTruststoreTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid truststore version");
        log.info("---------------------------------------------------------------------------------------------");
        File file = new File("src/test/resources/client-truststore.jks");
        String truststorePath = file.getAbsolutePath();
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
                                "exchange.name = 'tlstest2',  tls.enabled = 'true', " +
                                "tls.truststore.Type = 'JKS', " +
                                "tls.truststore.path = '" + truststorePath + "',  " +
                                "tls.version= 'TLSv', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

}
