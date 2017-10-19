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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RabbitMQQueueTestCase {
    private static final Logger log = Logger.getLogger(RabbitMQQueueTestCase.class);
    private AtomicInteger eventCount1 = new AtomicInteger(0);
    private AtomicInteger eventCount2 = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;
    private List<String> receivedEventNameList;
    private List<String> receivedEventNameList1;

    @BeforeMethod
    public void init() {
        eventCount1.set(0);
        eventCount2.set(0);
    }

    @Test
    public void rabbitmqQueueConsumerTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test when queue name is provided");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'directQueue1', queue.name = 'rabbitmqQueue', " +
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
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'directQueue1', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
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

    @Test
    public void rabbitmqQueueExclusiveConsumerTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with queue exclusive is true");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'directQueue2', queue.name = 'exclusive', " +
                                "queue.exclusive.enabled = 'true', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    receivedEventNameList.add(event.getData(0).toString());
                    eventCount1.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'directQueue2', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
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

    @Test
    public void rabbitmqQueueAutoDeleteConsumerTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with queue autodelete is true");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'directQueue3', queue.name = 'autodelete', " +
                                "queue.autodelete.enabled = 'true', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    receivedEventNameList.add(event.getData(0).toString());
                    eventCount1.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'directQueue3', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
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

    @Test
    public void rabbitmqDurableQueueConsumerTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with queue durable is true");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'directQueue4', queue.name = 'durableQueue', " +
                                "queue.durable.enabled = 'true', " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount1.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'directQueue4', " +
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
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount1, timeout);
        AssertJUnit.assertEquals(3, eventCount1.get());
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqMultipleSourceWithDifferentQueueTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("Multiple RabbitMQ Source test case with different queue name");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream BarStream (symbol string, price float, volume long); " +
                                "define stream BarStream2 (symbol string, price float, volume long); " +

                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'multipleQueue', queue.name = 'testCase1', " +
                                "@map(type='xml'))" +
                                "Define stream FooStream (symbol string, price float, volume long); " +

                                "@info(name = 'query2') " +
                                "@source(type='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'multipleQueue', queue.name = 'testCase2', " +
                                "@map(type='xml'))" +
                                "Define stream FooStream2 (symbol string, price float, volume long); " +

                                "from FooStream select symbol, price, volume insert into BarStream; " +
                                "from FooStream2 select symbol, price, volume insert into BarStream2; ");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount1.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount2.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'multipleQueue', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount1, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount2, timeout);
        AssertJUnit.assertEquals(3, eventCount1.get());
        AssertJUnit.assertEquals(3, eventCount2.get());
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqMultipleSourceWithSameQueueTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("Multiple RabbitMQ Source test case with same queue name");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(2);
        receivedEventNameList1 = new ArrayList<>(2);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream BarStream (symbol string, price float, volume long); " +
                                "define stream BarStream2 (symbol string, price float, volume long); " +

                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'testCase', queue.name = 'testCase', " +
                                "@map(type='xml'))" +
                                "Define stream FooStream (symbol string, price float, volume long); " +

                                "@info(name = 'query2') " +
                                "@source(type='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'testCase', queue.name = 'testCase', " +
                                "@map(type='xml'))" +
                                "Define stream FooStream2 (symbol string, price float, volume long); " +

                                "from FooStream select symbol, price, volume insert into BarStream; " +
                                "from FooStream2 select symbol, price, volume insert into BarStream2; ");
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount1.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });

        siddhiAppRuntime.addCallback("BarStream2", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount2.incrementAndGet();
                    receivedEventNameList1.add(event.getData(0).toString());
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'testCase', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        List<String> expected = new ArrayList<>(2);
        List<String> expected1 = new ArrayList<>(2);
        expected.add("WSO2");
        expected1.add("IBM");
        expected.add("WSO2");
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount1, timeout);
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount2, timeout);
        AssertJUnit.assertEquals(expected, receivedEventNameList);
        AssertJUnit.assertEquals(expected1, receivedEventNameList1);
        AssertJUnit.assertEquals(2, eventCount1.get());
        AssertJUnit.assertEquals(1, eventCount2.get());
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();
    }
}
