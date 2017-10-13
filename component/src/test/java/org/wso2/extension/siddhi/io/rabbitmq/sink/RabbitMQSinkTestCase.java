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

package org.wso2.extension.siddhi.io.rabbitmq.sink;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

public class RabbitMQSinkTestCase {
    private static final Logger log = Logger.getLogger(RabbitMQSinkTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void rabbitmqMandatoryFieldPublishTest() throws Exception {
        log.info("----------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with mandatory fields");
        log.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'mandatorySink'," +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        RabbitMQSinkTestUtil.consumer("mandatorySink", "direct", false,
                                      false, "", eventArrived, count);

        executionPlanRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(10000);
        count = RabbitMQSinkTestUtil.getCount();
        eventArrived = RabbitMQSinkTestUtil.geteventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }


    @Test
    public void rabbitmqDirectPublishTest() throws Exception {
        log.info("----------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with exchange type direct");
        log.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'direct', routing.key= 'direct', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        RabbitMQSinkTestUtil.consumer("direct", "direct", false,
                                      false, "direct", eventArrived, count);

        executionPlanRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});

        Thread.sleep(10000);
        count = RabbitMQSinkTestUtil.getCount();
        eventArrived = RabbitMQSinkTestUtil.geteventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void rabbitmqTopicPublishTest() throws Exception {
        log.info("----------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with exchange type topic");
        log.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'topicRouting', exchange.type='topic', " +
                        "routing.key= 'topic.test', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        RabbitMQSinkTestUtil.consumer("topicRouting", "topic", false,
                                      false, "topic.*", eventArrived, count);

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(10000);

        count = RabbitMQSinkTestUtil.getCount();
        eventArrived = RabbitMQSinkTestUtil.geteventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);

        executionPlanRuntime.shutdown();

    }

    @Test
    public void rabbitmqFanoutPublishTest() throws Exception {
        log.info("----------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with exchange type fanout");
        log.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'fanoutTest', exchange.type = 'fanout', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        RabbitMQSinkTestUtil.consumer("fanoutTest", "fanout", false,
                                      false, "", eventArrived, count);

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(10000);

        count = RabbitMQSinkTestUtil.getCount();
        eventArrived = RabbitMQSinkTestUtil.geteventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);

        executionPlanRuntime.shutdown();

    }

    @Test
    public void rabbitmqHeaderPublishTest() throws Exception {
        log.info("----------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with exchange type headers");
        log.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'headersTest', exchange.type = 'headers', headers= \"'A:1','B:2'\", " +
                        "exchange.autodelete.enabled = 'true', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        RabbitMQSinkTestUtil.consumer("headersTest", "headers", false,
                                      true, "", eventArrived, count);

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(10000);

        count = RabbitMQSinkTestUtil.getCount();
        eventArrived = RabbitMQSinkTestUtil.geteventArrived();
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test (expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqTimestampPublishTest() throws InterruptedException {
        log.info("----------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test with invalid timestamp format");
        log.info("----------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'TimestampTest',  timestamp = 'jan/13/2017', " +
                        "exchange.autodelete.enabled = 'true', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream (symbol string, price float, volume long);" +
                        "from FooStream select symbol, price, volume insert into BarStream;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 75.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
        executionPlanRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void rabbitmqWithoutUriSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test without URI");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', " +
                        "exchange.type='topic', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
    }


    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void rabbitmqWithoutExchangeNameSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test without exchange name");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.type='topic', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void rabbitmqInvalidExchangeTypeSinkTest() {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink test without exchange name");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name='testexchangetype', exchange.type='exchange', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
    }
}
