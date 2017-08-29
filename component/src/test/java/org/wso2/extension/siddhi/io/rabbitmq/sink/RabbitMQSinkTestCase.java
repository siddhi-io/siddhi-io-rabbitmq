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
import org.wso2.siddhi.core.stream.input.InputHandler;

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
    @Test
    public void rabbitmqWithoutUriSinkTest() throws InterruptedException {
        try {
            log.info("---------------------------------------------------------------------------------------------");
            log.info("RabbitMQ Sink test without URI");
            log.info("---------------------------------------------------------------------------------------------");
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream FooStream1 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type ='rabbitmq' " +
                            "exchange.name = 'testUri', routing.key= 'test', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream1 (symbol string, price float, volume long);" +
                            "from FooStream1 select symbol, price, volume insert into BarStream1;");
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();
        } catch (Exception e) {
            log.warn("Error while connecting with the RabbitMQ Server ");
        }

    }


    @Test
    public void rabbitmqWithoutExchangeNameSinkTest() throws InterruptedException {
        try {
            log.info("---------------------------------------------------------------------------------------------");
            log.info("RabbitMQ Sink test without exchange name");
            log.info("---------------------------------------------------------------------------------------------");
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                    "@App:name('TestExecutionPlan') " +
                            "define stream FooStream1 (symbol string, price float, volume long); " +
                            "@info(name = 'query1') " +
                            "@sink(type ='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                            "exchange.type='topic', exchange.durable.enabled= 'true', " +
                            "routing.key= 'topic.test', " +
                            "@map(type='xml'))" +
                            "Define stream BarStream1 (symbol string, price float, volume long);" +
                            "from FooStream1 select symbol, price, volume insert into BarStream1;");
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();
        } catch (Exception e) {
            log.warn("Exchange name is not mentioned");
        }

    }


    //TODO test for TLS pubish with default value

    /* @Test
     public void rabbitmqTLSPublishTest() throws Exception {

     SiddhiManager siddhiManager = new SiddhiManager();
     SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
     "@App:name('TestExecutionPlan') " +
     "define stream FooStream (symbol string, price float, volume long); " +
     "@info(name = 'query1') " +
     "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@localhost:5671', " +
     "exchange.name = 'tlstest', exchange.type = 'fanout', tls.enabled = 'true', " +
     "tls.truststore.Type = 'JKS', tls.truststore.path = '/home/sivaramya/rabbitmq/rabbitstore', " +
     "tls.version= 'TLSv1.2', tls.truststore.password = 'MySecretPassword', " +
     "@map(type='xml'))" +
     "Define stream BarStream (symbol string, price float, volume long);" +
     "from FooStream select symbol, price, volume insert into BarStream;");
     InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");

     RabbitMQSinkTestUtil.consumer("tlstest", "fanout", false,
             false, "", eventArrived, count);
     executionPlanRuntime.start();

     fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
     fooStream.send(new Object[]{"IBM", 75.6f, 100L});
     fooStream.send(new Object[]{"WSO2", 57.6f, 100L});
     Thread.sleep(10000);

     count = RabbitMQSinkTestUtil.getCount();
     eventArrived = RabbitMQSinkTestUtil.geteventArrived();
     AssertJunit.assertEquals(3, count);
     AssertJunit.assertTrue(eventArrived);

     executionPlanRuntime.shutdown();

     }*/


}
