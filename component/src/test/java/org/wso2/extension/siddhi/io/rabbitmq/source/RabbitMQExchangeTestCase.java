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

public class RabbitMQExchangeTestCase {
    private static final Logger log = Logger.getLogger(RabbitMQExchangeTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private List<String> receivedEventNameList;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        eventCount.set(0);
    }

    @Test
    public void rabbitmqHeadersAutodeleteConsumerTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with exchange type headers and exchange autodelete is true");
        log.info("---------------------------------------------------------------------------------------------");
        receivedEventNameList = new ArrayList<>(3);
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'headersAutodelete', exchange.type = 'headers', " +
                                "exchange.autodelete.enabled = 'true', headers= \"'A:1','B:2'\", " +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount.incrementAndGet();
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
                        "exchange.name = 'headersAutodelete', exchange.type = 'headers', " +
                        "exchange.autodelete.enabled = 'true', " +
                        "headers= \"'A:1','B:2'\", " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        List<String> expected = new ArrayList<>(2);
        expected.add("WSO2");
        expected.add("IBM");
        expected.add("WSO2");
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        AssertJUnit.assertEquals(expected, receivedEventNameList);
        AssertJUnit.assertEquals(3, eventCount.get());
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqTopicDurableConsumerTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with exchange type topic and exchange durable is true");
        log.info("---------------------------------------------------------------------------------------------");
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(
                        "@App:name('TestExecutionPlan') " +
                                "define stream FooStream1 (symbol string, price float, volume long); " +
                                "@info(name = 'query1') " +
                                "@source(type='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                                "exchange.name = 'topictest', exchange.type='topic', " +
                                "exchange.durable.enabled= 'true'," +
                                "@map(type='xml'))" +
                                "Define stream BarStream1 (symbol string, price float, volume long);" +
                                "from FooStream1 select symbol, price, volume insert into BarStream1;");
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventCount.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri ='amqp://guest:guest@172.17.0.2:5672', " +
                        "exchange.name = 'topictest', exchange.type='topic', exchange.durable.enabled= 'true', " +
                        "@map(type='xml'))" +
                        "Define stream BarStream1 (symbol string, price float, volume long);" +
                        "from FooStream1 select symbol, price, volume insert into BarStream1;");
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream1");

        executionPlanRuntime.start();
        ArrayList<Event> arrayList = new ArrayList<Event>();
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 55.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"IBM", 75.6f, 100L}));
        arrayList.add(new Event(System.currentTimeMillis(), new Object[]{"WSO2", 57.6f, 100L}));
        fooStream.send(arrayList.toArray(new Event[3]));
        List<String> expected = new ArrayList<>(2);
        expected.add("WSO2");
        expected.add("IBM");
        expected.add("WSO2");
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        AssertJUnit.assertEquals(expected, receivedEventNameList);
        AssertJUnit.assertEquals(3, eventCount.get());
        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqWithoutExchangeNameTest() throws InterruptedException {
        try {
            log.info("---------------------------------------------------------------------------------------------");
            log.info("RabbitMQ Source test without exchange name");
            log.info("---------------------------------------------------------------------------------------------");
            receivedEventNameList = new ArrayList<>(3);
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                    .createSiddhiAppRuntime(
                            "@App:name('TestExecutionPlan') " +
                                    "define stream FooStream1 (symbol string, price float, volume long); " +
                                    "@info(name = 'query1') " +
                                    "@source(type='rabbitmq', uri = 'amqp://guest:guest@172.17.0.2:5672', " +
                                    "exchange.type='topic', " +
                                    "@map(type='xml'))" +
                                    "Define stream BarStream1 (symbol string, price float, volume long);" +
                                    "from FooStream1 select symbol, price, volume insert into BarStream1;");
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.warn("Exchange name is not mentioned");
        }

    }
}
