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
import java.util.ArrayList;

public class RabbitMQUriTestCase {
    private static final Logger log = Logger.getLogger(RabbitMQUriTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void rabbitmqWithoutUriTest() throws InterruptedException {
        try {
            log.info("---------------------------------------------------------------------------------------------");
            log.info("RabbitMQ Source test without URI");
            log.info("---------------------------------------------------------------------------------------------");
            SiddhiManager siddhiManager = new SiddhiManager();
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                    .createSiddhiAppRuntime(
                            "@App:name('TestExecutionPlan') " +
                                    "define stream FooStream1 (symbol string, price float, volume long); " +
                                    "@info(name = 'query1') " +
                                    "@source(type='rabbitmq', " +
                                    "exchange.name = 'testUri', routing.key= 'test', " +
                                    "@map(type='xml'))" +
                                    "Define stream BarStream1 (symbol string, price float, volume long);" +
                                    "from FooStream1 select symbol, price, volume insert into BarStream1;");
            siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    for (Event event : events) {
                        log.info(event);
                        eventArrived = true;
                        count++;
                    }
                }
            });
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.warn("Error while connecting with the RabbitMQ Server ");
        }

    }

    @Test
    public void rabbitmqInvalidUriTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with invalid URI");
        log.info("---------------------------------------------------------------------------------------------");
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
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest?guest@172.17.0.2?5672', " +
                        "exchange.name = 'invaliduriTest', routing.key= 'invalid', " +
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
        Thread.sleep(10000);
        AssertJUnit.assertEquals(0, count);
        AssertJUnit.assertFalse(eventArrived);

        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqInvalidUriCredentialsTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with invalid URI credentials");
        log.info("---------------------------------------------------------------------------------------------");
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
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://admin:admin@172.17.0.2:5672', " +
                        "exchange.name = 'invaliduriTest', routing.key= 'invalid', " +
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
        Thread.sleep(10000);
        AssertJUnit.assertEquals(0, count);
        AssertJUnit.assertFalse(eventArrived);

        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqInvalidUriHostnameTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Sink and Source test with invalid URI hostname");
        log.info("---------------------------------------------------------------------------------------------");
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
        siddhiAppRuntime.addCallback("BarStream1", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event event : events) {
                    log.info(event);
                    eventArrived = true;
                    count++;
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('TestExecutionPlan') " +
                        "define stream FooStream1 (symbol string, price float, volume long); " +
                        "@info(name = 'query1') " +
                        "@sink(type ='rabbitmq', uri = 'amqp://guest:guest@host:5672', " +
                        "exchange.name = 'invaliduriTest', routing.key= 'invalid', " +
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
        Thread.sleep(10000);
        AssertJUnit.assertEquals(0, count);
        AssertJUnit.assertFalse(eventArrived);

        executionPlanRuntime.shutdown();
        siddhiAppRuntime.shutdown();

    }
}
