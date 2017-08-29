package org.wso2.extension.siddhi.io.rabbitmq.source;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;

public class RabbitMQUriTestCase {
    private static final Logger log = Logger.getLogger(RabbitMQUriTestCase.class);

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
                                    "exchange.name = 'testUri', " +
                                    "@map(type='xml'))" +
                                    "Define stream BarStream1 (symbol string, price float, volume long);" +
                                    "from FooStream1 select symbol, price, volume insert into BarStream1;");
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
        } catch (Exception e) {
            log.warn("Error while connecting with the RabbitMQ Server ");
        }

    }

    @Test
    public void rabbitmqInvalidUriTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI");
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
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqInvalidUriCredentialsTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI credentials");
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
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void rabbitmqInvalidUriHostnameTest() throws InterruptedException {
        log.info("---------------------------------------------------------------------------------------------");
        log.info("RabbitMQ Source test with invalid URI hostname");
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
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
