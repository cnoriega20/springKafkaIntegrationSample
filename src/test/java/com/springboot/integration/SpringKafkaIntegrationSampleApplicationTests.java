package com.springboot.integration;

import com.springboot.integration.domain.Employee;
import com.springboot.integration.handlers.CountDownLatchHandler;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaIntegrationSampleApplicationTests {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SpringKafkaIntegrationSampleApplicationTests.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private CountDownLatchHandler countDownLatchHandler;

    private static String SPRING_INTEGRATION_KAFKA_TOPIC = "spring-integration-kafka.t";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule =
            new EmbeddedKafkaRule(1, true, SPRING_INTEGRATION_KAFKA_TOPIC);

    @Test
    public void testIntegration() throws Exception {
        MessageChannel producingChannel =
                applicationContext.getBean("producingChannel", MessageChannel.class);

        Map<String, Object> headers =
                Collections.singletonMap(KafkaHeaders.TOPIC, SPRING_INTEGRATION_KAFKA_TOPIC);
        List<Employee> employeeList = Arrays.asList(
                Employee.builder().employeeId(UUID.randomUUID()).firstName("Cesar").lastName("Noriega").salary(200000).build(),
                Employee.builder().employeeId(UUID.randomUUID()).firstName("Kumar").lastName("Patel").salary(100000).build(),
                Employee.builder().employeeId(UUID.randomUUID()).firstName("Luis").lastName("Rios").salary(80000).build(),
                Employee.builder().employeeId(UUID.randomUUID()).firstName("George").lastName("Smith").salary(90000).build(),
                Employee.builder().employeeId(UUID.randomUUID()).firstName("Lynn").lastName("Harper").salary(21000).build(),
                Employee.builder().employeeId(UUID.randomUUID()).firstName("Floyd").lastName("Renton").salary(4000).build()

        );
        LOGGER.info("sending 10 messages");
        for (int i = 0; i < employeeList.size(); i++) {
            GenericMessage<Employee> message =
                    new GenericMessage<>(employeeList.get(i), headers);
            producingChannel.send(message);
            LOGGER.info("sent message='{}'", message);
        }

        countDownLatchHandler.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertEquals(0,countDownLatchHandler.getLatch().getCount());
    }
}



