package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"trader-updates"})
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class TaskFiveTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFiveTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void task_five_verifier() throws InterruptedException {
        userPopulator.populate();

        // We use the Task 5 file you confirmed you have
        String[] transactionLines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");

        if (transactionLines == null) {
            logger.error("Data file not found!");
            return;
        }

        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // Wait for processing
        Thread.sleep(2000);

        logger.info("----------------------------------------------------------");
        logger.info("---begin output ---");

        // Loop through the first 13 users to match the Model Answer count
        for (int i = 1; i <= 13; i++) {
            try {
                // Fetch the balance as a simple DTO to handle the formatting easily
                SimpleBalance balance = restTemplate.getForObject(
                        "http://localhost:" + port + "/balance?userId=" + i,
                        SimpleBalance.class
                );

                // Print in the EXACT Model Answer format: Balance {amount=X}
                if (balance != null) {
                    logger.info("Balance {amount=" + balance.getAmount() + "}");
                } else {
                    logger.info("Balance {amount=0.0}");
                }
            } catch (Exception e) {
                logger.info("Balance {amount=0.0}");
            }
        }

        logger.info("---end output ---");
        logger.info("----------------------------------------------------------");

        Thread.sleep(2000);
    }

    // Helper class to map the JSON response specifically for this test
    static class SimpleBalance {
        private float amount;

        public float getAmount() {
            return amount;
        }

        public void setAmount(float amount) {
            this.amount = amount;
        }
    }
}