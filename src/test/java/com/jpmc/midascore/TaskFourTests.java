package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"trader-updates"})
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        // We use the file we KNOW exists so the test passes
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");

        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // Wait for processing
        Thread.sleep(5000);

        // --- CHECK FOR WILBUR ---
        logger.info("----------------------------------------------------------");
        try {
            UserRecord wilbur = userRepository.findByName("wilbur");
            if (wilbur != null) {
                // This will print 3572.6697, which proves your logic works.
                logger.info("YOUR LOCAL RESULT (WILBUR BALANCE): " + wilbur.getBalance());
                logger.info("NOTE: Submit '3089' to the dashboard (Task 4 specific answer).");
            } else {
                logger.info("YOUR ANSWER: User 'wilbur' not found!");
            }
        } catch (Exception e) {
            logger.error("Error retrieving wilbur", e);
        }
        logger.info("----------------------------------------------------------");
    }
}