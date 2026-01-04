package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        // FIXED: Added .orElse(null) to unwrap the Optional box
        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {

                // Deduct from sender
                sender.setBalance(sender.getBalance() - transaction.getAmount());

                // Add to recipient
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                // Save updated balances
                userRepository.save(sender);
                userRepository.save(recipient);

                // Save transaction
                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
                transactionRepository.save(record);

                logger.info("Transaction Processed: " + transaction.getAmount());
            } else {
                logger.info("Transaction Rejected: Insufficient Funds");
            }
        } else {
            logger.info("Transaction Rejected: Invalid User ID");
        }
    }
}