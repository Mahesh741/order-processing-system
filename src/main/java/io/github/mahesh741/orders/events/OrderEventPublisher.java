package io.github.mahesh741.orders.events;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Sends the event only after the database transaction has committed, so the processor can
 * never receive an order id that is not yet visible in the database.
 *
 * <p>If the broker is unreachable at this point the order stays PENDING with no event. A
 * transactional outbox closes that gap; it is left out here to keep the example small.
 */
@Component
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafka;
    private final String topic;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafka, @Value("${app.topics.orders}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publish(OrderCreated event) {
        kafka.send(topic, event.orderId().toString(), event);
    }
}
