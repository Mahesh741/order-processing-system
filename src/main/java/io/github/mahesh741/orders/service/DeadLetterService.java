package io.github.mahesh741.orders.service;

import io.github.mahesh741.orders.domain.DeadLetter;
import io.github.mahesh741.orders.domain.DeadLetterRepository;
import io.github.mahesh741.orders.domain.Order;
import io.github.mahesh741.orders.domain.OrderRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DeadLetterService {

    private final DeadLetterRepository deadLetters;
    private final OrderRepository orders;
    private final ApplicationEventPublisher events;

    public DeadLetterService(DeadLetterRepository deadLetters, OrderRepository orders,
                             ApplicationEventPublisher events) {
        this.deadLetters = deadLetters;
        this.orders = orders;
        this.events = events;
    }

    @Transactional(readOnly = true)
    public List<DeadLetter> recent() {
        return deadLetters.findTop50ByOrderByCreatedAtDesc();
    }

    /**
     * Put a dead-lettered order back on the main topic, typically after the cause (an outage,
     * a missing catalogue entry) has been fixed. It gets a fresh set of retries.
     */
    @Transactional
    public DeadLetter replay(UUID deadLetterId) {
        DeadLetter deadLetter = deadLetters.findById(deadLetterId)
                .orElseThrow(() -> new NotFoundException("dead letter " + deadLetterId + " not found"));
        if (deadLetter.isReplayed()) {
            throw new IllegalStateException("dead letter " + deadLetterId + " was already replayed");
        }
        Order order = orders.findById(deadLetter.getOrderId())
                .orElseThrow(() -> new NotFoundException("order " + deadLetter.getOrderId() + " not found"));

        order.resetForReplay();
        deadLetter.markReplayed();
        events.publishEvent(OrderService.toEvent(order));
        return deadLetter;
    }
}
