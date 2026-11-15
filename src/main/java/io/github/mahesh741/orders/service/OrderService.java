package io.github.mahesh741.orders.service;

import io.github.mahesh741.orders.domain.Order;
import io.github.mahesh741.orders.domain.OrderRepository;
import io.github.mahesh741.orders.events.OrderCreated;
import java.util.List;
import java.util.UUID;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final ApplicationEventPublisher events;

    public OrderService(OrderRepository orders, ApplicationEventPublisher events) {
        this.orders = orders;
        this.events = events;
    }

    /** Accept the order and return immediately; processing happens asynchronously. */
    @Transactional
    public Order place(String customerId, String sku, int quantity, long amountCents) {
        Order order = orders.save(new Order(customerId, sku, quantity, amountCents));
        events.publishEvent(toEvent(order));
        return order;
    }

    @Transactional(readOnly = true)
    public Order get(UUID id) {
        return orders.findById(id).orElseThrow(() -> new NotFoundException("order " + id + " not found"));
    }

    @Transactional(readOnly = true)
    public List<Order> recent() {
        return orders.findTop50ByOrderByCreatedAtDesc();
    }

    static OrderCreated toEvent(Order order) {
        return new OrderCreated(order.getId(), order.getCustomerId(), order.getSku(),
                order.getQuantity(), order.getAmountCents());
    }
}
