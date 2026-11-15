package io.github.mahesh741.orders.api;

import io.github.mahesh741.orders.domain.Order;
import io.github.mahesh741.orders.domain.OrderStatus;
import io.github.mahesh741.orders.service.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    public record PlaceOrderRequest(
            @NotBlank String customerId,
            @NotBlank String sku,
            @Min(1) int quantity,
            @Positive long amountCents) { }

    public record OrderResponse(UUID id, String customerId, String sku, int quantity, long amountCents,
                                OrderStatus status, int attempts, String failureReason,
                                String paymentReference, Instant createdAt, Instant updatedAt) {

        static OrderResponse from(Order o) {
            return new OrderResponse(o.getId(), o.getCustomerId(), o.getSku(), o.getQuantity(), o.getAmountCents(),
                    o.getStatus(), o.getAttempts(), o.getFailureReason(), o.getPaymentReference(),
                    o.getCreatedAt(), o.getUpdatedAt());
        }
    }

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    /** 202 Accepted: the order is stored and queued, not yet processed. Poll the Location. */
    @PostMapping
    public ResponseEntity<OrderResponse> place(@Valid @RequestBody PlaceOrderRequest request) {
        Order order = orders.place(request.customerId(), request.sku(), request.quantity(), request.amountCents());
        return ResponseEntity.accepted()
                .location(URI.create("/api/orders/" + order.getId()))
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) {
        return OrderResponse.from(orders.get(id));
    }

    @GetMapping
    public List<OrderResponse> recent() {
        return orders.recent().stream().map(OrderResponse::from).toList();
    }
}
