package io.github.mahesh741.orders.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(nullable = false)
    private OrderStatus status;

    /** How many times the processor has picked this order up, across retries and replays. */
    @Column(nullable = false)
    private int attempts;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "payment_reference")
    private String paymentReference;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {
        // for JPA
    }

    public Order(String customerId, String sku, int quantity, long amountCents) {
        this.id = UUID.randomUUID();
        this.customerId = customerId;
        this.sku = sku;
        this.quantity = quantity;
        this.amountCents = amountCents;
        this.status = OrderStatus.PENDING;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public void recordAttempt() {
        attempts++;
        touch();
    }

    public void recordFailure(String reason) {
        String text = reason == null ? "unknown error" : reason;
        failureReason = text.length() > 255 ? text.substring(0, 255) : text;
        touch();
    }

    public void complete(String paymentReference) {
        this.status = OrderStatus.COMPLETED;
        this.paymentReference = paymentReference;
        this.failureReason = null;
        touch();
    }

    public void fail() {
        this.status = OrderStatus.FAILED;
        touch();
    }

    public void resetForReplay() {
        this.status = OrderStatus.PENDING;
        this.failureReason = null;
        touch();
    }

    private void touch() {
        updatedAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
    public long getAmountCents() { return amountCents; }
    public OrderStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public String getFailureReason() { return failureReason; }
    public String getPaymentReference() { return paymentReference; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
