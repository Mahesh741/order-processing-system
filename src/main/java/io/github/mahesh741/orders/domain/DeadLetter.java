package io.github.mahesh741.orders.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** An order event that could not be processed, kept so an operator can inspect and replay it. */
@Entity
@Table(name = "dead_letters")
public class DeadLetter {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "source_topic", nullable = false)
    private String sourceTopic;

    @Column(nullable = false)
    private String reason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "replayed_at")
    private Instant replayedAt;

    protected DeadLetter() {
        // for JPA
    }

    public DeadLetter(UUID orderId, String sourceTopic, String reason) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.sourceTopic = sourceTopic;
        this.reason = reason;
        this.createdAt = Instant.now();
    }

    public void markReplayed() {
        this.replayedAt = Instant.now();
    }

    public boolean isReplayed() {
        return replayedAt != null;
    }

    public UUID getId() { return id; }
    public UUID getOrderId() { return orderId; }
    public String getSourceTopic() { return sourceTopic; }
    public String getReason() { return reason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getReplayedAt() { return replayedAt; }
}
