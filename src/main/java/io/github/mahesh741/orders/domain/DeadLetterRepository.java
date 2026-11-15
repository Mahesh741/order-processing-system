package io.github.mahesh741.orders.domain;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterRepository extends JpaRepository<DeadLetter, UUID> {

    List<DeadLetter> findTop50ByOrderByCreatedAtDesc();

    List<DeadLetter> findByOrderId(UUID orderId);
}
