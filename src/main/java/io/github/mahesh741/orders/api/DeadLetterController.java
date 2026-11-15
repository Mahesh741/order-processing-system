package io.github.mahesh741.orders.api;

import io.github.mahesh741.orders.domain.DeadLetter;
import io.github.mahesh741.orders.service.DeadLetterService;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dead-letters")
public class DeadLetterController {

    public record DeadLetterResponse(UUID id, UUID orderId, String sourceTopic, String reason,
                                     Instant createdAt, Instant replayedAt) {

        static DeadLetterResponse from(DeadLetter d) {
            return new DeadLetterResponse(d.getId(), d.getOrderId(), d.getSourceTopic(), d.getReason(),
                    d.getCreatedAt(), d.getReplayedAt());
        }
    }

    private final DeadLetterService deadLetters;

    public DeadLetterController(DeadLetterService deadLetters) {
        this.deadLetters = deadLetters;
    }

    @GetMapping
    public List<DeadLetterResponse> recent() {
        return deadLetters.recent().stream().map(DeadLetterResponse::from).toList();
    }

    @PostMapping("/{id}/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public DeadLetterResponse replay(@PathVariable UUID id) {
        return DeadLetterResponse.from(deadLetters.replay(id));
    }
}
