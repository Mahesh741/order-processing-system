package io.github.mahesh741.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mahesh741.orders.domain.DeadLetter;
import io.github.mahesh741.orders.domain.DeadLetterRepository;
import io.github.mahesh741.orders.domain.Order;
import io.github.mahesh741.orders.domain.OrderRepository;
import io.github.mahesh741.orders.domain.OrderStatus;
import io.github.mahesh741.orders.events.OrderCreated;
import io.github.mahesh741.orders.payment.SimulatedPaymentGateway;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

/**
 * End-to-end tests: HTTP request, database, a real (embedded) Kafka broker with the retry
 * and dead-letter topics, and back to the database.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // In-memory H2 by default; set TEST_DB_URL to run the same tests against PostgreSQL.
        "spring.datasource.url=${TEST_DB_URL:jdbc:h2:mem:orders;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH}",
        "spring.datasource.username=${TEST_DB_USER:sa}",
        "spring.datasource.password=${TEST_DB_PASSWORD:}",
        "app.topics.partitions=1",
        "app.retry.delay-ms=100"
})
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1)
@DirtiesContext
class OrderFlowIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired OrderRepository orders;
    @Autowired DeadLetterRepository deadLetters;
    @Autowired SimulatedPaymentGateway gateway;
    @Autowired KafkaTemplate<String, Object> kafka;
    @Value("${app.topics.orders}") String topic;

    @BeforeEach
    void healthyGateway() {
        gateway.setOutage(false);
        gateway.failNext(0);
    }

    @Test
    void anOrderIsAcceptedImmediatelyAndCompletedAsynchronously() throws Exception {
        UUID id = placeOrder("SKU-1", 2, 4_999);

        Order order = awaitStatus(id, OrderStatus.COMPLETED);

        assertThat(order.getAttempts()).isEqualTo(1);
        assertThat(order.getPaymentReference()).startsWith("pay_");
        assertThat(gateway.hasCharged(id)).isTrue();
        mvc.perform(get("/api/orders/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"));
    }

    @Test
    void transientPaymentFailuresAreRetriedUntilTheySucceed() throws Exception {
        gateway.failNext(2);

        UUID id = placeOrder("SKU-2", 1, 1_000);
        Order order = awaitStatus(id, OrderStatus.COMPLETED);

        assertThat(order.getAttempts()).as("two failures, then success").isEqualTo(3);
        assertThat(order.getFailureReason()).isNull();
        assertThat(deadLetters.findByOrderId(id)).isEmpty();
    }

    @Test
    void anOrderIsDeadLetteredWhenRetriesRunOutAndCanBeReplayedLater() throws Exception {
        gateway.setOutage(true);

        UUID id = placeOrder("SKU-3", 1, 2_500);
        Order failed = awaitStatus(id, OrderStatus.FAILED);

        assertThat(failed.getAttempts()).as("one delivery plus three retries").isEqualTo(4);
        assertThat(gateway.hasCharged(id)).isFalse();
        List<DeadLetter> letters = deadLetters.findByOrderId(id);
        assertThat(letters).hasSize(1);
        assertThat(letters.get(0).getSourceTopic()).isEqualTo(topic + "-dlt");
        assertThat(letters.get(0).getReason()).contains("unavailable");

        // The outage ends and an operator replays the dead letter.
        gateway.setOutage(false);
        UUID deadLetterId = letters.get(0).getId();
        mvc.perform(post("/api/dead-letters/{id}/replay", deadLetterId))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.replayedAt").isNotEmpty());

        Order recovered = awaitStatus(id, OrderStatus.COMPLETED);
        assertThat(recovered.getAttempts()).isEqualTo(5);
        assertThat(gateway.hasCharged(id)).isTrue();

        mvc.perform(post("/api/dead-letters/{id}/replay", deadLetterId))
                .andExpect(status().isConflict());
    }

    @Test
    void anInvalidOrderSkipsTheRetriesAndIsDeadLetteredAtOnce() throws Exception {
        UUID id = placeOrder("UNKNOWN-42", 1, 1_000);

        Order order = awaitStatus(id, OrderStatus.FAILED);

        assertThat(order.getAttempts()).as("retrying cannot fix an unknown SKU").isEqualTo(1);
        assertThat(deadLetters.findByOrderId(id)).singleElement()
                .satisfies(letter -> assertThat(letter.getReason()).contains("does not exist"));
        assertThat(gateway.hasCharged(id)).isFalse();
    }

    @Test
    void aDuplicateEventDoesNotChargeTheCustomerTwice() throws Exception {
        UUID id = placeOrder("SKU-5", 1, 9_900);
        Order completed = awaitStatus(id, OrderStatus.COMPLETED);
        int chargesBefore = gateway.chargeCount();
        int callsBefore = gateway.callCount();

        // Kafka redelivers the same event, as it may after a consumer rebalance.
        kafka.send(topic, id.toString(), new OrderCreated(id, completed.getCustomerId(), completed.getSku(),
                completed.getQuantity(), completed.getAmountCents())).get();

        // The topic has one partition, so once a later order is done the duplicate was consumed too.
        UUID later = placeOrder("SKU-6", 1, 100);
        awaitStatus(later, OrderStatus.COMPLETED);

        assertThat(orders.findById(id).orElseThrow().getAttempts()).isEqualTo(1);
        assertThat(gateway.chargeCount()).isEqualTo(chargesBefore + 1);
        assertThat(gateway.callCount()).as("only the later order reached the gateway").isEqualTo(callsBefore + 1);
    }

    @Test
    void aDuplicateEventForAFailedOrderDoesNotCreateASecondDeadLetter() throws Exception {
        UUID id = placeOrder("UNKNOWN-7", 1, 1_000);
        Order failed = awaitStatus(id, OrderStatus.FAILED);

        kafka.send(topic, id.toString(), new OrderCreated(id, failed.getCustomerId(), failed.getSku(),
                failed.getQuantity(), failed.getAmountCents())).get();
        UUID later = placeOrder("SKU-8", 1, 100);
        awaitStatus(later, OrderStatus.COMPLETED);

        assertThat(orders.findById(id).orElseThrow().getAttempts()).isEqualTo(1);
        assertThat(deadLetters.findByOrderId(id)).hasSize(1);
    }

    @Test
    void anInvalidRequestIsRejectedBeforeAnythingIsStoredOrPublished() throws Exception {
        long before = orders.count();

        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"customerId\":\"\",\"sku\":\"SKU-1\",\"quantity\":0,\"amountCents\":-5}"))
                .andExpect(status().isBadRequest());

        assertThat(orders.count()).isEqualTo(before);
    }

    @Test
    void anUnknownOrderIsA404() throws Exception {
        mvc.perform(get("/api/orders/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").exists());
    }

    private UUID placeOrder(String sku, int quantity, long amountCents) throws Exception {
        String body = json.writeValueAsString(new Request("customer-1", sku, quantity, amountCents));
        String response = mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn().getResponse().getContentAsString();
        JsonNode node = json.readTree(response);
        return UUID.fromString(node.get("id").asText());
    }

    private Order awaitStatus(UUID id, OrderStatus expected) {
        await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(orders.findById(id).orElseThrow().getStatus()).isEqualTo(expected));
        return orders.findById(id).orElseThrow();
    }

    private record Request(String customerId, String sku, int quantity, long amountCents) { }
}
