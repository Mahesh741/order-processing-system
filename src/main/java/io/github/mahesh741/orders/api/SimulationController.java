package io.github.mahesh741.orders.api;

import io.github.mahesh741.orders.payment.SimulatedPaymentGateway;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Lets you switch a payment outage on and off to watch retries and dead letters happen. */
@RestController
@RequestMapping("/api/simulation/payment-outage")
public class SimulationController {

    public record OutageRequest(boolean enabled) { }

    private final SimulatedPaymentGateway gateway;

    public SimulationController(SimulatedPaymentGateway gateway) {
        this.gateway = gateway;
    }

    @GetMapping
    public Map<String, Boolean> get() {
        return Map.of("enabled", gateway.isOutage());
    }

    @PutMapping
    public Map<String, Boolean> set(@RequestBody OutageRequest request) {
        gateway.setOutage(request.enabled());
        return Map.of("enabled", gateway.isOutage());
    }
}
