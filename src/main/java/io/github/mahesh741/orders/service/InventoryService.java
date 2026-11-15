package io.github.mahesh741.orders.service;

import org.springframework.stereotype.Service;

/** Stand-in for a stock lookup. SKUs prefixed {@code UNKNOWN-} do not exist in the catalogue. */
@Service
public class InventoryService {

    public void reserve(String sku, int quantity) {
        if (sku.startsWith("UNKNOWN-")) {
            throw new InvalidOrderException("SKU " + sku + " does not exist");
        }
    }
}
