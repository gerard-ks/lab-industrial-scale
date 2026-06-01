package com.poc.modules.order.contract;

import java.util.UUID;

public interface OrderFacade {
    // Le monde extérieur ne voit que cette méthode métier pure.
    void placeOrder(UUID orderId, String productId, double amount);
}
