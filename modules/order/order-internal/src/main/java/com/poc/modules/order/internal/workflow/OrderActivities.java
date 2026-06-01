package com.poc.modules.order.internal.workflow;

import java.util.UUID;

// L'interface (Locale au module)
public interface OrderActivities {
    void createPendingOrder(UUID orderId, String productId, double amount);

    void validateOrder(UUID orderId, UUID correlationId);

    void cancelOrder(UUID orderId, UUID correlationId, String reason);
}
