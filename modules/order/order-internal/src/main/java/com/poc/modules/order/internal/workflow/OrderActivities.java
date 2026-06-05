package com.poc.modules.order.internal.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.UUID;

// L'interface (Locale au module)
@ActivityInterface
public interface OrderActivities {
    @ActivityMethod
    void createPendingOrder(UUID orderId, String productId, double amount);

    @ActivityMethod
    void validateOrder(UUID orderId, UUID correlationId);

    @ActivityMethod
    void cancelOrder(UUID orderId, UUID correlationId, String reason);
}
