package com.poc.modules.order.internal.workflow;

import com.poc.modules.order.internal.OrderService;
import io.temporal.spring.boot.ActivityImpl;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@ActivityImpl(taskQueues = "OrderTaskQueue")
public class OrderActivitiesImpl implements OrderActivities {

    private final OrderService orderService;

    public OrderActivitiesImpl(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void createPendingOrder(UUID orderId, String productId, double amount) {
        orderService.createPendingOrder(orderId, productId, amount);
    }

    @Override
    public void validateOrder(UUID orderId, UUID correlationId) {
        orderService.validateOrder(orderId, correlationId);
    }

    @Override
    public void cancelOrder(UUID orderId, UUID correlationId, String reason) {
        orderService.cancelOrder(orderId, correlationId, reason);
    }
}
