package com.poc.modules.inventory.contract.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.UUID;

@ActivityInterface
public interface InventoryActivities {
    @ActivityMethod
    void reserveStock(UUID orderId, String productId);
}
