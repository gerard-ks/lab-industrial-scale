package com.poc.modules.notification.internal.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import java.util.UUID;

@ActivityInterface
public interface EmailActivities {
    @ActivityMethod
    void sendEmail(UUID orderId, String message);
}
