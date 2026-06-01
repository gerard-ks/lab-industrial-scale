package com.poc.modules.notification.internal.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.UUID;

@WorkflowInterface
public interface EmailWorkflow {
    @WorkflowMethod
    void sendEmailAsync(UUID orderId, String message);
}
