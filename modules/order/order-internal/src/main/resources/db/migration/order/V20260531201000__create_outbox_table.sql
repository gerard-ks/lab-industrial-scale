CREATE TABLE IF NOT EXISTS order_schema.outbox (
    id UUID PRIMARY KEY,

    -- Routage NATS
    event_type VARCHAR(255) NOT NULL,

    -- Contexte DDD
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    version INT NOT NULL,

    -- Observabilité & Tracing
    correlation_id UUID NOT NULL,
    causation_id UUID,

    -- Payload métier
    payload JSONB NOT NULL,

    -- Pour le Polling de Redpanda Connect
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- BONNE PRATIQUE : Index simple sur created_at uniquement pour accélérer
-- la purge de nuit asynchrone (ex: DELETE WHERE created_at < NOW() - 3 days)
CREATE INDEX idx_order_outbox_retention ON order_schema.outbox(created_at);