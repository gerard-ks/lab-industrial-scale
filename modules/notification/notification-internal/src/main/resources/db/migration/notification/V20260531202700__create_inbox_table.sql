CREATE TABLE IF NOT EXISTS notification_schema.inbox (
    message_id UUID NOT NULL,
    handler_name VARCHAR(255) NOT NULL,

    -- NOUVEAU : On DOIT stocker la donnée pour que Java la lise plus tard !
    correlation_id UUID NOT NULL, -- Pour retrouver la Saga associée
    payload JSONB NOT NULL,       -- Le contenu de la réponse (ex: raison de l'échec)

    -- L'État du Worker Java (Est-ce que l'Orchestrateur l'a lu ?)
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    CONSTRAINT chk_inbox_status CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),

    -- Observabilité et Purge
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,

    -- DLQ Locale (Pour l'analyse des échecs)
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,

    PRIMARY KEY (message_id, handler_name) -- Le bouclier anti-doublon est préservé !
);

-- Index critique pour le Worker Java qui va lire l'Inbox !
CREATE INDEX idx_notification_inbox_pending ON notification_schema.inbox(received_at) WHERE status = 'PENDING';