CREATE TABLE IF NOT EXISTS notification_schema.inbox (
    message_id UUID NOT NULL,
    handler_name VARCHAR(255) NOT NULL,

    -- NOUVEAU : On DOIT stocker la donnée pour que Java la lise plus tard !
    correlation_id UUID NOT NULL, -- Pour retrouver la Saga associée
    payload JSONB NOT NULL,       -- Le contenu de la réponse (ex: raison de l'échec)

    processed_at TIMESTAMPTZ,

    PRIMARY KEY (message_id, handler_name) -- Le bouclier anti-doublon est préservé !
);

-- Index de rétention simple pour la tâche de nettoyage automatique de nuit
-- (ex: DELETE FROM inbox WHERE processed_at < NOW() - INTERVAL '7 days')
CREATE INDEX IF NOT EXISTS idx_notification_inbox_retention ON notification_schema.inbox(processed_at);