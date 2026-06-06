CREATE DATABASE temporal_db;
CREATE DATABASE temporal_visibility_db;

GRANT ALL PRIVILEGES ON DATABASE temporal_db TO lab_user;
GRANT ALL PRIVILEGES ON DATABASE temporal_visibility_db TO lab_user;

-- Accorde le rôle REPLICATION à l'utilisateur principal du Lab
ALTER USER lab_user WITH REPLICATION;