CREATE TABLE external_flags (
    user_id INTEGER PRIMARY KEY,
    vip     INTEGER NOT NULL DEFAULT 0
);

INSERT INTO external_flags (user_id, vip) VALUES
    (1, 1),
    (2, 0);
