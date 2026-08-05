CREATE TABLE users (
    user_id   INTEGER PRIMARY KEY,
    user_name TEXT NOT NULL,
    active    INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE roles (
    role_id   INTEGER PRIMARY KEY,
    role_name TEXT NOT NULL,
    active    INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE user_roles (
    user_id INTEGER NOT NULL REFERENCES users (user_id),
    role_id INTEGER NOT NULL REFERENCES roles (role_id),
    PRIMARY KEY (user_id, role_id)
);

INSERT INTO users (user_id, user_name, active) VALUES
    (1, 'admin', 1),
    (2, 'guest', 1);

INSERT INTO roles (role_id, role_name, active) VALUES
    (1, 'Administrator', 1),
    (2, 'User', 1);

INSERT INTO user_roles (user_id, role_id) VALUES
    (1, 1),
    (1, 2),
    (2, 2);
