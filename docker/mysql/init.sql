CREATE TABLE users (
    user_id   INT PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    active    TINYINT NOT NULL DEFAULT 1
);

CREATE TABLE roles (
    role_id   INT PRIMARY KEY,
    role_name VARCHAR(255) NOT NULL,
    active    TINYINT NOT NULL DEFAULT 1
);

CREATE TABLE user_roles (
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    PRIMARY KEY (user_id, role_id),
    CONSTRAINT fk_user_roles_user FOREIGN KEY (user_id) REFERENCES users (user_id),
    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_id) REFERENCES roles (role_id)
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
