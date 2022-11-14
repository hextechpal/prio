CREATE TABLE topics (
    name VARCHAR(20) primary key NOT NULL,
    description VARCHAR(255) DEFAULT NULL,
    created_at BIGINT DEFAULT NULL,
    updated_at BIGINT DEFAULT NULL
);