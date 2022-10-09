create table jobs (
    id BIGINT primary key NOT NULL AUTO_INCREMENT,
    payload BLOB,
    priority INT NOT NULL,
    created_at BIGINT DEFAULT NULL,
    updated_at BIGINT DEFAULT NULL
);