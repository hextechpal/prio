CREATE TABLE jobs (
    id BIGINT primary key NOT NULL AUTO_INCREMENT,
    topic VARCHAR(20) NOT NULL,
    payload BLOB,
    priority INT NOT NULL,
    status TINYINT default 1,
    created_at BIGINT DEFAULT NULL,
    updated_at BIGINT DEFAULT NULL,
    FOREIGN KEY (topic) REFERENCES topics(name)
);

CREATE INDEX topic_prio_updated_idx ON jobs(topic, priority DESC, updated_at ASC);
