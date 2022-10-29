CREATE TABLE jobs (
    id BIGINT primary key NOT NULL AUTO_INCREMENT,
    topic VARCHAR(20) NOT NULL,
    payload BLOB,
    priority INT NOT NULL,
    status TINYINT default 1,

    claimed_at BIGINT DEFAULT NULL,
    claimed_by VARCHAR(50) DEFAULT NULL,

    completed_at BIGINT DEFAULT NULL,

    created_at BIGINT DEFAULT NULL,
    updated_at BIGINT DEFAULT NULL,

    FOREIGN KEY (topic) REFERENCES topics(name)
);

CREATE INDEX top_job_idx ON jobs(topic, status, priority DESC, updated_at ASC);