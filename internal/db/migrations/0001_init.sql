-- Templates are versioned: editing a template inserts a new version and
-- deactivates the previous one, so historical notifications stay reproducible.
CREATE TABLE template (
    id          VARCHAR2(36)  PRIMARY KEY,
    code        VARCHAR2(100) NOT NULL,
    channel     VARCHAR2(10)  NOT NULL CHECK (channel IN ('EMAIL', 'SMS', 'PUSH')),
    subject     VARCHAR2(500),
    body        CLOB          NOT NULL,
    version     NUMBER(10)    NOT NULL,
    active      NUMBER(1)     DEFAULT 1 NOT NULL CHECK (active IN (0, 1)),
    created_at  TIMESTAMP     DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    CONSTRAINT uq_template_code_version UNIQUE (code, version)
);

CREATE INDEX ix_template_code_active ON template (code, active);

CREATE TABLE notification (
    id            VARCHAR2(36)   PRIMARY KEY,
    message_id    VARCHAR2(200)  NOT NULL,
    request_hash  VARCHAR2(64)   NOT NULL,
    channel       VARCHAR2(10)   NOT NULL CHECK (channel IN ('EMAIL', 'SMS', 'PUSH')),
    recipient     VARCHAR2(320)  NOT NULL,
    template_code VARCHAR2(100)  NOT NULL,
    template_ver  NUMBER(10)     NOT NULL,
    payload_json  VARCHAR2(4000),
    subject       VARCHAR2(500),
    body          CLOB           NOT NULL,
    status        VARCHAR2(20)   NOT NULL,
    priority      VARCHAR2(10)   DEFAULT 'NORMAL' NOT NULL,
    attempts      NUMBER(5)      DEFAULT 0 NOT NULL,
    last_error    VARCHAR2(500),
    provider_ref  VARCHAR2(200),
    created_at    TIMESTAMP      DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    updated_at    TIMESTAMP      DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    CONSTRAINT uq_notification_message_id UNIQUE (message_id)
);

CREATE INDEX ix_notification_status ON notification (channel, status, created_at);

CREATE INDEX ix_notification_created ON notification (created_at);

CREATE TABLE delivery_attempt (
    id              VARCHAR2(36)  PRIMARY KEY,
    notification_id VARCHAR2(36)  NOT NULL REFERENCES notification (id),
    attempt_no      NUMBER(5)     NOT NULL,
    provider        VARCHAR2(50)  NOT NULL,
    outcome         VARCHAR2(20)  NOT NULL,
    error_code      VARCHAR2(100),
    error_message   VARCHAR2(500),
    latency_ms      NUMBER(10)    NOT NULL,
    attempted_at    TIMESTAMP     DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL
);

CREATE INDEX ix_attempt_notification ON delivery_attempt (notification_id, attempt_no);

CREATE INDEX ix_attempt_time ON delivery_attempt (attempted_at);

CREATE TABLE dlq_message (
    id              VARCHAR2(36)  PRIMARY KEY,
    notification_id VARCHAR2(36)  NOT NULL REFERENCES notification (id),
    channel         VARCHAR2(10)  NOT NULL,
    reason          VARCHAR2(500) NOT NULL,
    payload         CLOB          NOT NULL,
    created_at      TIMESTAMP     DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    replayed_at     TIMESTAMP
);

CREATE INDEX ix_dlq_pending ON dlq_message (replayed_at, created_at);

CREATE TABLE rate_limit_config (
    channel         VARCHAR2(10) PRIMARY KEY,
    permits_per_sec NUMBER(10, 2) NOT NULL,
    burst           NUMBER(10)    NOT NULL
);

INSERT INTO rate_limit_config (channel, permits_per_sec, burst) VALUES ('EMAIL', 50, 100);

INSERT INTO rate_limit_config (channel, permits_per_sec, burst) VALUES ('SMS', 20, 40);

INSERT INTO rate_limit_config (channel, permits_per_sec, burst) VALUES ('PUSH', 100, 200);

-- Transactional outbox: the API writes the notification row and its outbox row
-- in one transaction; a relay publishes outbox rows to Kafka afterwards. This
-- closes the dual-write gap between Oracle and Kafka.
CREATE TABLE outbox (
    seq          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic        VARCHAR2(200)  NOT NULL,
    msg_key      VARCHAR2(320)  NOT NULL,
    payload      CLOB           NOT NULL,
    headers_json VARCHAR2(2000),
    created_at   TIMESTAMP      DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX ix_outbox_unpublished ON outbox (published_at, seq);
