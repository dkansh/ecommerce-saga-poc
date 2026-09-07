CREATE TABLE inbox (
 message_id uuid PRIMARY KEY,
 received_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE outbox (
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 message_key text NOT NULL,
 topic text NOT NULL,
 payload text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 sent_at timestamptz
);
CREATE INDEX outbox_pending ON outbox(sequence) WHERE sent_at IS NULL;
CREATE TABLE failed_message (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 topic text NOT NULL,
 partition_id integer NOT NULL,
 offset_id bigint NOT NULL,
 message_key text,
 payload text NOT NULL,
 error text NOT NULL,
 failed_at timestamptz NOT NULL DEFAULT now(),
 replayed_at timestamptz,
 UNIQUE(topic, partition_id, offset_id)
);
CREATE TABLE fault_attempt (
 saga_id uuid NOT NULL,
 fault text NOT NULL,
 attempts integer NOT NULL,
 PRIMARY KEY(saga_id, fault)
);
CREATE TABLE fault_unblocked (saga_id uuid PRIMARY KEY);
