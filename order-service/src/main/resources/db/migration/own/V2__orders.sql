CREATE TABLE orders (
 id uuid PRIMARY KEY,
 idempotency_key varchar(128) NOT NULL UNIQUE,
 request_json text NOT NULL,
 message_json text NOT NULL,
 status varchar(32) NOT NULL,
 cancel_mask integer NOT NULL DEFAULT 0 CHECK (cancel_mask BETWEEN 0 AND 7),
 reason text NOT NULL DEFAULT '',
 deadline timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_due ON orders(deadline)
 WHERE status NOT IN ('COMPLETED','CANCELLED','MANUAL_INTERVENTION');
CREATE TABLE saga_history (
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 saga_id uuid NOT NULL REFERENCES orders(id),
 event text NOT NULL,
 status text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX saga_history_order ON saga_history(saga_id,sequence);
