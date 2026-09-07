create table payment (
    saga_id uuid primary key,
    amount_cents bigint not null check (amount_cents >= 0),
    state text not null check (state in ('CHARGED', 'REJECTED', 'REFUNDED')),
    created_at timestamptz not null default current_timestamp,
    updated_at timestamptz not null default current_timestamp
);
