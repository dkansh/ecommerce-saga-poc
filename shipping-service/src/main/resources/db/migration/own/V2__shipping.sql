create table shipment (
    saga_id uuid primary key,
    sku text not null,
    quantity integer not null check (quantity > 0),
    state text not null check (state in ('BOOKED', 'REJECTED', 'CANCELLED')),
    created_at timestamptz not null default current_timestamp,
    updated_at timestamptz not null default current_timestamp
);
