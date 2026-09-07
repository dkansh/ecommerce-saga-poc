create table inventory_stock (
    sku text primary key,
    available integer not null check (available >= 0)
);

insert into inventory_stock(sku, available) values
    ('SKU-1', 100),
    ('SOLD-OUT', 0);

create table inventory_reservation (
    saga_id uuid primary key,
    sku text not null,
    quantity integer not null check (quantity > 0),
    state text not null check (state in ('RESERVED', 'REJECTED', 'RELEASED')),
    created_at timestamptz not null default current_timestamp,
    updated_at timestamptz not null default current_timestamp
);
