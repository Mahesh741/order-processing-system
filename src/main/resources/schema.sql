create table if not exists orders (
    id                uuid primary key,
    customer_id       varchar(255) not null,
    sku               varchar(255) not null,
    quantity          integer      not null,
    amount_cents      bigint       not null,
    status            varchar(255) not null,
    attempts          integer      not null,
    failure_reason    varchar(255),
    payment_reference varchar(255),
    created_at        timestamp(6) with time zone not null,
    updated_at        timestamp(6) with time zone not null
);

create index if not exists orders_created_at_idx on orders (created_at desc);

create table if not exists dead_letters (
    id           uuid primary key,
    order_id     uuid         not null references orders (id),
    source_topic varchar(255) not null,
    reason       varchar(255) not null,
    created_at   timestamp(6) with time zone not null,
    replayed_at  timestamp(6) with time zone
);

create index if not exists dead_letters_order_id_idx on dead_letters (order_id);
