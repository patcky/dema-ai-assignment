create schema company_schema;
create table if not exists company_schema.raw_products (
  payload jsonb,
  timestamp timestamp
);
create table if not exists company_schema.raw_orders (
  payload jsonb,
  timestamp timestamp
);
create table if not exists company_schema.products (
  productid varchar(255) primary key not null,
  name varchar(255) not null,
  quantity integer not null,
  category varchar(255) not null,
  subcategory varchar(255) not null
);
create table if not exists company_schema.orders (
  orderid varchar(255) primary key not null,
  productid varchar(255) not null,
  currency varchar(255) not null,
  quantity integer not null,
  shippingcost numeric not null,
  amount numeric not null,
  channel varchar(255) not null,
  channelgroup varchar(255) not null,
  campaign varchar(255),
  datetime timestamp not null,
  foreign key (productid) references company_schema.products(productid)
);
create table if not exists company_schema.errors (
  recordid varchar(255) primary key,
  recordtype varchar(255),
  errors jsonb,
  timestamp timestamp
);

create index if not exists product_category_index on company_schema.products (category);
create index if not exists product_subcategory_index on company_schema.products (subcategory);
create index if not exists order_channel_index on company_schema.orders (channel);
create index if not exists order_channelgroup_index on company_schema.orders (channelgroup);
create index if not exists order_campaign_index on company_schema.orders (campaign);
create index if not exists order_date_index on company_schema.orders (date(datetime));
create index if not exists error_date_index on company_schema.errors (date(timestamp));

cluster company_schema.orders using order_date_index;
cluster company_schema.errors using error_date_index;
