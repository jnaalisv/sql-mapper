delete from products;

insert
into products(product_type, product_code, rank, unit_price)
values
  ('TINY', 'A1', 3, 12.5),
  ('SMALL', 'B2', 2, 22.5),
  ('BIG', 'C3', 1, 14);
