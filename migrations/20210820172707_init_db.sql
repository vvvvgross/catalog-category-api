-- +goose Up
CREATE TABLE Category (
  id BIGSERIAL PRIMARY KEY,
  foo VARCHAR
);

-- +goose Down
DROP TABLE Category;
