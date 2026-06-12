-- +goose Up
CREATE TABLE categories (
  id BIGSERIAL PRIMARY KEY,
  foo TEXT NOT NULL,
  removed BOOLEAN DEFAULT FALSE NOT NULL,
  created TIMESTAMP DEFAULT NOW() NOT NULL,
  updated TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE TABLE categories_events (
  id BIGSERIAL PRIMARY KEY,
  category_id BIGINT NOT NULL,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  payload JSONB NOT NULL,
  updated TIMESTAMP DEFAULT NOW() NOT NULL
);

ALTER TABLE categories_events
ADD CONSTRAINT fk_categories_events_categories
FOREIGN KEY (category_id)
REFERENCES categories (id)
ON DELETE CASCADE;

CREATE INDEX idx_categories_not_removed_id ON categories (id) WHERE removed = false;

CREATE INDEX idx_categories_events_status_updated ON categories_events (status, updated);

-- +goose Down
DROP TABLE IF EXISTS categories_events;
DROP TABLE IF EXISTS categories;
