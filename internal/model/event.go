package model

import "time"

const (
	CategoryEventTypeCreated = "Created"
	CategoryEventTypeUpdated = "Updated"
	CategoryEventTypeRemoved = "Removed"

	CategoryEventStatusPending = "pending"
	CategoryEventStatusLocked  = "locked"
)

type CategoryEvent struct {
	ID         uint64    `db:"id"`
	CategoryID uint64    `db:"category_id"`
	Type       string    `db:"type"`
	Status     string    `db:"status"`
	Payload    []byte    `db:"payload"`
	Updated    time.Time `db:"updated"`
}
