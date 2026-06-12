package model

import "time"

type Category struct {
	ID      uint64    `db:"id"`
	Foo     string    `db:"foo"`
	Removed bool      `db:"removed"`
	Created time.Time `db:"created"`
	Updated time.Time `db:"updated"`
}
