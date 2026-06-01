package model

type Category struct {
	ID  uint64 `db:"id"`
	Foo string `db:"foo"`
}
