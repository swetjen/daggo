package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func NewTest() (*Queries, *sql.DB, error) {
	dsn := fmt.Sprintf("file:daggo-test-%d?mode=memory&cache=shared", time.Now().UnixNano())
	return Open(context.Background(), dsn)
}
