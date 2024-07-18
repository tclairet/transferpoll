package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

const duplicateErr = "UNIQUE constraint failed: transfers.id"

const create string = `
  CREATE TABLE IF NOT EXISTS transfers (
  id TEXT PRIMARY KEY,    
  value TEXT,
  sender TEXT,
  receiver TEXT,
  block INTEGER
  );`

type sqliteStore struct {
	db *sql.DB
}

func newSqlite(file string) (*sqliteStore, error) {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(create); err != nil {
		return nil, err
	}
	return &sqliteStore{db}, nil
}

func (s sqliteStore) Add(transfer Transfer) error {
	_, err := s.db.Exec("INSERT INTO transfers VALUES(?,?,?,?,?);", transfer.ID, transfer.From, transfer.To, transfer.Value, transfer.Block)
	if err != nil {
		// ignore duplicate error
		if strings.Contains(err.Error(), duplicateErr) {
			return nil
		}
		return err
	}
	return nil
}

func (s sqliteStore) Read() ([]Transfer, error) {
	rows, err := s.db.Query("SELECT * FROM transfers")
	if err != nil {
		return nil, err
	}
	var transfers []Transfer
	for rows.Next() {
		transfer := Transfer{}
		if err := rows.Scan(&transfer.ID, &transfer.From, &transfer.To, &transfer.Value, &transfer.Block); err != nil {
			return nil, err
		}
		transfers = append(transfers, transfer)
	}
	return transfers, nil
}
