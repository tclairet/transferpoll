package main

import (
	"cloud.google.com/go/bigtable"
	"cmp"
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"slices"
	"strconv"
	"strings"
)

const duplicateErr = "UNIQUE constraint failed: transfers.id"
const tableName = "transfers"
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

type store interface {
	Add(transfer Transfer) error
	Read() ([]Transfer, error)
	Close() error
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

func (s sqliteStore) Close() error {
	return s.db.Close()
}

var columnFamilyNames = []string{"id", "from", "to", "value", "block"}

type bigTableStore struct {
	client *bigtable.Client
}

func (b bigTableStore) Add(transfer Transfer) error {
	tbl := b.client.Open(tableName)
	mut := bigtable.NewMutation()
	mut.Set("id", "id", bigtable.Now(), []byte(transfer.ID))
	mut.Set("from", "from", bigtable.Now(), []byte(transfer.From))
	mut.Set("to", "to", bigtable.Now(), []byte(transfer.To))
	mut.Set("value", "value", bigtable.Now(), []byte(transfer.Value))
	mut.Set("block", "block", bigtable.Now(), []byte(fmt.Sprintf("%d", transfer.Block)))

	if err := tbl.Apply(context.Background(), transfer.ID, mut); err != nil {
		return fmt.Errorf("could not apply row mutation: %v", err)
	}
	return nil
}

func (b bigTableStore) Read() ([]Transfer, error) {
	tbl := b.client.Open(tableName)

	var transfers []Transfer
	err := tbl.ReadRows(context.Background(), bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		block, err := strconv.Atoi(string(row["block"][0].Value))
		if err != nil {
			return false
		}
		transfers = append(transfers, Transfer{
			ID:    string(row["id"][0].Value),
			From:  string(row["from"][0].Value),
			To:    string(row["to"][0].Value),
			Value: string(row["value"][0].Value),
			Block: uint64(block),
		})
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("could not ReadRows: %v", err)
	}

	slices.SortFunc(transfers, func(a, b Transfer) int {
		return cmp.Compare(a.Block, b.Block)
	})

	return transfers, nil
}

func (b bigTableStore) Close() error {
	return b.client.Close()
}

func bigTable() (*bigTableStore, error) {
	project := flag.String("project", "onboarding-435910", "The Google Cloud Platform project ID. Required.")
	instance := flag.String("instance", "testn1", "The Google Cloud Bigtable instance ID. Required.")
	flag.Parse()

	for _, f := range []string{"project", "instance"} {
		if flag.Lookup(f).Value.String() == "" {
			return nil, fmt.Errorf("the %s flag is required", f)
		}
	}

	ctx := context.Background()

	adminClient, err := bigtable.NewAdminClient(ctx, *project, *instance)
	if err != nil {
		return nil, fmt.Errorf("could not create admin client: %v", err)
	}
	defer adminClient.Close()

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch table list: %v", err)
	}

	if !slices.Contains(tables, tableName) {
		if err := adminClient.CreateTable(ctx, tableName); err != nil {
			return nil, fmt.Errorf("could not create table %s: %v", tableName, err)
		}
	}

	tblInfo, err := adminClient.TableInfo(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("could not read info for table %s: %v", tableName, err)
	}

	for _, columnFamilyName := range columnFamilyNames {
		if !slices.Contains(tblInfo.Families, columnFamilyName) {
			if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
				return nil, fmt.Errorf("could not create column family %s: %v", columnFamilyName, err)
			}
		}
	}

	client, err := bigtable.NewClient(ctx, *project, *instance)
	if err != nil {
		return nil, fmt.Errorf("could not create data operations client: %v", err)
	}

	return &bigTableStore{client: client}, nil
}
