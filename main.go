package main

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"log"
	"log/slog"
	"os"
	"strconv"
)

var usdc = common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")

func main() {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/ed3aa9894bb04b2994eb5268fe3c7d22")
	if err != nil {
		log.Fatalf("invalid eth client: %s", err)
	}

	store, err := newSqlite("./transfers.db")
	if err != nil {
		log.Fatalf("cannot open db: %s", err)
	}

	var block uint64
	if len(os.Args) < 2 {
		slog.Info("no block provided, using last block")
		block, err = client.BlockNumber(context.Background())
		if err != nil {
			log.Fatalf("cannot get last block: %s", err)
		}
	} else {
		blockStr := os.Args[1]
		b, err := strconv.ParseInt(blockStr, 10, 64)
		if err != nil {
			log.Fatalf("invalid block provided: %s", err)
		}
		block = uint64(b)
	}
	slog.Info("poll", "block", block)

	poller := poller{client: client}

	transfers, err := poller.getErc20Transfers(usdc, block)
	if err != nil {
		log.Fatalf("cannot retrieve ERC20 transfers: %s", err)
	}

	for _, transfer := range transfers {
		if err := store.Add(transfer); err != nil {
			slog.Error("save transfer", "message", err)
		}
	}
	transfers, err = store.Read()
	if err != nil {
		log.Fatalf("cannot read db: %s", err)
	}
	for _, transfer := range transfers {
		fmt.Println(transfer)
	}
}
