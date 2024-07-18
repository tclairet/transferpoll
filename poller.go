package main

import (
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

type Transfer struct {
	ID    string
	From  string
	To    string
	Value string
	Block uint64
}

type poller struct {
	client bind.ContractBackend
}

func (poller *poller) getErc20Transfers(address common.Address, block uint64) ([]Transfer, error) {
	token, err := NewToken(address, poller.client)
	if err != nil {
		return nil, err
	}

	transfersIterator, err := token.FilterTransfer(&bind.FilterOpts{Start: block, End: &block}, nil, nil)
	if err != nil {
		return nil, err
	}
	defer transfersIterator.Close()

	var transfers []Transfer
	for transfersIterator.Next() {
		transfers = append(transfers, Transfer{
			ID:    fmt.Sprintf("%s:%d", transfersIterator.Event.Raw.TxHash, transfersIterator.Event.Raw.Index),
			From:  transfersIterator.Event.From.String(),
			To:    transfersIterator.Event.To.String(),
			Value: transfersIterator.Event.Value.String(),
			Block: transfersIterator.Event.Raw.BlockNumber,
		})
	}
	if err := transfersIterator.Error(); err != nil {
		return nil, err
	}
	return transfers, nil
}
