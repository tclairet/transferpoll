Write a go application that uses the eth json rpc api to 
extract all USDC token transfers included in a given ethereum mainnet block 
saves them into a sqlite database (each row should include at least sender / recipient and the value that was transferred)
Public provider: https://github.com/arddluma/awesome-list-rpc-nodes-providers?tab=readme-ov-file#ethereum

# transferpoll

Retrieve USDC transfer from an ethereum rpc client and save them to an sqlite db.

## run

By default it will poll the last block.

```
./transferpoll
```

You can also specify the block you want. 

```
./transferpoll <block number>
```