# eventDumper

outputs something like:

```
BlockID		 : 045d80390e81bc2f62a29446528a647719f9ccf8690b37407c2b4ebbd73a91ee
TransactionID	 : 280d3f24b42d2d4409b7ac19b003b2cb8a63e7dc86f86ba7d12f98493394dd81
TransactionIndex : 00000000
EventIndex	 : 00000009
Event Type	: A.0b2a3299cc857e29.TopShot.Deposit
Event Payload	: {"type":"Event","value":{"id":"A.0b2a3299cc857e29.TopShot.Deposit","fields":[{"name":"id","value":{"type":"UInt64","value":"548929"}},{"name":"to","value":{"type":"Optional","value":{"type":"Address","value":"0xed8707e2ae5bba5a"}}}]}}
```


- download protocolStateArchive for spork
- change flow-go-sdk , flow-go versions according to spork 
- NOTE: events are not ordered ( blockID and transactionID ordered technically, which means no order ) 
