package network

import (
	"encoding/hex"
	"net/http"
	"p2pFirst/blockchain"
	"text/template"
	"time"
)

type htmlData struct {
	Blocks []*blockchain.Block
}

func (data htmlData) TimeStampToString(timeStamp int32) string {
	return time.Unix(int64(timeStamp), 0).Format("2006-01-02 15:04:05")
}

func (data htmlData) HashToString(hash [32]byte) string {
	return hex.EncodeToString(hash[:])
}

func handlerUI(w http.ResponseWriter, r *http.Request) {

	dataStr := r.PostFormValue("data")
	if dataStr != "" {
		MyBlockchain.AddBlock(dataStr)
		go broadAddBlock(MyBlockchain.Blocks[len(MyBlockchain.Blocks)-1])
	}

	t := template.Must(template.ParseFiles("index.html"))

	data := htmlData{
		Blocks: MyBlockchain.Blocks,
	}

	t.Execute(w, data)
}
