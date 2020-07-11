package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"incomeBlockchain/blockchain"
	"incomeBlockchain/config"
	"math"
	"net/http"
	"strconv"
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

func incomeHandler(w http.ResponseWriter, r *http.Request) {

	incomeStr := r.PostFormValue("income")
	if incomeStr != "" {
		income, err := strconv.Atoi(incomeStr)
		if err == nil && income < math.MaxInt32 && income > math.MinInt32 {
			incomeBlockchain.AddBlock(int32(income))
		}
	}

	t := template.Must(template.ParseFiles("index.html"))

	data := htmlData{
		Blocks: incomeBlockchain.Blocks,
	}

	t.Execute(w, data)
	//w.Write([]byte(hex.EncodeToString(incomeBlockchain.Blocks[0].Hash[:])))
}

var incomeBlockchain *blockchain.Blockchain

func init() {
	var configFile string
	flag.StringVar(&configFile, "c", "config/config.json", "specify config file's path")
	flag.Parse()

	config.InitConfig(configFile)

	incomeBlockchain = blockchain.NewBlockchain()
}

func main() {
	fmt.Println("income blockchain", "0.1", "started at", config.Cfg.Address)

	mux := http.NewServeMux()
	mux.HandleFunc("/", incomeHandler)

	server := &http.Server{
		Addr:           config.Cfg.Address,
		Handler:        mux,
		ReadTimeout:    time.Duration(config.Cfg.ReadTimeout * int64(time.Second)),
		WriteTimeout:   time.Duration(config.Cfg.WriteTimeout * int64(time.Second)),
		MaxHeaderBytes: 1 << 20,
	}
	server.ListenAndServe()
}
