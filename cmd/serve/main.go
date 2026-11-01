package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DrMkdaddy/kv-serving-scheduler/internal/proxy"
)

func main() {
	configPath := flag.String("config", "configs/qwen3-4b-real.json", "proxy config")
	listen := flag.String("listen", "", "override listen address")
	upstream := flag.String("upstream", "", "override upstream llama-server URL")
	flag.Parse()

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	var cfg proxy.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatal(err)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *upstream != "" {
		cfg.Upstream = *upstream
	}
	srv, err := proxy.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		httpServer.Close()
	}()
	fmt.Printf("scheduler listening on %s -> %s (%d slots, %d blocks of %d tokens)\n",
		cfg.Listen, cfg.Upstream, cfg.Scheduler.Slots, cfg.Scheduler.TotalBlocks, cfg.Scheduler.BlockSize)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
