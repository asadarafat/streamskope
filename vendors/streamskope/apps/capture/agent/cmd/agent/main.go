package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	agent "github.com/asadarafat/streamskope/vendors/streamskope/apps/capture/agent"
)

func main() {
	store, err := agent.NewInClusterKubernetesStore()
	if err != nil {
		log.Fatalf("capture agent startup failed: %v", err)
	}
	manager := agent.NewSessionManager(store, time.Now)
	tunnel := agent.NewTunnelHandler(agent.TunnelLimits{
		IdleTimeout:    60 * time.Second,
		MaxConnections: 8,
		MaxFrameBytes:  16 * 1024 * 1024,
		WriteTimeout:   10 * time.Second,
	}, manager.BrokerAddress)
	server := &http.Server{
		Addr:              ":8080",
		Handler:           agent.NewAPIHandler(manager, tunnel),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      65 * time.Second,
		IdleTimeout:       75 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go reconcile(ctx, manager)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			log.Printf("capture agent shutdown failed: %v", err)
		}
	}()
	log.Printf("StreamSkope Capture agent listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("capture agent failed: %v", err)
	}
}

func reconcile(ctx context.Context, manager *agent.SessionManager) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		reconcileCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		if err := manager.Reconcile(reconcileCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("capture reconciliation failed: %v", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
