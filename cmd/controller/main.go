package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"orbit/internal/controller"
	"orbit/internal/metrics"
	"orbit/internal/rpc"
	v1 "orbit/internal/rpc/orbitv1/orbit/v1"
	"orbit/internal/scheduler"
)

func main() {
	address := flag.String("addr", ":9000", "listen address")
	metricsAddress := flag.String("metrics-addr", ":9090", "metrics and health listen address")
	timeout := flag.Duration("worker-timeout", 5*time.Second, "worker heartbeat timeout")
	flag.Parse()

	state, err := controller.New(scheduler.FirstFit{}, 3)
	if err != nil {
		slog.Error("create controller", "error", err)
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		slog.Error("listen", "error", err)
		os.Exit(1)
	}
	server := grpc.NewServer()
	registry := prometheus.NewRegistry()
	instrumentation := metrics.New(registry)
	v1.RegisterOrbitControllerServer(server, rpc.NewServer(state, instrumentation))
	httpMux := http.NewServeMux()
	httpMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	httpMux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusOK) })
	httpMux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) { response.WriteHeader(http.StatusOK) })
	httpServer := &http.Server{Addr: *metricsAddress, Handler: httpMux}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	shutdown := make(chan struct{})
	go func() {
		ticker := time.NewTicker(*timeout / 2)
		defer ticker.Stop()
		for {
			select {
			case <-shutdown:
				return
			case <-ticker.C:
				if _, err := state.ExpireWorkers(time.Now(), *timeout); err != nil {
					slog.Error("expire workers", "error", err)
				}
				stats := state.Stats()
				instrumentation.SetGauges(stats.Workers, stats.Queued, stats.Running)
			}
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil {
			slog.Error("serve", "error", err)
		}
	}()
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server", "error", err)
		}
	}()
	slog.Info("controller listening", "address", *address)
	<-stop
	close(shutdown)
	server.GracefulStop()
	_ = httpServer.Shutdown(context.Background())
}
