package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/controller"
	"github.com/MuhammadMaazA/Orbit/internal/metrics"
	"github.com/MuhammadMaazA/Orbit/internal/rpc"
	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
	"github.com/MuhammadMaazA/Orbit/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
	address := flag.String("addr", ":9000", "listen address")
	metricsAddress := flag.String("metrics-addr", ":9090", "metrics and health listen address")
	timeout := flag.Duration("worker-timeout", 5*time.Second, "worker heartbeat timeout")
	policyName := flag.String("policy", "first-fit", "scheduling policy: first-fit, best-fit, bin-pack, or energy")
	maxAttempts := flag.Int("max-attempts", 3, "maximum attempts per job")
	maxQueuedJobs := flag.Int("max-queued-jobs", 1_000, "maximum queued jobs; zero disables the limit")
	agingInterval := flag.Duration("aging-interval", 30*time.Second, "time required for a queued job to gain one effective priority level")
	dataDir := flag.String("data-dir", "", "directory for WAL and snapshots; empty disables persistence")
	powerIdle := flag.Float64("power-idle", 100, "idle worker power in watts")
	powerCPU := flag.Float64("power-cpu", 10, "power per allocated CPU in watts")
	powerGPU := flag.Float64("power-gpu", 50, "power per allocated GPU in watts")
	flag.Parse()

	if *timeout <= 0 {
		slog.Error("worker timeout must be positive")
		os.Exit(2)
	}
	policy, err := policyForName(*policyName)
	if err != nil {
		slog.Error("invalid policy", "error", err)
		os.Exit(2)
	}
	config := controller.Config{MaxAttempts: *maxAttempts, MaxQueuedJobs: *maxQueuedJobs, AgingInterval: *agingInterval, PowerIdleWatts: *powerIdle, PowerCPUWatts: *powerCPU, PowerGPUWatts: *powerGPU}
	if *dataDir != "" {
		store, storeErr := storage.NewFileStore(*dataDir)
		if storeErr != nil {
			slog.Error("create store", "error", storeErr)
			os.Exit(1)
		}
		config.Store = store
	}
	state, err := controller.NewWithConfig(policy, config)
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
	rpcServer := rpc.NewServer(state, instrumentation)
	v1.RegisterOrbitControllerServer(server, rpcServer)
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
				if err := rpcServer.ExpireWorkers(time.Now(), *timeout); err != nil {
					slog.Error("expire workers", "error", err)
				}
				stats := state.Stats()
				instrumentation.SetGauges(stats.Workers, stats.Draining, stats.Queued, stats.Running)
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

func policyForName(name string) (scheduler.Policy, error) {
	switch name {
	case "first-fit":
		return scheduler.FirstFit{}, nil
	case "best-fit":
		return scheduler.BestFit{}, nil
	case "bin-pack":
		return scheduler.BinPack{}, nil
	case "energy":
		return scheduler.EnergyAware{}, nil
	default:
		return nil, fmt.Errorf("unsupported policy %q", name)
	}
}
