package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"orbit/internal/controller"
	"orbit/internal/rpc"
	v1 "orbit/internal/rpc/orbitv1/orbit/v1"
	"orbit/internal/scheduler"
)

func main() {
	address := flag.String("addr", ":9000", "listen address")
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
	v1.RegisterOrbitControllerServer(server, rpc.NewServer(state))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		ticker := time.NewTicker(*timeout / 2)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := state.ExpireWorkers(time.Now(), *timeout); err != nil {
				slog.Error("expire workers", "error", err)
			}
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil {
			slog.Error("serve", "error", err)
		}
	}()
	slog.Info("controller listening", "address", *address)
	<-stop
	server.GracefulStop()
}
