package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "orbit/internal/rpc/orbitv1/orbit/v1"
)

func main() {
	address := flag.String("controller", "127.0.0.1:9000", "controller address")
	id := flag.String("id", "worker-1", "worker ID")
	cpu := flag.Int("cpu", 8, "CPU capacity")
	memory := flag.Int("memory-mb", 16_384, "memory capacity in MB")
	gpu := flag.Int("gpu", 0, "GPU capacity")
	duration := flag.Duration("duration", time.Second, "simulated job duration")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	connection, err := grpc.DialContext(ctx, *address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		slog.Error("connect controller", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	stream, err := v1.NewOrbitControllerClient(connection).WorkerSession(ctx)
	if err != nil {
		slog.Error("open worker session", "error", err)
		os.Exit(1)
	}
	session := *id + "-" + time.Now().Format("150405.000000000")
	if err := stream.Send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_Register{Register: &v1.Worker{Id: *id, SessionId: session, Total: &v1.ResourceRequest{Cpu: int32(*cpu), MemoryMb: int32(*memory), Gpu: int32(*gpu)}}}}); err != nil {
		slog.Error("register worker", "error", err)
		os.Exit(1)
	}

	var sendMu sync.Mutex
	send := func(message *v1.WorkerSessionMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(message)
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_HeartbeatSessionId{HeartbeatSessionId: session}}); err != nil {
					return
				}
			}
		}
	}()
	for {
		message, err := stream.Recv()
		if err != nil {
			return
		}
		assignment := message.GetAssignment()
		if assignment == nil {
			continue
		}
		go func(assignment *v1.Assignment) {
			time.Sleep(*duration)
			_ = send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_Completion{Completion: &v1.Completion{Assignment: assignment, Success: true}}})
		}(assignment)
	}
}
