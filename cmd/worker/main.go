package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	address := flag.String("controller", "127.0.0.1:9000", "controller address")
	id := flag.String("id", "worker-1", "worker ID")
	cpu := flag.Int("cpu", 8, "CPU capacity")
	memory := flag.Int("memory-mb", 16_384, "memory capacity")
	gpu := flag.Int("gpu", 0, "GPU capacity")
	duration := flag.Duration("duration", time.Second, "simulated job duration")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		if err := runSession(ctx, *address, *id, *cpu, *memory, *gpu, *duration); err != nil && ctx.Err() == nil {
			slog.Warn("worker session ended", "error", err, "retry_in", backoff)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

func runSession(parent context.Context, address, id string, cpu, memory, gpu int, duration time.Duration) error {
	sessionContext, cancelSession := context.WithCancel(parent)
	defer cancelSession()

	dialContext, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect controller: %w", err)
	}
	defer connection.Close()
	if err := waitUntilReady(dialContext, connection); err != nil {
		return fmt.Errorf("connect controller: %w", err)
	}
	stream, err := v1.NewOrbitControllerClient(connection).WorkerSession(sessionContext)
	if err != nil {
		return fmt.Errorf("open worker session: %w", err)
	}
	session := fmt.Sprintf("%s-%d", id, time.Now().UnixNano())
	sendMu := sync.Mutex{}
	send := func(message *v1.WorkerSessionMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(message)
	}
	if err := send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_Register{Register: &v1.Worker{Id: id, SessionId: session, Total: &v1.ResourceRequest{Cpu: int32(cpu), MemoryMb: int32(memory), Gpu: int32(gpu)}}}}); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	go sendHeartbeats(sessionContext, session, send)
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		assignment := message.GetAssignment()
		if assignment == nil {
			continue
		}
		go completeAfter(sessionContext, assignment, duration, send)
	}
}

func waitUntilReady(ctx context.Context, connection *grpc.ClientConn) error {
	connection.Connect()
	for {
		state := connection.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !connection.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func sendHeartbeats(ctx context.Context, session string, send func(*v1.WorkerSessionMessage) error) {
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
}

func completeAfter(ctx context.Context, assignment *v1.Assignment, duration time.Duration, send func(*v1.WorkerSessionMessage) error) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		_ = send(&v1.WorkerSessionMessage{Payload: &v1.WorkerSessionMessage_Completion{Completion: &v1.Completion{Assignment: assignment, Success: true}}})
	}
}
