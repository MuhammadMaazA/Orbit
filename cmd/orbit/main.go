package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "orbit/internal/rpc/orbitv1/orbit/v1"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "submit" {
		fmt.Fprintln(os.Stderr, "usage: orbit submit -id JOB_ID -cpu CPU -memory-mb MB [-gpu GPU] [-controller ADDRESS]")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("submit", flag.ExitOnError)
	address := flags.String("controller", "127.0.0.1:9000", "controller address")
	id := flags.String("id", "", "job ID")
	cpu := flags.Int("cpu", 1, "CPU request")
	memory := flags.Int("memory-mb", 1, "memory request in MB")
	gpu := flags.Int("gpu", 0, "GPU request")
	_ = flags.Parse(os.Args[2:])
	if *id == "" {
		slog.Error("job ID is required")
		os.Exit(2)
	}
	connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("connect controller", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	assignment, err := v1.NewOrbitControllerClient(connection).Submit(context.Background(), &v1.Job{Id: *id, Resources: &v1.ResourceRequest{Cpu: int32(*cpu), MemoryMb: int32(*memory), Gpu: int32(*gpu)}})
	if err != nil {
		slog.Error("submit job", "error", err)
		os.Exit(1)
	}
	if assignment.Id == "" {
		fmt.Println("queued")
	} else {
		fmt.Printf("assigned %s to %s\n", assignment.Id, assignment.WorkerId)
	}
}
