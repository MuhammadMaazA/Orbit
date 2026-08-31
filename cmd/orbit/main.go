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
	if len(os.Args) < 2 || (os.Args[1] != "submit" && os.Args[1] != "status" && os.Args[1] != "drain" && os.Args[1] != "undrain") {
		fmt.Fprintln(os.Stderr, "usage: orbit submit|status|drain|undrain [flags]")
		os.Exit(2)
	}
	flags := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	address := flags.String("controller", "127.0.0.1:9000", "controller address")
	id := flags.String("id", "", "job ID")
	if os.Args[1] == "drain" || os.Args[1] == "undrain" {
		_ = flags.Parse(os.Args[2:])
		if *id == "" {
			slog.Error("worker ID is required")
			os.Exit(2)
		}
		connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("connect controller", "error", err)
			os.Exit(1)
		}
		defer connection.Close()
		client := v1.NewOrbitControllerClient(connection)
		var response *v1.WorkerStateResponse
		if os.Args[1] == "drain" {
			response, err = client.DrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: *id})
		} else {
			response, err = client.UndrainWorker(context.Background(), &v1.WorkerStateRequest{WorkerId: *id})
		}
		if err != nil {
			slog.Error("change worker state", "error", err)
			os.Exit(1)
		}
		fmt.Printf("%s draining=%t\n", *id, response.Draining)
		return
	}
	if os.Args[1] == "status" {
		_ = flags.Parse(os.Args[2:])
		connection, err := grpc.Dial(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("connect controller", "error", err)
			os.Exit(1)
		}
		defer connection.Close()
		client := v1.NewOrbitControllerClient(connection)
		if *id == "" {
			jobs, listErr := client.ListJobs(context.Background(), &v1.JobListRequest{})
			if listErr != nil {
				slog.Error("list jobs", "error", listErr)
				os.Exit(1)
			}
			for _, job := range jobs.Jobs {
				fmt.Printf("%s status=%s attempt=%d worker=%s assignment=%s\n", job.Job.Id, job.Status, job.Attempt, job.WorkerId, job.AssignmentId)
			}
			return
		}
		jobStatus, statusErr := client.GetJob(context.Background(), &v1.JobStatusRequest{JobId: *id})
		if statusErr != nil {
			slog.Error("get job", "error", statusErr)
			os.Exit(1)
		}
		fmt.Printf("%s status=%s attempt=%d worker=%s assignment=%s\n", jobStatus.Job.Id, jobStatus.Status, jobStatus.Attempt, jobStatus.WorkerId, jobStatus.AssignmentId)
		return
	}
	cpu := flags.Int("cpu", 1, "CPU request")
	memory := flags.Int("memory-mb", 1, "memory request in MB")
	gpu := flags.Int("gpu", 0, "GPU request")
	priority := flags.Int("priority", 1, "job priority")
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
	assignment, err := v1.NewOrbitControllerClient(connection).Submit(context.Background(), &v1.Job{Id: *id, Resources: &v1.ResourceRequest{Cpu: int32(*cpu), MemoryMb: int32(*memory), Gpu: int32(*gpu)}, Priority: int32(*priority)})
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
