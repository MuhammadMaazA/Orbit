package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"orbit/internal/controller"
	"orbit/internal/model"
	v1 "orbit/internal/rpc/orbitv1/orbit/v1"
)

type Server struct {
	v1.UnimplementedOrbitControllerServer
	controller *controller.Controller
}

func NewServer(controller *controller.Controller) *Server {
	return &Server{controller: controller}
}

func (s *Server) Submit(_ context.Context, request *v1.Job) (*v1.Assignment, error) {
	if request == nil || request.Resources == nil {
		return nil, status.Error(codes.InvalidArgument, "job and resources are required")
	}
	assignments, err := s.controller.Submit(fromJob(request))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(assignments) == 0 {
		return &v1.Assignment{}, nil
	}
	return toAssignment(assignments[0]), nil
}

func (s *Server) WorkerSession(stream v1.OrbitController_WorkerSessionServer) error {
	var workerID, sessionID string
	for {
		message, err := stream.Recv()
		if err != nil {
			if workerID != "" {
				_, _ = s.controller.WorkerLost(workerID, sessionID)
			}
			return err
		}
		switch payload := message.Payload.(type) {
		case *v1.WorkerSessionMessage_Register:
			workerID, sessionID = payload.Register.Id, payload.Register.SessionId
			assignments, err := s.controller.RegisterWorker(workerID, sessionID, fromCapacity(payload.Register.Total))
			if err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			if err := sendAssignments(stream, assignments); err != nil {
				return err
			}
		case *v1.WorkerSessionMessage_HeartbeatSessionId:
			if err := s.controller.Heartbeat(workerID, payload.HeartbeatSessionId, now()); err != nil {
				return status.Error(codes.PermissionDenied, err.Error())
			}
			if err := stream.Send(&v1.ControllerSessionMessage{Payload: &v1.ControllerSessionMessage_HeartbeatAckSessionId{HeartbeatAckSessionId: payload.HeartbeatSessionId}}); err != nil {
				return err
			}
		case *v1.WorkerSessionMessage_Completion:
			if payload.Completion.Assignment == nil {
				return status.Error(codes.InvalidArgument, "completion assignment is required")
			}
			assignments, accepted, err := s.controller.Complete(fromAssignment(payload.Completion.Assignment), payload.Completion.Success)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			if accepted {
				if err := sendAssignments(stream, assignments); err != nil {
					return err
				}
			}
		}
	}
}

var now = func() time.Time { return time.Now() }

func sendAssignments(stream v1.OrbitController_WorkerSessionServer, assignments []controller.Assignment) error {
	for _, assignment := range assignments {
		if err := stream.Send(&v1.ControllerSessionMessage{Payload: &v1.ControllerSessionMessage_Assignment{Assignment: toAssignment(assignment)}}); err != nil {
			return err
		}
	}
	return nil
}

func fromJob(job *v1.Job) model.Job {
	return model.Job{ID: job.Id, CPU: int(job.Resources.Cpu), MemoryMB: int(job.Resources.MemoryMb), GPU: int(job.Resources.Gpu)}
}

func fromCapacity(resources *v1.ResourceRequest) model.Capacity {
	total := model.ResourceRequest{CPU: int(resources.Cpu), MemoryMB: int(resources.MemoryMb), GPU: int(resources.Gpu)}
	return model.Capacity{Total: total, Available: total}
}

func fromAssignment(assignment *v1.Assignment) controller.Assignment {
	return controller.Assignment{ID: assignment.Id, Job: fromJob(assignment.Job), WorkerID: assignment.WorkerId, SessionID: assignment.SessionId, Attempt: int(assignment.Attempt)}
}

func toAssignment(assignment controller.Assignment) *v1.Assignment {
	return &v1.Assignment{Id: assignment.ID, Job: &v1.Job{Id: assignment.Job.ID, Resources: &v1.ResourceRequest{Cpu: int32(assignment.Job.CPU), MemoryMb: int32(assignment.Job.MemoryMB), Gpu: int32(assignment.Job.GPU)}}, WorkerId: assignment.WorkerID, SessionId: assignment.SessionID, Attempt: int32(assignment.Attempt)}
}

var _ v1.OrbitControllerServer = (*Server)(nil)
