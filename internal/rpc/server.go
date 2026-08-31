package rpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/MuhammadMaazA/Orbit/internal/controller"
	"github.com/MuhammadMaazA/Orbit/internal/metrics"
	"github.com/MuhammadMaazA/Orbit/internal/model"
	v1 "github.com/MuhammadMaazA/Orbit/internal/rpc/orbitv1/orbit/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	v1.UnimplementedOrbitControllerServer
	controller       *controller.Controller
	metrics          *metrics.Metrics
	mu               sync.RWMutex
	metricsMu        sync.Mutex
	sessions         map[string]*workerSession
	reportedRequeued uint64
}

type workerSession struct {
	stream v1.OrbitController_WorkerSessionServer
	mu     sync.Mutex
}

func NewServer(controller *controller.Controller, instrumentation ...*metrics.Metrics) *Server {
	server := &Server{controller: controller, sessions: make(map[string]*workerSession)}
	if len(instrumentation) > 0 {
		server.metrics = instrumentation[0]
	}
	return server
}

func (s *Server) ExpireWorkers(now time.Time, timeout time.Duration) error {
	assignments, err := s.controller.ExpireWorkers(now, timeout)
	s.dispatch(assignments)
	if err != nil {
		return err
	}
	if s.metrics != nil {
		s.updateMetrics()
	}
	return nil
}

func (s *Server) Submit(_ context.Context, request *v1.Job) (*v1.Assignment, error) {
	if request == nil || request.Resources == nil {
		return nil, status.Error(codes.InvalidArgument, "job and resources are required")
	}
	assignments, err := s.controller.Submit(fromJob(request))
	s.dispatch(assignments)
	if err != nil {
		if errors.Is(err, controller.ErrQueueFull) && s.metrics != nil {
			s.metrics.JobsRejected.Inc()
		}
		if errors.Is(err, controller.ErrQueueFull) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if s.metrics != nil {
		s.metrics.JobsSubmitted.Inc()
		s.updateMetrics()
	}
	if len(assignments) == 0 {
		return &v1.Assignment{}, nil
	}
	return toAssignment(assignments[0]), nil
}

func (s *Server) GetJob(_ context.Context, request *v1.JobStatusRequest) (*v1.JobStatusResponse, error) {
	view, ok := s.controller.GetJob(request.GetJobId())
	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	response := toJobStatus(view)
	if view.Assignment != nil {
		response.WorkerId = view.Assignment.WorkerID
		response.AssignmentId = view.Assignment.ID
	}
	return response, nil
}

func (s *Server) ListJobs(_ context.Context, _ *v1.JobListRequest) (*v1.JobListResponse, error) {
	views := s.controller.ListJobs()
	response := &v1.JobListResponse{Jobs: make([]*v1.JobStatusResponse, 0, len(views))}
	for _, view := range views {
		response.Jobs = append(response.Jobs, toJobStatus(view))
	}
	return response, nil
}

func toJobStatus(view controller.JobView) *v1.JobStatusResponse {
	response := &v1.JobStatusResponse{Job: &v1.Job{Id: view.Job.ID, Resources: &v1.ResourceRequest{Cpu: int32(view.Job.CPU), MemoryMb: int32(view.Job.MemoryMB), Gpu: int32(view.Job.GPU)}, Priority: int32(view.Job.Priority)}, Status: string(view.Status), Attempt: int32(view.Attempts)}
	if view.Assignment != nil {
		response.WorkerId = view.Assignment.WorkerID
		response.AssignmentId = view.Assignment.ID
	}
	return response
}

func (s *Server) DrainWorker(_ context.Context, request *v1.WorkerStateRequest) (*v1.WorkerStateResponse, error) {
	if err := s.controller.DrainWorker(request.GetWorkerId()); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if s.metrics != nil {
		s.updateMetrics()
	}
	return &v1.WorkerStateResponse{Draining: true}, nil
}

func (s *Server) UndrainWorker(_ context.Context, request *v1.WorkerStateRequest) (*v1.WorkerStateResponse, error) {
	assignments, err := s.controller.UndrainWorker(request.GetWorkerId())
	s.dispatch(assignments)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if s.metrics != nil {
		s.updateMetrics()
	}
	return &v1.WorkerStateResponse{}, nil
}

func (s *Server) WorkerSession(stream v1.OrbitController_WorkerSessionServer) error {
	var workerID, sessionID string
	var session *workerSession
	defer func() {
		if workerID == "" || session == nil {
			return
		}
		s.removeSession(workerID, session)
		assignments, _ := s.controller.WorkerLost(workerID, sessionID)
		s.dispatch(assignments)
	}()
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		switch payload := message.Payload.(type) {
		case *v1.WorkerSessionMessage_Register:
			if payload.Register.Total == nil {
				return status.Error(codes.InvalidArgument, "worker capacity is required")
			}
			workerID, sessionID = payload.Register.Id, payload.Register.SessionId
			session = &workerSession{stream: stream}
			s.mu.Lock()
			s.sessions[workerID] = session
			s.mu.Unlock()
			assignments, err := s.controller.RegisterWorker(workerID, sessionID, fromCapacity(payload.Register.Total))
			s.dispatch(assignments)
			if err != nil {
				return status.Error(codes.InvalidArgument, err.Error())
			}
			if s.metrics != nil {
				s.metrics.WorkersRegistered.Inc()
				s.updateMetrics()
			}
		case *v1.WorkerSessionMessage_HeartbeatSessionId:
			if session == nil {
				return status.Error(codes.FailedPrecondition, "worker must register first")
			}
			if err := s.controller.Heartbeat(workerID, payload.HeartbeatSessionId, now()); err != nil {
				return status.Error(codes.PermissionDenied, err.Error())
			}
			if err := session.send(&v1.ControllerSessionMessage{Payload: &v1.ControllerSessionMessage_HeartbeatAckSessionId{HeartbeatAckSessionId: payload.HeartbeatSessionId}}); err != nil {
				return err
			}
		case *v1.WorkerSessionMessage_Completion:
			if payload.Completion.Assignment == nil {
				return status.Error(codes.InvalidArgument, "completion assignment is required")
			}
			assignments, accepted, err := s.controller.Complete(fromAssignment(payload.Completion.Assignment), payload.Completion.Success)
			s.dispatch(assignments)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			if accepted {
				if s.metrics != nil {
					if payload.Completion.Success {
						s.metrics.JobsCompleted.Inc()
					} else {
						s.metrics.JobsFailed.Inc()
					}
					s.updateMetrics()
				}
			} else if s.metrics != nil {
				s.metrics.StaleResultsRejected.Inc()
			}
		}
	}
}

func (s *Server) dispatch(assignments []controller.Assignment) {
	for _, assignment := range assignments {
		s.mu.RLock()
		session := s.sessions[assignment.WorkerID]
		s.mu.RUnlock()
		if session != nil {
			if s.metrics != nil {
				s.metrics.SchedulingAttempts.Inc()
			}
			_ = session.send(&v1.ControllerSessionMessage{Payload: &v1.ControllerSessionMessage_Assignment{Assignment: toAssignment(assignment)}})
		}
	}
}

func (session *workerSession) send(message *v1.ControllerSessionMessage) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.stream.Send(message)
}

func (s *Server) removeSession(workerID string, session *workerSession) {
	s.mu.Lock()
	if s.sessions[workerID] == session {
		delete(s.sessions, workerID)
	}
	s.mu.Unlock()
}

func (s *Server) updateMetrics() {
	stats := s.controller.Stats()
	s.metricsMu.Lock()
	if stats.Requeued > s.reportedRequeued {
		s.metrics.JobsRequeued.Add(float64(stats.Requeued - s.reportedRequeued))
		s.reportedRequeued = stats.Requeued
	}
	s.metricsMu.Unlock()
	s.metrics.SetGauges(stats.Workers, stats.Draining, stats.Queued, stats.Running)
	s.metrics.SetEnergy(stats.ActiveWorkers, stats.EnergyJoules)
}

var now = func() time.Time { return time.Now() }

func fromJob(job *v1.Job) model.Job {
	return model.Job{ID: job.Id, CPU: int(job.Resources.Cpu), MemoryMB: int(job.Resources.MemoryMb), GPU: int(job.Resources.Gpu), Priority: int(job.Priority)}
}

func fromCapacity(resources *v1.ResourceRequest) model.Capacity {
	total := model.ResourceRequest{CPU: int(resources.Cpu), MemoryMB: int(resources.MemoryMb), GPU: int(resources.Gpu)}
	return model.Capacity{Total: total, Available: total}
}

func fromAssignment(assignment *v1.Assignment) controller.Assignment {
	return controller.Assignment{ID: assignment.Id, Job: fromJob(assignment.Job), WorkerID: assignment.WorkerId, SessionID: assignment.SessionId, Attempt: int(assignment.Attempt)}
}

func toAssignment(assignment controller.Assignment) *v1.Assignment {
	return &v1.Assignment{Id: assignment.ID, Job: &v1.Job{Id: assignment.Job.ID, Resources: &v1.ResourceRequest{Cpu: int32(assignment.Job.CPU), MemoryMb: int32(assignment.Job.MemoryMB), Gpu: int32(assignment.Job.GPU)}, Priority: int32(assignment.Job.Priority)}, WorkerId: assignment.WorkerID, SessionId: assignment.SessionID, Attempt: int32(assignment.Attempt)}
}

var _ v1.OrbitControllerServer = (*Server)(nil)
