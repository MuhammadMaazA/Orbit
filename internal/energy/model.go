package energy

import "github.com/MuhammadMaazA/Orbit/internal/model"

type Config struct {
	IdleWatts float64
	CPUWatts  float64
	GPUWatts  float64
}

type Snapshot struct {
	ActiveWorkers    int
	Joules           float64
	ActiveWorkerTime float64
}

type worker struct {
	capacity model.Capacity
	last     float64
}

type Model struct {
	config           Config
	workers          map[string]worker
	joules           float64
	activeWorkerTime float64
}

func New(config Config) *Model {
	return &Model{config: config, workers: make(map[string]worker)}
}

func (m *Model) Register(id string, capacity model.Capacity, at float64) {
	m.workers[id] = worker{capacity: capacity, last: at}
}

func (m *Model) Observe(id string, capacity model.Capacity, at float64) {
	state, ok := m.workers[id]
	if !ok {
		m.Register(id, capacity, at)
		return
	}
	m.account(state, at)
	state.capacity = capacity
	state.last = at
	m.workers[id] = state
}

func (m *Model) Remove(id string, at float64) {
	state, ok := m.workers[id]
	if !ok {
		return
	}
	m.account(state, at)
	delete(m.workers, id)
}

func (m *Model) Snapshot(at float64) Snapshot {
	active := 0
	for id, state := range m.workers {
		m.account(state, at)
		state.last = at
		m.workers[id] = state
		if isActive(state.capacity) {
			active++
		}
	}
	return Snapshot{ActiveWorkers: active, Joules: m.joules, ActiveWorkerTime: m.activeWorkerTime}
}

func (m *Model) account(state worker, at float64) {
	if at <= state.last {
		return
	}
	seconds := at - state.last
	m.joules += power(m.config, state.capacity) * seconds
	if isActive(state.capacity) {
		m.activeWorkerTime += seconds
	}
}

func power(config Config, capacity model.Capacity) float64 {
	usedCPU := capacity.Total.CPU - capacity.Available.CPU
	usedGPU := capacity.Total.GPU - capacity.Available.GPU
	return config.IdleWatts + float64(usedCPU)*config.CPUWatts + float64(usedGPU)*config.GPUWatts
}

func isActive(capacity model.Capacity) bool {
	return capacity.Available.CPU < capacity.Total.CPU || capacity.Available.MemoryMB < capacity.Total.MemoryMB || capacity.Available.GPU < capacity.Total.GPU
}
