package model

type Job struct {
	ID       string
	CPU      int
	MemoryMB int
	GPU      int
}

func (j Job) Resources() Resources {
	return Resources{CPU: j.CPU, MemoryMB: j.MemoryMB, GPU: j.GPU}
}
