package model

type Job struct {
	ID       string
	CPU      int
	MemoryMB int
	GPU      int
	Priority int
}

func (j Job) Resources() ResourceRequest {
	return ResourceRequest{CPU: j.CPU, MemoryMB: j.MemoryMB, GPU: j.GPU}
}
