package simulation

import (
	"math/rand"

	"orbit/internal/model"
)

func Generate(seed int64, count int) []Job {
	random := rand.New(rand.NewSource(seed))
	jobs := make([]Job, count)
	arrival := 0
	for i := range jobs {
		arrival += random.Intn(3)
		jobs[i] = Job{Spec: model.Job{ID: "job-" + itoa(i+1), CPU: 1 + random.Intn(4), MemoryMB: 512 + random.Intn(4)*512}, Arrival: arrival, Duration: 1 + random.Intn(5)}
	}
	return jobs
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}
