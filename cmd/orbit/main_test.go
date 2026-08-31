package main

import (
	"strings"
	"testing"

	"github.com/MuhammadMaazA/Orbit/internal/replay"
	"github.com/MuhammadMaazA/Orbit/internal/scheduler"
)

func TestReplayPolicyKnownNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		want scheduler.Policy
	}{
		{"first-fit", scheduler.FirstFit{}},
		{"best-fit", scheduler.BestFit{}},
		{"bin-pack", scheduler.BinPack{}},
		{"energy", scheduler.EnergyAware{}},
	} {
		policy, err := replayPolicy(tc.name)
		if err != nil {
			t.Fatalf("replayPolicy(%q) error = %v", tc.name, err)
		}
		if policy.Name() != tc.want.Name() {
			t.Fatalf("replayPolicy(%q) = %q, want %q", tc.name, policy.Name(), tc.want.Name())
		}
	}
}

func TestReplayPolicyRejectsUnknownName(t *testing.T) {
	if _, err := replayPolicy("does-not-exist"); err == nil {
		t.Fatal("replayPolicy() accepted an unknown policy name")
	}
}

func TestExplainSelectionMatchesEachPolicy(t *testing.T) {
	activeEnergy := replay.Decision{Selected: "worker-a", Candidates: []replay.Candidate{{WorkerID: "worker-a", Feasible: true, Active: true}}}
	fallbackEnergy := replay.Decision{Selected: "worker-a", Candidates: []replay.Candidate{{WorkerID: "worker-a", Feasible: true, Active: false}}}
	for _, tc := range []struct {
		name     string
		policy   string
		decision replay.Decision
		contains string
	}{
		{"best-fit", "best-fit", replay.Decision{}, "headroom"},
		{"bin-pack", "bin-pack", replay.Decision{}, "closest to full"},
		{"energy active", "energy", activeEnergy, "already active"},
		{"energy fallback", "energy", fallbackEnergy, "fell back"},
		{"unknown policy", "made-up", replay.Decision{}, "selection criteria"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := explainSelection(tc.policy, tc.decision)
			if got == "" {
				t.Fatal("explainSelection() returned an empty string")
			}
			if !strings.Contains(got, tc.contains) {
				t.Fatalf("explainSelection(%q) = %q, want it to contain %q", tc.policy, got, tc.contains)
			}
		})
	}
}
