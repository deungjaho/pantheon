package domain

import "testing"

// TestValidRunTransitionV2 verifies the control-plane §8.1 state machine.
// This is the canonical Run state machine for P0 Runtime Closure. Legacy
// RunState (pending/preparing/...) is preserved as a compatibility facade
// (see acceptance-contract G3-BC).
func TestValidRunTransitionV2(t *testing.T) {
	cases := []struct {
		from, to RunStateV2
		want     bool
	}{
		// requested → planning | blocked | canceled | failed
		{RunV2Requested, RunV2Planning, true},
		{RunV2Requested, RunV2Blocked, true},
		{RunV2Requested, RunV2Canceled, true},
		{RunV2Requested, RunV2Failed, true},
		{RunV2Requested, RunV2Running, false},
		{RunV2Requested, RunV2Completed, false},

		// planning → ready | blocked | canceled | failed
		{RunV2Planning, RunV2Ready, true},
		{RunV2Planning, RunV2Blocked, true},
		{RunV2Planning, RunV2Canceled, true},
		{RunV2Planning, RunV2Failed, true},
		{RunV2Planning, RunV2Running, false},

		// ready → running | planning (loop) | blocked | canceled | failed
		{RunV2Ready, RunV2Running, true},
		{RunV2Ready, RunV2Planning, true},
		{RunV2Ready, RunV2Blocked, true},
		{RunV2Ready, RunV2Canceled, true},
		{RunV2Ready, RunV2Failed, true},
		{RunV2Ready, RunV2Verifying, false},

		// running → verifying | planning (loop) | blocked | canceled | failed
		{RunV2Running, RunV2Verifying, true},
		{RunV2Running, RunV2Planning, true},
		{RunV2Running, RunV2Blocked, true},
		{RunV2Running, RunV2Canceled, true},
		{RunV2Running, RunV2Failed, true},
		{RunV2Running, RunV2Completed, false},

		// verifying → completed | running (reject) | planning (loop) | failed
		{RunV2Verifying, RunV2Completed, true},
		{RunV2Verifying, RunV2Running, true},
		{RunV2Verifying, RunV2Planning, true},
		{RunV2Verifying, RunV2Failed, true},
		{RunV2Verifying, RunV2Blocked, false},
		{RunV2Verifying, RunV2Canceled, false},

		// completed: terminal
		{RunV2Completed, RunV2Running, false},
		{RunV2Completed, RunV2Planning, false},

		// blocked → running (resume) | canceled | failed
		{RunV2Blocked, RunV2Running, true},
		{RunV2Blocked, RunV2Canceled, true},
		{RunV2Blocked, RunV2Failed, true},
		{RunV2Blocked, RunV2Planning, false},

		// failed / canceled: terminal
		{RunV2Failed, RunV2Running, false},
		{RunV2Canceled, RunV2Running, false},
	}
	for _, c := range cases {
		got := ValidRunTransitionV2(c.from, c.to)
		if got != c.want {
			t.Errorf("RunV2 %s -> %s = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestCheckRunTransitionV2Error(t *testing.T) {
	if err := CheckRunTransitionV2(RunV2Completed, RunV2Running); err == nil {
		t.Fatal("expected error for completed -> running")
	}
	if err := CheckRunTransitionV2(RunV2Requested, RunV2Planning); err != nil {
		t.Fatalf("unexpected error for requested -> planning: %v", err)
	}
}

// TestRunContractV2Fields verifies the §8.2 Run contract fields exist and
// carry the lease/evidence structure (control-plane §8.2).
func TestRunContractV2Fields(t *testing.T) {
	r := RunContractV2{
		RunID:     "run_test",
		ProjectID: "prj_test",
		Owner:     "agt_pm",
		Workspace: "/tmp/wt",
		State:     RunV2Requested,
		Epoch:     1,
		Lease: RunLease{
			Holder:        "agt_worker",
			RenewDeadline: 0,
		},
		LastEvent:  "evt_1",
		Checkpoint: "",
		Evidence:   []string{},
	}
	if r.RunID != "run_test" {
		t.Fatalf("run_id = %q", r.RunID)
	}
	if r.Lease.Holder != "agt_worker" {
		t.Fatalf("lease.holder = %q", r.Lease.Holder)
	}
	if len(r.Evidence) != 0 {
		t.Fatalf("evidence len = %d, want 0", len(r.Evidence))
	}
}
