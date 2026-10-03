package telemetry

import (
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

func TestIngestRejectsObsoleteBindingGeneration(t *testing.T) {
	now := time.Now
	vessels := vessel.NewRegistry()
	vesselState, _ := model.NewVessel("V-01", "Vessel", "north", now())
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", now())
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	ingestor := NewIngestor(probes, vessel.NewStateService(vessels, now), NewSnapshotStore())
	stale := probeState.NewSample(50, now())
	if _, err := probe.NewLifecycle(probes, now).Rebind("P-01", "V-02"); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Ingest(stale); err == nil {
		t.Fatal("obsolete binding sample was accepted")
	}
}

func TestIngestStaleSampleDoesNotNotifyObservers(t *testing.T) {
	base := time.Now()
	now := func() time.Time { return base }
	vessels := vessel.NewRegistry()
	vesselState, _ := model.NewVessel("V-01", "Vessel", "north", base)
	vesselState = vesselState.WithTelemetry(60, 100, -180, base)
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, _ := model.NewProbe("P-01", model.ProbeLevel, "V-01", base)
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	snapshots := NewSnapshotStore()
	snapshots.Replace(vessels.List(), base)
	ingestor := NewIngestor(probes, vessel.NewStateService(vessels, now), snapshots)
	var notified []model.Sample
	if err := ingestor.AddObserver(func(sample model.Sample) {
		notified = append(notified, sample)
	}); err != nil {
		t.Fatal(err)
	}

	stale := probeState.NewSample(10, base.Add(-2*time.Minute))
	if err := ingestor.Ingest(stale); err == nil {
		t.Fatal("stale sample was accepted")
	}
	if len(notified) != 0 {
		t.Fatalf("stale sample reached %d observers", len(notified))
	}
	accepted, dropped := ingestor.Stats()
	if accepted != 0 || dropped != 1 {
		t.Fatalf("stale sample stats: accepted=%d dropped=%d", accepted, dropped)
	}
	current, ok := vessels.Get("V-01")
	if !ok {
		t.Fatal("vessel missing")
	}
	if current.LevelMM != 60 {
		t.Fatalf("stale sample changed vessel level to %v", current.LevelMM)
	}
	if got := ingestor.Snapshot().Vessels["V-01"].LevelMM; got != 60 {
		t.Fatalf("stale sample changed snapshot level to %v", got)
	}

	fresh := probeState.NewSample(10, base.Add(time.Second))
	if err := ingestor.Ingest(fresh); err != nil {
		t.Fatalf("fresh sample was rejected: %v", err)
	}
	if len(notified) != 1 || notified[0].Value != 10 {
		t.Fatalf("fresh sample did not propagate to observers: %v", notified)
	}
	accepted, dropped = ingestor.Stats()
	if accepted != 1 || dropped != 1 {
		t.Fatalf("fresh sample stats: accepted=%d dropped=%d", accepted, dropped)
	}
}
