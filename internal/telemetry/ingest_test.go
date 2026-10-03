package telemetry_test

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/monitor"
	"github.com/wyw14/cry-98/internal/pressure"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/telemetry"
	"github.com/wyw14/cry-98/internal/vessel"
)

type ingestFixture struct {
	now      time.Time
	vessels  *vessel.Registry
	probes   *probe.Registry
	ingestor *telemetry.Ingestor
}

func newIngestFixture(t *testing.T, kind model.ProbeKind) ingestFixture {
	t.Helper()
	clock := time.Unix(1_000_000, 0).UTC()
	now := func() time.Time { return clock }
	vessels := vessel.NewRegistry()
	vesselState, err := model.NewVessel("V-01", "Vessel", "north", clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := vessels.Register(vesselState); err != nil {
		t.Fatal(err)
	}
	probes := probe.NewRegistry(4)
	probeState, err := model.NewProbe("P-01", kind, "V-01", clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := probes.Register(probeState); err != nil {
		t.Fatal(err)
	}
	ingestor := telemetry.NewIngestor(probes, vessel.NewStateService(vessels, now), telemetry.NewSnapshotStore())
	return ingestFixture{now: clock, vessels: vessels, probes: probes, ingestor: ingestor}
}

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
	ingestor := telemetry.NewIngestor(probes, vessel.NewStateService(vessels, now), telemetry.NewSnapshotStore())
	stale := probeState.NewSample(50, now())
	if _, err := probe.NewLifecycle(probes, now).Rebind("P-01", "V-02"); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Ingest(stale); err == nil {
		t.Fatal("obsolete binding sample was accepted")
	}
}

func TestIngestStaleSampleDoesNotReachObservers(t *testing.T) {
	fixture := newIngestFixture(t, model.ProbeLevel)
	probeState, _ := fixture.probes.Get("P-01")

	notified := 0
	if err := fixture.ingestor.AddObserver(func(model.Sample) { notified++ }); err != nil {
		t.Fatal(err)
	}

	// Latest level is 60, observed now.
	current := fixture.now.Add(5 * time.Minute)
	if err := fixture.ingestor.Ingest(probeState.NewSample(60, current)); err != nil {
		t.Fatal(err)
	}

	// A sample observed two minutes in the past (relative to the 60 reading)
	// arrives late and must be refused entirely.
	stale := probeState.NewSample(10, current.Add(-2*time.Minute))
	if err := fixture.ingestor.Ingest(stale); err == nil {
		t.Fatal("stale sample was accepted")
	}

	state, _ := fixture.vessels.Get("V-01")
	if state.LevelMM != 60 {
		t.Fatalf("vessel level changed to %v, want 60", state.LevelMM)
	}
	if snapshot, ok := fixture.ingestor.Snapshot().Vessels["V-01"]; !ok || snapshot.LevelMM != 60 {
		t.Fatalf("snapshot level changed, want 60")
	}
	if notified != 1 {
		t.Fatalf("observer invoked %d times, want 1 (valid sample only)", notified)
	}
	accepted, dropped := fixture.ingestor.Stats()
	if accepted != 1 || dropped != 1 {
		t.Fatalf("stats accepted=%d dropped=%d, want 1/1", accepted, dropped)
	}
}

func TestIngestOutOfOrderSampleDoesNotReachObservers(t *testing.T) {
	fixture := newIngestFixture(t, model.ProbeLevel)
	probeState, _ := fixture.probes.Get("P-01")

	notified := 0
	if err := fixture.ingestor.AddObserver(func(model.Sample) { notified++ }); err != nil {
		t.Fatal(err)
	}

	current := fixture.now.Add(5 * time.Minute)
	if err := fixture.ingestor.Ingest(probeState.NewSample(60, current)); err != nil {
		t.Fatal(err)
	}
	// Within the staleness window, but still older than the applied state.
	late := probeState.NewSample(10, current.Add(-30*time.Second))
	if err := fixture.ingestor.Ingest(late); err == nil {
		t.Fatal("out-of-order sample was accepted")
	}

	state, _ := fixture.vessels.Get("V-01")
	if state.LevelMM != 60 {
		t.Fatalf("vessel level changed to %v, want 60", state.LevelMM)
	}
	if notified != 1 {
		t.Fatalf("observer invoked %d times, want 1 (valid sample only)", notified)
	}
}

func TestIngestFaultedVesselRejectsSample(t *testing.T) {
	fixture := newIngestFixture(t, model.ProbeLevel)
	probeState, _ := fixture.probes.Get("P-01")

	notified := 0
	if err := fixture.ingestor.AddObserver(func(model.Sample) { notified++ }); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.vessels.Update("V-01", func(current model.Vessel) (model.Vessel, error) {
		return current.WithStatus(model.VesselFault, fixture.now.Add(time.Minute)), nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.ingestor.Ingest(probeState.NewSample(10, fixture.now.Add(2*time.Minute))); !errors.Is(err, vessel.ErrTelemetryRejected) {
		t.Fatalf("faulted vessel accepted sample with err=%v", err)
	}
	if notified != 0 {
		t.Fatalf("observer invoked %d times for faulted vessel, want 0", notified)
	}
}

func TestIngestValidSamplePropagatesToAllObservers(t *testing.T) {
	fixture := newIngestFixture(t, model.ProbeLevel)
	probeState, _ := fixture.probes.Get("P-01")

	var seen model.Sample
	if err := fixture.ingestor.AddObserver(func(sample model.Sample) { seen = sample }); err != nil {
		t.Fatal(err)
	}
	at := fixture.now.Add(time.Minute)
	if err := fixture.ingestor.Ingest(probeState.NewSample(42, at)); err != nil {
		t.Fatal(err)
	}
	if seen.ProbeID != "P-01" || seen.Value != 42 || !seen.ObservedAt.Equal(at) {
		t.Fatalf("observer received %+v, want value 42 at %v", seen, at)
	}
	state, _ := fixture.vessels.Get("V-01")
	if state.LevelMM != 42 {
		t.Fatalf("vessel level = %v, want 42", state.LevelMM)
	}
}

func TestIngestRejectedSampleDoesNotTriggerFillOrHealth(t *testing.T) {
	fixture := newIngestFixture(t, model.ProbeLevel)
	clock := fixture.now
	probeState, _ := fixture.probes.Get("P-01")

	// Mirrors the runtime wiring: the low-level observer starts an automatic
	// fill and the probe monitor advances the health timestamp.
	lowLevelTriggers := 0
	if err := fixture.ingestor.AddObserver(func(sample model.Sample) {
		if sample.Kind == model.ProbeLevel && sample.Value < 20 {
			lowLevelTriggers++
		}
	}); err != nil {
		t.Fatal(err)
	}
	probeMonitor := monitor.NewProbeMonitor(fixture.probes, time.Minute, func() time.Time { return clock })
	if err := fixture.ingestor.AddObserver(probeMonitor.Observe); err != nil {
		t.Fatal(err)
	}

	// A fresh healthy reading first.
	if err := fixture.ingestor.Ingest(probeState.NewSample(60, clock.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	// Late low-level reading that the vessel refuses: must not start a fill or
	// advance the health clock.
	if err := fixture.ingestor.Ingest(probeState.NewSample(10, clock.Add(-time.Minute))); err == nil {
		t.Fatal("stale low-level sample was accepted")
	}
	if lowLevelTriggers != 0 {
		t.Fatalf("automatic fill triggered %d times by stale sample", lowLevelTriggers)
	}
	health, _ := probeMonitor.Health("P-01")
	if !health.LastSample.Equal(clock.Add(time.Minute)) {
		t.Fatalf("health clock advanced to %v from stale sample", health.LastSample)
	}

	// A current low-level reading still propagates and triggers the fill path.
	if err := fixture.ingestor.Ingest(probeState.NewSample(10, clock.Add(2*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if lowLevelTriggers != 1 {
		t.Fatalf("valid low-level sample triggered fill %d times, want 1", lowLevelTriggers)
	}
}

func TestIngestRejectedPressureSampleStaysOutOfWindow(t *testing.T) {
	fixture := newIngestFixture(t, model.ProbePressure)
	probeState, _ := fixture.probes.Get("P-01")

	pressureMonitor := pressure.NewMonitor(func() time.Time { return fixture.now })
	if err := pressureMonitor.AddVessel("V-01", 5, 1.5); err != nil {
		t.Fatal(err)
	}
	if err := fixture.ingestor.AddObserver(func(sample model.Sample) {
		if sample.Kind == model.ProbePressure {
			_ = pressureMonitor.Add(sample)
		}
	}); err != nil {
		t.Fatal(err)
	}

	// Valid baseline, then a stale sample that must be ignored.
	if err := fixture.ingestor.Ingest(probeState.NewSample(100, fixture.now.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := fixture.ingestor.Ingest(probeState.NewSample(50, fixture.now.Add(-time.Minute))); err == nil {
		t.Fatal("stale pressure sample was accepted")
	}
	result, err := pressureMonitor.Evaluate("V-01")
	if err != nil {
		t.Fatal(err)
	}
	if result.Samples != 1 {
		t.Fatalf("pressure window holds %d samples, want 1", result.Samples)
	}

	if err := fixture.ingestor.Ingest(probeState.NewSample(101, fixture.now.Add(2*time.Minute))); err != nil {
		t.Fatal(err)
	}
	result, _ = pressureMonitor.Evaluate("V-01")
	if result.Samples != 2 {
		t.Fatalf("pressure window holds %d samples, want 2", result.Samples)
	}
}
