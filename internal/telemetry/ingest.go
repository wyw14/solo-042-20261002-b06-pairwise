package telemetry

import (
	"errors"
	"sync"
	"time"

	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/probe"
	"github.com/wyw14/cry-98/internal/vessel"
)

type Ingestor struct {
	probes    *probe.Registry
	vessels   *vessel.StateService
	snapshots *SnapshotStore
	mu        sync.RWMutex
	accepted  uint64
	dropped   uint64
	observers []func(model.Sample)
}

func (i *Ingestor) AddObserver(observer func(model.Sample)) error {
	if observer == nil {
		return errors.New("telemetry observer is required")
	}
	i.mu.Lock()
	i.observers = append(i.observers, observer)
	i.mu.Unlock()
	return nil
}

func NewIngestor(probes *probe.Registry, vessels *vessel.StateService, snapshots *SnapshotStore) *Ingestor {
	return &Ingestor{probes: probes, vessels: vessels, snapshots: snapshots}
}

func (i *Ingestor) Ingest(sample model.Sample) error {
	probeState, ok := i.probes.Get(sample.ProbeID)
	if !ok {
		return errors.New("sample probe is not registered")
	}
	if !probeState.Connected || sample.CalibrationEpoch != probeState.CalibrationEpoch || sample.BindingGeneration != probeState.BindingGeneration {
		i.recordDrop()
		return errors.New("sample belongs to an obsolete probe generation")
	}
	if !sample.Kind.Valid() {
		i.recordDrop()
		return errors.New("unsupported sample kind")
	}
	if _, ok := i.vessels.Registry().Get(probeState.VesselID); !ok {
		return errors.New("sample vessel is not registered")
	}
	updated, applied, err := i.vessels.ApplyTelemetry(probeState.VesselID, sample.Kind, sample.Value, sample.ObservedAt)
	if err != nil {
		i.recordDrop()
		return err
	}
	if !applied {
		// Out-of-order sample declined by the vessel state: it must not reach
		// snapshots or any downstream observer (fill, pressure, health).
		i.recordDrop()
		return errors.New("telemetry sample was superseded")
	}
	i.snapshots.Apply(updated)
	i.mu.Lock()
	i.accepted++
	observers := append([]func(model.Sample){}, i.observers...)
	i.mu.Unlock()
	for _, observer := range observers {
		observer(sample)
	}
	return nil
}

func (i *Ingestor) recordDrop() {
	i.mu.Lock()
	i.dropped++
	i.mu.Unlock()
}

func (i *Ingestor) Stats() (accepted, dropped uint64) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.accepted, i.dropped
}

func (i *Ingestor) Snapshot() Snapshot {
	return i.snapshots.StableSnapshot()
}

func (i *Ingestor) LastUpdated() time.Time {
	return i.snapshots.StableSnapshot().UpdatedAt
}
