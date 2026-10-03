package vessel

import (
	"errors"
	"time"

	"github.com/wyw14/cry-98/internal/model"
)

// ErrStaleTelemetry is returned when a sample was observed more than a minute
// before the vessel's latest state and must never reach downstream observers.
var ErrStaleTelemetry = errors.New("telemetry sample is stale")

// ErrTelemetryRejected is returned when the vessel's current status refuses new
// telemetry (for example while faulted or offline).
var ErrTelemetryRejected = errors.New("vessel is not accepting telemetry")

type StateService struct {
	registry *Registry
	now      func() time.Time
}

func NewStateService(registry *Registry, now func() time.Time) *StateService {
	if now == nil {
		now = time.Now
	}
	return &StateService{registry: registry, now: now}
}

func (s *StateService) Registry() *Registry {
	return s.registry
}

func (s *StateService) BeginFill(id string) (model.Vessel, error) {
	return s.registry.Update(id, func(current model.Vessel) (model.Vessel, error) {
		if current.Status == model.VesselFilling {
			return current, nil
		}
		if current.Status != model.VesselReady {
			return model.Vessel{}, errors.New("vessel is not ready for filling")
		}
		return current.WithStatus(model.VesselFilling, s.now()), nil
	})
}

func (s *StateService) FinishFill(id string) (model.Vessel, error) {
	return s.registry.Update(id, func(current model.Vessel) (model.Vessel, error) {
		if current.Status != model.VesselFilling {
			return model.Vessel{}, errors.New("vessel has no active fill")
		}
		return current.WithStatus(model.VesselReady, s.now()), nil
	})
}

// ApplyTelemetry records a probe sample against a vessel. The boolean result is
// true only when the vessel state actually adopted the sample. Samples that are
// stale (observed more than a minute behind the latest state), out of order
// (older than the latest applied telemetry), or refused by the vessel's current
// status are not applied and must not be propagated to downstream observers.
// The new reading is merged with the other dimensions of the current state
// while the registry lock is held so concurrent probes cannot overwrite each
// other.
func (s *StateService) ApplyTelemetry(id string, kind model.ProbeKind, value float64, observedAt time.Time) (model.Vessel, bool, error) {
	return s.registry.UpdateResult(id, func(current model.Vessel) (model.Vessel, error) {
		switch current.Status {
		case model.VesselReady, model.VesselFilling:
		default:
			return model.Vessel{}, ErrTelemetryRejected
		}
		if observedAt.Before(current.UpdatedAt.Add(-time.Minute)) {
			return model.Vessel{}, ErrStaleTelemetry
		}
		if !observedAt.After(current.UpdatedAt) {
			// Superseded by telemetry that has already been applied.
			return current, nil
		}
		level, pressure, temperature := current.LevelMM, current.PressureKPa, current.TemperatureC
		switch kind {
		case model.ProbeLevel:
			level = value
		case model.ProbePressure:
			pressure = value
		case model.ProbeTemperature:
			temperature = value
		default:
			return model.Vessel{}, errors.New("unsupported sample kind")
		}
		return current.WithTelemetry(level, pressure, temperature, observedAt), nil
	})
}
