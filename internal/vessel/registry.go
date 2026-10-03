package vessel

import (
	"errors"
	"sort"
	"sync"

	"github.com/wyw14/cry-98/internal/model"
)

type Registry struct {
	mu      sync.RWMutex
	vessels map[string]model.Vessel
}

func NewRegistry() *Registry {
	return &Registry{vessels: make(map[string]model.Vessel)}
}

func (r *Registry) Register(v model.Vessel) error {
	if err := v.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.vessels[v.ID]; exists {
		return errors.New("vessel already registered")
	}
	r.vessels[v.ID] = v
	return nil
}

func (r *Registry) Get(id string) (model.Vessel, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.vessels[id]
	return v, ok
}

func (r *Registry) List() []model.Vessel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]model.Vessel, 0, len(r.vessels))
	for _, v := range r.vessels {
		result = append(result, v)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (r *Registry) Update(id string, apply func(model.Vessel) (model.Vessel, error)) (model.Vessel, error) {
	next, _, err := r.UpdateResult(id, apply)
	return next, err
}

// UpdateResult applies an update under the registry lock and reports whether the
// callback actually produced a new state. apply must return the current vessel
// unchanged (with a nil error) to decline the update without failing it.
func (r *Registry) UpdateResult(id string, apply func(model.Vessel) (model.Vessel, error)) (model.Vessel, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.vessels[id]
	if !ok {
		return model.Vessel{}, false, errors.New("vessel not found")
	}
	next, err := apply(current)
	if err != nil {
		return model.Vessel{}, false, err
	}
	if next.ID != current.ID || next.Generation < current.Generation {
		return model.Vessel{}, false, errors.New("vessel identity or generation changed illegally")
	}
	applied := next != current
	if applied {
		if err := next.Validate(); err != nil {
			return model.Vessel{}, false, err
		}
		r.vessels[id] = next
	}
	return next, applied, nil
}
