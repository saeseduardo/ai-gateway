// Package router maps a chat completion request's model name to the
// provider.Provider responsible for serving it.
package router

import (
	"errors"
	"fmt"

	"github.com/saeseduardo/ai-gateway/internal/provider"
)

// ErrModelNotFound is returned (wrapped) by Resolve when no provider
// has been registered for the requested model.
var ErrModelNotFound = errors.New("router: model not found")

// Router maps model names to the provider.Provider that serves them.
//
// The mapping is currently populated by hand at startup (see
// cmd/gateway/main.go's list of gpt-*/claude-* models); a later
// module will move it into config so models can be added or removed
// without a rebuild.
type Router struct {
	providers map[string]provider.Provider
}

// New returns an empty Router. Populate it with Register before use.
func New() *Router {
	return &Router{providers: make(map[string]provider.Provider)}
}

// Register maps model to p, overwriting any existing mapping for that
// model name.
func (r *Router) Register(model string, p provider.Provider) {
	r.providers[model] = p
}

// Resolve returns the provider.Provider registered for model. If no
// provider has been registered for it, it returns an error wrapping
// ErrModelNotFound.
func (r *Router) Resolve(model string) (provider.Provider, error) {
	p, ok := r.providers[model]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrModelNotFound, model)
	}
	return p, nil
}
