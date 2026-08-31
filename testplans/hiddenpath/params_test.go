package main

import (
	"testing"

	sphinx "github.com/ipfs/boxo/bitswap/network/sphinx"
)

func TestParseDrawPolicy(t *testing.T) {
	if got, err := parseDrawPolicy("exclusive"); err != nil || got != sphinx.DrawExclusive {
		t.Errorf("parseDrawPolicy(exclusive) = %v, %v; want DrawExclusive", got, err)
	}
	if got, err := parseDrawPolicy("independent"); err != nil || got != sphinx.DrawIndependent {
		t.Errorf("parseDrawPolicy(independent) = %v, %v; want DrawIndependent", got, err)
	}
	if got, err := parseDrawPolicy("per-attempt"); err != nil || got != sphinx.DrawPerAttempt {
		t.Errorf("parseDrawPolicy(per-attempt) = %v, %v; want DrawPerAttempt", got, err)
	}
	if _, err := parseDrawPolicy("uniform"); err == nil {
		t.Error("parseDrawPolicy accepted an unknown policy name")
	}
}

// only the independent draw conflicts with the edge bias; the per-attempt
// draw keeps the within-attempt sample and stays compatible
func TestPerAttemptDrawAllowsEdgeBias(t *testing.T) {
	if err := validateDrawEpsilon(sphinx.DrawPerAttempt, 0.5); err != nil {
		t.Errorf("validateDrawEpsilon rejected the per-attempt draw with a bias: %v", err)
	}
}

func TestValidateDrawEpsilon(t *testing.T) {
	if err := validateDrawEpsilon(sphinx.DrawIndependent, 0); err == nil {
		t.Error("validateDrawEpsilon accepted the independent draw at the edge_epsilon = 0 boundary")
	}
	if err := validateDrawEpsilon(sphinx.DrawIndependent, 0.5); err == nil {
		t.Error("validateDrawEpsilon accepted the independent draw with an enabled bias")
	}
	if err := validateDrawEpsilon(sphinx.DrawIndependent, -1); err != nil {
		t.Errorf("validateDrawEpsilon rejected the disabled bias: %v", err)
	}
	if err := validateDrawEpsilon(sphinx.DrawExclusive, 0.5); err != nil {
		t.Errorf("validateDrawEpsilon rejected the exclusive draw with a bias: %v", err)
	}
}
