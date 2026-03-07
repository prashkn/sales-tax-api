package geocoder

import (
	"testing"
)

func TestParseResponse_FullMatch(t *testing.T) {
	resp := censusResponse{}
	resp.Result.AddressMatches = []addressMatch{
		{
			MatchedAddress: "123 MAIN ST, BEVERLY HILLS, CA, 90210",
			Coordinates:    coordinates{X: -118.4, Y: 34.07},
			Geographies: geographies{
				CensusBlocks: []censusBlock{
					{State: "06", County: "037", Tract: "701002", Block: "2014", GEOID: "060370701002014"},
				},
				States:             []geoEntity{{GEOID: "06", Name: "California"}},
				Counties:           []geoEntity{{GEOID: "06037", Name: "Los Angeles County"}},
				IncorporatedPlaces: []geoEntity{{GEOID: "0603744000", Name: "Beverly Hills"}},
			},
		},
	}

	result, err := parseResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.StateFIPS != "06" {
		t.Errorf("StateFIPS = %q, want %q", result.StateFIPS, "06")
	}
	if result.CountyFIPS != "06037" {
		t.Errorf("CountyFIPS = %q, want %q", result.CountyFIPS, "06037")
	}
	if result.PlaceFIPS != "0603744000" {
		t.Errorf("PlaceFIPS = %q, want %q", result.PlaceFIPS, "0603744000")
	}
}

func TestParseResponse_NoAddressMatches(t *testing.T) {
	resp := censusResponse{}
	resp.Result.AddressMatches = []addressMatch{}

	result, err := parseResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result for no matches, got %+v", result)
	}
}

func TestParseResponse_UnincorporatedArea(t *testing.T) {
	// Unincorporated areas have no IncorporatedPlaces entry.
	resp := censusResponse{}
	resp.Result.AddressMatches = []addressMatch{
		{
			MatchedAddress: "456 RURAL RD, SOMEWHERE, CA, 93001",
			Geographies: geographies{
				CensusBlocks: []censusBlock{
					{State: "06", County: "111", GEOID: "061110001001001"},
				},
				States:             []geoEntity{{GEOID: "06"}},
				Counties:           []geoEntity{{GEOID: "06111"}},
				IncorporatedPlaces: []geoEntity{}, // empty — unincorporated
			},
		},
	}

	result, err := parseResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.StateFIPS != "06" {
		t.Errorf("StateFIPS = %q, want %q", result.StateFIPS, "06")
	}
	if result.CountyFIPS != "06111" {
		t.Errorf("CountyFIPS = %q, want %q", result.CountyFIPS, "06111")
	}
	if result.PlaceFIPS != "" {
		t.Errorf("PlaceFIPS = %q, want empty for unincorporated area", result.PlaceFIPS)
	}
}

func TestParseResponse_FallbackToStatesCounties(t *testing.T) {
	// When CensusBlocks is empty, fall back to States/Counties arrays.
	resp := censusResponse{}
	resp.Result.AddressMatches = []addressMatch{
		{
			Geographies: geographies{
				CensusBlocks:       []censusBlock{},
				States:             []geoEntity{{GEOID: "36"}},
				Counties:           []geoEntity{{GEOID: "36061"}},
				IncorporatedPlaces: []geoEntity{{GEOID: "3651000"}},
			},
		},
	}

	result, err := parseResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.StateFIPS != "36" {
		t.Errorf("StateFIPS = %q, want %q", result.StateFIPS, "36")
	}
	if result.CountyFIPS != "36061" {
		t.Errorf("CountyFIPS = %q, want %q", result.CountyFIPS, "36061")
	}
	if result.PlaceFIPS != "3651000" {
		t.Errorf("PlaceFIPS = %q, want %q", result.PlaceFIPS, "3651000")
	}
}

func TestParseResponse_CountyFromBlockFields(t *testing.T) {
	// Verify county FIPS is built from State+County fields on the block.
	resp := censusResponse{}
	resp.Result.AddressMatches = []addressMatch{
		{
			Geographies: geographies{
				CensusBlocks: []censusBlock{
					{State: "17", County: "031"},
				},
				States:   []geoEntity{},
				Counties: []geoEntity{},
			},
		},
	}

	result, err := parseResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CountyFIPS != "17031" {
		t.Errorf("CountyFIPS = %q, want %q", result.CountyFIPS, "17031")
	}
}

func TestParseResponse_EmptyGeographies(t *testing.T) {
	// Match exists but geographies are all empty — should return nil.
	resp := censusResponse{}
	resp.Result.AddressMatches = []addressMatch{
		{
			MatchedAddress: "789 NOWHERE ST",
			Geographies:    geographies{},
		},
	}

	result, err := parseResponse(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for empty geographies, got %+v", result)
	}
}
