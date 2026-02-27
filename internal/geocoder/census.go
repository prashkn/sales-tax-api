package geocoder

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const censusBaseURL = "https://geocoding.geo.census.gov/geocoder/geographies/address"

// Result holds the resolved FIPS codes from a geocoded address.
type Result struct {
	StateFIPS  string // 2-digit state FIPS, e.g. "06"
	CountyFIPS string // 5-digit state+county FIPS, e.g. "06037"
	PlaceFIPS  string // 7-digit state+place FIPS, e.g. "0644000" (empty if unincorporated)
}

// Client calls the US Census Bureau Geocoder API to resolve
// street addresses into FIPS jurisdiction codes.
type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Geocode resolves a street address to FIPS codes using the Census Geocoder.
// Returns nil (no error) if the address could not be matched.
func (c *Client) Geocode(ctx context.Context, street, city, state, zip string) (*Result, error) {
	params := url.Values{
		"street":    {street},
		"city":      {city},
		"state":     {state},
		"zip":       {zip},
		"benchmark": {"Public_AR_Current"},
		"vintage":   {"Current_Current"},
		"format":    {"json"},
	}

	reqURL := censusBaseURL + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("census geocoder request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("census geocoder returned status %d", resp.StatusCode)
	}

	var body censusResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decoding census response: %w", err)
	}

	return parseResponse(body)
}

// parseResponse extracts FIPS codes from the Census Geocoder JSON response.
// Returns nil if no address match was found.
func parseResponse(body censusResponse) (*Result, error) {
	matches := body.Result.AddressMatches
	if len(matches) == 0 {
		return nil, nil
	}

	geo := matches[0].Geographies
	result := &Result{}

	// State FIPS from Census Blocks (most reliable — every matched address has one).
	if len(geo.CensusBlocks) > 0 {
		block := geo.CensusBlocks[0]
		result.StateFIPS = block.State
		if block.State != "" && block.County != "" {
			result.CountyFIPS = block.State + block.County
		}
	}

	// Fall back to States/Counties arrays if blocks didn't have what we need.
	if result.StateFIPS == "" && len(geo.States) > 0 {
		result.StateFIPS = geo.States[0].GEOID
	}
	if result.CountyFIPS == "" && len(geo.Counties) > 0 {
		result.CountyFIPS = geo.Counties[0].GEOID
	}

	// Place (city) FIPS — only present for incorporated areas.
	if len(geo.IncorporatedPlaces) > 0 {
		result.PlaceFIPS = geo.IncorporatedPlaces[0].GEOID
	}

	if result.StateFIPS == "" {
		return nil, nil
	}

	return result, nil
}

// Census Geocoder API response types.

type censusResponse struct {
	Result struct {
		AddressMatches []addressMatch `json:"addressMatches"`
	} `json:"result"`
}

type addressMatch struct {
	MatchedAddress string      `json:"matchedAddress"`
	Coordinates    coordinates `json:"coordinates"`
	Geographies    geographies `json:"geographies"`
}

type coordinates struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type geographies struct {
	CensusBlocks       []censusBlock  `json:"Census Blocks"`
	States             []geoEntity    `json:"States"`
	Counties           []geoEntity    `json:"Counties"`
	IncorporatedPlaces []geoEntity    `json:"Incorporated Places"`
}

type censusBlock struct {
	State  string `json:"STATE"`
	County string `json:"COUNTY"`
	Tract  string `json:"TRACT"`
	Block  string `json:"BLOCK"`
	GEOID  string `json:"GEOID"`
}

type geoEntity struct {
	GEOID string `json:"GEOID"`
	Name  string `json:"NAME"`
}
