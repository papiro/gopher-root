package types

// CouplingID uniquely identifies one coupling transform.
type CouplingID string

// CouplingDescriptor declares static coupling identity and optional segment compatibility.
type CouplingDescriptor struct {
	// ID must be unique within one engine configuration.
	ID CouplingID
	// FromSegment, when non-empty, constrains which upstream segment this coupling can attach to.
	FromSegment SegmentID
	// ToSegment, when non-empty, constrains which downstream segment this coupling can attach to.
	ToSegment SegmentID
}

// Connection links one upstream segment to one downstream segment through one coupling.
type Connection struct {
	// From is the upstream segment.
	From SegmentID
	// To is the downstream segment.
	To SegmentID
	// CouplingID identifies the coupling used on this connection.
	CouplingID CouplingID
}

// Topology defines engine-owned segment ordering and coupling assignment.
type Topology struct {
	// Connections encodes all directed segment-to-segment links.
	Connections []Connection
	// AllowAmbiguousOrder controls whether one segment may branch to multiple downstream segments.
	AllowAmbiguousOrder bool
}

// EngineConfig contains metadata needed to validate an engine-owned topology before runtime.
type EngineConfig struct {
	// Segments are the known segment descriptors available to the engine.
	Segments []SegmentDescriptor
	// Couplings are the known coupling descriptors available to the engine.
	Couplings []CouplingDescriptor
	// Topology is the engine-owned ordering and connection map.
	Topology Topology
}
