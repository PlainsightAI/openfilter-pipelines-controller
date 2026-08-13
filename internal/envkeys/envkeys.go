// Package envkeys holds the environment-variable names and the sidecar-file suffix that form
// the runtime contract between the controller (which injects them onto claimer and filter
// containers) and the claimer binary (which reads them). It is a dependency-free leaf package
// on purpose: cmd/claimer can share these values without importing the controller/CRD/k8s
// types, removing the hand-maintained duplication that previously risked silent drift. PLAT-1499.
package envkeys

const (
	// SourcePath is where the claimer stores the downloaded object; filters read the media from it.
	SourcePath = "SOURCE_PATH"

	// VideoInputPath is a deprecated alias of SourcePath, kept so in-flight specs referencing
	// $(VIDEO_INPUT_PATH) keep resolving during rollout.
	VideoInputPath = "VIDEO_INPUT_PATH"

	// FilterOverrideSourceURIFile names the sidecar file the claimer writes next to the download;
	// the entry filter reads the object's real source URI from it and reports it as meta['src'].
	FilterOverrideSourceURIFile = "FILTER_OVERRIDE_SOURCE_URI_FILE"

	// SourceURIFileSuffix is appended to the download path to form the sidecar filename.
	SourceURIFileSuffix = ".source_uri"

	// DefaultInputPath is where the claimer stores the downloaded object when SourcePath is
	// unset. Deliberately extension-less: entry filters are extension-agnostic and the object's
	// real source URI travels via the .source_uri sidecar, not the filename. Shared so the
	// controller's fallback and the claimer's default can't diverge.
	DefaultInputPath = "/ws/input"
)
