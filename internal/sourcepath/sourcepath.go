// Package sourcepath holds the constants that form the batch source-file handoff contract between
// the controller (which injects them onto claimer and filter containers) and the claimer binary
// (which reads them): the environment-variable names for the download path, the sidecar-file
// suffix, and the default download path. It is a dependency-free leaf package on purpose so
// cmd/claimer can share these values without importing the controller/CRD/k8s types, removing the
// hand-maintained duplication that previously risked silent drift between the two binaries.
package sourcepath

const (
	// EnvVar is the environment variable naming where the claimer stores the downloaded object;
	// filters read the media from it.
	EnvVar = "SOURCE_PATH"

	// DeprecatedEnvVar is a deprecated alias of EnvVar, kept so in-flight specs referencing
	// $(VIDEO_INPUT_PATH) keep resolving during rollout. Removed at 1.0.
	DeprecatedEnvVar = "VIDEO_INPUT_PATH"

	// OverrideFileEnvVar names the sidecar file the claimer writes next to the download; the entry
	// filter reads the object's real source URI from it and reports it as meta['src'].
	OverrideFileEnvVar = "FILTER_OVERRIDE_SOURCE_URI_FILE"

	// SidecarSuffix is appended to the download path to form the sidecar filename.
	SidecarSuffix = ".source_uri"

	// Default is where the claimer stores the downloaded object when no path is set. Deliberately
	// extension-less: entry filters are extension-agnostic and the object's real source URI travels
	// via the sidecar file, not the filename. Shared so the controller's fallback and the claimer's
	// default can't diverge.
	Default = "/ws/input"
)
