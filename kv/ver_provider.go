package kv

var (
	LatestVersion Version = nil
)

type Version interface{}

type VersionProvider interface {
	GetCurrentVersion() Version
}
