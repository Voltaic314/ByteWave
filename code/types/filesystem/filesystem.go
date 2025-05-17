// Package filesystem provides types for file system operations and data structures.
package filesystem

// File represents a basic file with attributes
type File struct {
	Name         string
	Path         string
	Identifier   string
	ParentID     string
	Size         int64
	LastModified string
	Level        int
}

// Folder represents a basic folder with attributes
type Folder struct {
	Name         string
	Path         string
	Identifier   string
	ParentID     string
	LastModified string
	Level        int
}
