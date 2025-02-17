package filesystem

import "fmt"

// Folder represents a basic folder with attributes like name, path, identifier, and parentID.
type Folder struct {
	Name       string
	Path       string
	Identifier string
	ParentID   string // Optional field using a pointer
	LastModified string // LastModified is optional, so we use a pointer
}

// String method to provide a formatted representation of Folder (like __repr__)
func (f Folder) String() string {
	return fmt.Sprintf("Folder(name=%s, path=%s)", f.Name, f.Path)
}
