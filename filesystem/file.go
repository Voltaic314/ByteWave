package filesystem

import "fmt"

// File represents a basic file with attributes like name, path, size, and parentID.
type File struct {
	Name      string
	Path      string
	Identifier string
	ParentID  string // Using a pointer to represent optional values
	Size      int64  // Size is optional, so we use a pointer
	LastModified string // LastModified is optional, so we use a pointer
}

// String method to provide a formatted representation of File (similar to __repr__)
func (f File) String() string {
	return fmt.Sprintf("File(name=%s, path=%s, size=%v)", f.Name, f.Path, f.Size)
}