package filesystem

import "fmt"

// Folder represents a basic folder with attributes like name, path, identifier, and parentID.
type Folder struct {
	Name          string
	Path          string
	Identifier    string
	ParentID      string 
	LastModified  string 
	Level         int
}

// String method to provide a formatted representation of Folder (like __repr__)
func (f Folder) String() string {
	return fmt.Sprintf("Folder(name=%s, path=%s, lastModified=%s, level=%v)", f.Name, f.Path, f.LastModified, f.Level)
}
