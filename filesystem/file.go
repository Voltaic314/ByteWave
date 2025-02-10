package filesystem

import "fmt"

// FileSubItem represents a basic file with attributes like name, path, size, and parentID.
type FileSubItem struct {
	Name      string
	Path      string
	Identifier string
	ParentID  *string // Using a pointer to represent optional values
	Size      *int64  // Size is optional, so we use a pointer
}

// String method to provide a formatted representation of FileSubItem (similar to __repr__)
func (f FileSubItem) String() string {
	return fmt.Sprintf("FileSubItem(name=%s, path=%s, size=%v)", f.Name, f.Path, f.Size)
}

// File represents a file with separate source and destination attributes.
type File struct {
	Source      *FileSubItem // Using pointers to allow nil values
	Destination *FileSubItem
}

// String method for File struct
func (f File) String() string {
	return fmt.Sprintf("File(Source=%v, Destination=%v)", f.Source, f.Destination)
}
