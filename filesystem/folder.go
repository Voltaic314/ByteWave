package filesystem

import "fmt"

// FolderSubItem represents a basic folder with attributes like name, path, identifier, and parentID.
type FolderSubItem struct {
	Name       string
	Path       string
	Identifier string
	ParentID   *string // Optional field using a pointer
}

// String method to provide a formatted representation of FolderSubItem (like __repr__)
func (f FolderSubItem) String() string {
	return fmt.Sprintf("FolderSubItem(name=%s, path=%s)", f.Name, f.Path)
}

// Folder represents a folder with separate source and destination attributes.
type Folder struct {
	Source      *FolderSubItem // Using pointers to allow nil values
	Destination *FolderSubItem
}

// String method for Folder struct
func (f Folder) String() string {
	return fmt.Sprintf("Folder(Source=%v, Destination=%v)", f.Source, f.Destination)
}
