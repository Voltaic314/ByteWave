package filesystem
import "fmt"

// File represents a basic file with attributes like name, path, size, and parentID.
type File struct {
	Name          string
	Path          string
	Identifier    string
	ParentID      string 
	Size          int64  
	LastModified  string 
	Level         int
}

// String method to provide a formatted representation of File (similar to __repr__)
func (f File) String() string {
	return fmt.Sprintf("File(name=%s, path=%s, size=%v, lastModified=%s, level=%v)", f.Name, f.Path, f.Size, f.LastModified, f.Level)
}