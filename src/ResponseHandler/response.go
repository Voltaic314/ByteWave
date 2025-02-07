package ResponseHandler

import (
	"fmt"
	"time"
)

// Response represents an API or function response with status, data, errors, and warnings.
type Response struct {
	Ok        bool        // Indicates if the operation was successful
	Data      interface{} // Generic response data
	Errors    []Error     // List of errors
	Warnings  []Warning   // List of warnings
	Timestamp time.Time   // Time of response creation
}

// NewResponse creates a new Response instance.
func NewResponse(ok bool, data interface{}, errors []Error, warnings []Warning) Response {
	return Response{
		Ok:        ok,
		Data:      data,
		Errors:    errors,
		Warnings:  warnings,
		Timestamp: time.Now(),
	}
}

// AddError appends an error to the response.
func (r *Response) AddError(err Error) {
	r.Errors = append(r.Errors, err)
}

// AddWarning appends a warning to the response.
func (r *Response) AddWarning(warn Warning) {
	r.Warnings = append(r.Warnings, warn)
}

// ToMap converts the Response to a map for serialization/logging.
func (r Response) ToMap() map[string]interface{} {
	errorsList := make([]map[string]interface{}, len(r.Errors))
	for i, err := range r.Errors {
		errorsList[i] = err.ToMap()
	}

	warningsList := make([]map[string]interface{}, len(r.Warnings))
	for i, warn := range r.Warnings {
		warningsList[i] = warn.ToMap()
	}

	return map[string]interface{}{
		"ok":        r.Ok,
		"data":      r.Data,
		"errors":    errorsList,
		"warnings":  warningsList,
		"timestamp": r.Timestamp.Format(time.RFC3339),
	}
}

// String implements the fmt.Stringer interface for pretty-printing.
func (r Response) String() string {
	return fmt.Sprintf(
		"Response(ok=%v, data=%v, errors=%d, warnings=%d, timestamp=%s)",
		r.Ok, r.Data, len(r.Errors), len(r.Warnings), r.Timestamp.Format(time.RFC3339),
	)
}
