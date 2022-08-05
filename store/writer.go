package app
  
import (
        "io"
	"context"

        "github.com/jaegertracing/jaeger/model"
        "github.com/jaegertracing/jaeger/storage/spanstore"
)

var _ spanstore.Writer = (*Writer)(nil)
var _ io.Closer = (*Writer)(nil)

// Writer handles all writes to object store for the Jaeger data model
type Writer struct {
       spanMeasurement     string
       spanMetaMeasurement string
       logMeasurement      string

       cfg                 Config
}

func (w *Writer) WriteSpan(context context.Context, span *model.Span) error {
        return nil
}

// Close triggers a graceful shutdown
func (w *Writer) Close() error {
        return nil
}
