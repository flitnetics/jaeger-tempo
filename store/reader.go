package app

import (
	"context"
	"time"
        "log"
        "fmt"
        _ "strconv"
        "encoding/json"
	"github.com/go-logfmt/logfmt"
        "strings"
        "net/http"
        "net/url"
	"io/ioutil"
        "strconv"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var _ spanstore.Reader = (*Reader)(nil)

// Reader can query for and load traces from your object store.
type Reader struct {
        cfg    Config
}

type SpanData struct {
        // defining struct variables
        Duration      string  `json:"duration"`
        Env           string  `json:"env"`
        Flags         string  `json:"flags"`
        Id            string  `json:"id"`
        OperationName string  `json:"operation_name"`
        ProcessTags   string  `json:"process_tags"`
        ProcessId     string  `json:"process_id"`
        ServiceName   string  `json:"service_name"`
        StartTime     string  `json:"start_time"`
        Tags          string  `json:"tags"`
        TraceIdHigh   string  `json:"trace_id_high"`
        TraceIdLow    string  `json:"trace_id_low"`
}

type LokiStream struct {
        SpanData SpanData        `json:"stream"` 
        SValues  [][]string      `json:"values"`
}

type LokiResult struct {
        Stream []LokiStream `json:"result"`
}

type LokiData struct {
        Result LokiResult `json:"data"`
}

// Span (not spanrange query)
type Metric struct {
        ServiceName   string  `json:"service_name"`
        OperationName string  `json:"operation_name"`
        Env           string  `json:"env"`
}

type sLokiStream struct {
        Metric   Metric `json:"metric"`
}

type sLokiResult struct {
        Stream []sLokiStream `json:"result"`
}

type sLokiData struct {
        Result sLokiResult `json:"data"`
}

// Query endpoint
type Value struct {
        StringValue string `json:"stringValue"`
}

type ResourceAttributes struct {
        Key string `json:"key"`
        Value Value `json:"value"`
}

type Resource struct {
        ResourceAttributes []ResourceAttributes `json:"attributes"`
}

type SpanAttributes struct {
        Key string `json:"key"`
        Value Value `json:"value"`
}

type Spans struct {
       TraceId           string       `json:"traceId"`
       SpanId            string       `json:"spanId"`
       StartTimeUnixNano string       `json:"startTimeUnixNano"`
       EndTimeUnixNano   string       `json:"endTimeUnixNano"`
       SpanAttributes    []SpanAttributes `json:"attributes"`
       Duration          string       `json:"duration"`
       Flags             string       `json:"flags"`
       OperationName     string       `json:"name"`
       ProcessTags       string       `json:"process_tags"`
       ProcessId         string       `json:"process_id"`
       ServiceName       string       `json:"service_name"`
       StartTime         string       `json:"start_time"`
       Tags              string       `json:"tags"`
       TraceIdHigh       string       `json:"traceId_high"`
       TraceIdLow        string       `json:"traceId_low"`
}

type InstrumentationLibrarySpans struct {
        InstrumentationLibrary map[string]string `json:"instrumentationLibrary"`
        Spans                  []Spans `json:"spans"`
}

type Batches struct {
        Resource Resource `json:"resource"`
        InstrumentationLibrarySpans []InstrumentationLibrarySpans `json:"instrumentationLibrarySpans"`
}

type Search struct {
        Batches []Batches `json:"batches"`
}

// tag 
type Tag struct {
        TagValues []string  `json:"tagValues"`
}

// Search Endpoint
type Traces struct {
        TraceId string `json:"traceID"`
	RootServiceName string `json:"rootServiceName"`
	RootTraceName string `json:"rootTraceName"`
	StartTimeUnixNano string `json:"startTimeUnixNano"`
	DurationMs uint32 `json:"durationMs"`
}

type Trace struct {
        Traces []Traces `json:"traces"`
}

// NewReader returns a new SpanReader for the object store.
func NewReader(cfg Config) *Reader {
	return &Reader{
                cfg: cfg,
	}
}

func GetTagValues() (Tag, error) {
        var tag Tag

        httpurl :=  "http://host.docker.internal:3200/api/search/tag/service.name/values"

        response, err := http.Get(httpurl)
        if err != nil {
                return Tag{}, err
        }

        body, err := ioutil.ReadAll(response.Body)
        if err != nil {
                return Tag{}, err
        }

        err = json.Unmarshal(body, &tag)
        if err != nil {
               log.Println("Problem with unmarshalling json: %s", err)
        }

        return tag, err
}

func GetSearch(service string) (Trace, error) {
        var result Trace

        httpurl :=  fmt.Sprintf("http://host.docker.internal:3200/api/search?tags=service.name=%s&limit=100000", service)

        response, err := http.Get(httpurl)
        if err != nil {
                return Trace{}, err
        }

        body, err := ioutil.ReadAll(response.Body)
        if err != nil {
                return Trace{}, err
        }

        err = json.Unmarshal(body, &result)
        if err != nil {
               log.Println("Problem with unmarshalling json: %s", err)
        }

        return result, err
}

func GetSearchTrace(tags string, minDuration time.Duration, maxDuration time.Duration, limit uint32, start time.Time, end time.Time) (Trace, error) {
        var result Trace

	startTime := start.Unix()
	endTime   := end.Unix()
        httpurl :=  fmt.Sprintf("http://host.docker.internal:3200/api/search?tags=service.name=%s&minDuration=%s&maxDuration=%s&limit=%d&start=%d&end=%d", tags, minDuration, maxDuration, limit, startTime, endTime)
	log.Println("GetSearchTraceURL: %s", httpurl)

        response, err := http.Get(httpurl)
        if err != nil {
                return Trace{}, err
        }

        body, err := ioutil.ReadAll(response.Body)
        if err != nil {
		log.Println("Error reading body: ", body)
                return Trace{}, err
        }

        err = json.Unmarshal(body, &result)
        if err != nil {
               log.Println("Problem with unmarshalling json: %s", err)
        }

        return result, err
}

func GetQuery(traceId string, start time.Time, end time.Time) (Search, error) {
        var result Search

        startTime := start.Unix()
        endTime   := end.Unix()
        httpurl :=  fmt.Sprintf("http://host.docker.internal:3200/api/traces/%s?start=%d&end=%d", traceId, startTime, endTime)

	log.Println("GetQueryURL: %s", httpurl)

        response, err := http.Get(httpurl)
        if err != nil {
                return Search{}, err
        }

        body, err := ioutil.ReadAll(response.Body)
        if err != nil {
                log.Println("Error reading body: ", body)
                return Search{}, err
        }


        err = json.Unmarshal(body, &result)
        if err != nil {
               log.Println("Problem with unmarshalling json: %s", err)
        }

        return result, err 
	return Search{}, nil
}

func GetSpansRange(r *Reader, fooLabelsWithName string, startTime time.Time, endTime time.Time, resultsLimit uint32) (LokiData, error) {
        var s_labels LokiData

        query   := url.QueryEscape(fooLabelsWithName)
        httpurl := fmt.Sprintf("http://localhost:3200/loki/api/v1/query_range?direction=BACKWARD&limit=%d&query=%s&start=%d&end=%d", resultsLimit, query, startTime.UnixNano(), endTime.UnixNano())

        response, err := http.Get(httpurl)
        if err != nil {
                return LokiData{}, err
        }

        body, err := ioutil.ReadAll(response.Body)
        if err != nil {
                return LokiData{}, err
        }

        err = json.Unmarshal(body, &s_labels)
        if err != nil {
               log.Println("Problem with unmarshalling json: %s", err)
        }

        return s_labels, err
}

func extractOperations(a Trace) []string {
    list := []string{}
    keys := make(map[string]bool)

    for _, entry := range a.Traces {
        if _, value := keys[entry.RootTraceName]; !value {
                // assign key value to list
                keys[entry.RootTraceName] = true
                list = append(list, entry.RootTraceName)
        }
    }
    return list
}

// GetServices returns all services traced by Jaeger
func (r *Reader) GetServices(ctx context.Context) ([]string, error) {
	services, err := GetTagValues()
        if err != nil {
                log.Println("error getting tag values!")
	}
        return services.TagValues, nil
}

// GetOperations returns all operations for a specific service traced by Jaeger
func (r *Reader) GetOperations(ctx context.Context, param spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
        results, err := GetSearch(param.ServiceName)
        if err != nil {
                log.Println("error getting doing search!")
        }
	operations := extractOperations(results)

	spans := make([]spanstore.Operation, 0, len(operations))
        for _, operation := range operations {
                if len(operation) > 0 {
                        spans = append(spans, spanstore.Operation{Name: operation})
                }
        }

        return spans, err
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (r *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
        log.Println("GetTrace executed")

        // will improvise this code later
        // traceID is in []model.TraceID{traceID}[0]
        var fooLabelsWithName = fmt.Sprintf("{env=\"prod\"} |= `trace_id_low=\"%d\"`", traceID.Low)
        log.Println("GetTrace Query: ", fooLabelsWithName)

        spans, err := GetSpansRange(r, fooLabelsWithName, time.Now().Add(time.Duration(-24) * time.Hour), time.Now(), uint32(100))
        chunks := spans.Result.Stream

        span := make([]*model.Span, 0, len(chunks)) 
        trace := make([]model.Trace_ProcessMapping, 0, len(chunks))
        for _, chunk := range chunks {
                var serviceName string
                var processId string
                var processTags map[string]interface{}

                // we decode the logfmt data in values
                // please refactor this decoder out to common code
                for _, value := range chunk.SValues {
                        d := logfmt.NewDecoder(strings.NewReader(string(value[1])))
                        for d.ScanRecord() {
                                for d.ScanKeyval() {
                                        if string(d.Key()) == "service_name" {
                                                serviceName = string(d.Value())
                                        }
                                        if string(d.Key()) == "process_id" {
                                                processId = string(d.Value())
                                        }
                                        if string(d.Key()) == "process_tags" {
                                                processTags = StrToMap(string(d.Value()))
                                        }
                                }
                        }
                        if d.Err() != nil {
                                log.Println("decoding logfmt error!", d.Err())
                        }
                        // end of decode

                        span = append(span, toModelSpanNew(value[1], chunk.SpanData))
                        trace = append(trace, model.Trace_ProcessMapping{
                                ProcessID: processId,
                                Process: model.Process{
                                        ServiceName: serviceName,
                                        Tags:        mapToModelKV(processTags),
                                },
                        })
                }
        }

        return &model.Trace{Spans: span, ProcessMap: trace}, err
}

func buildTraceWhere(query *spanstore.TraceQueryParameters) (string, time.Time, time.Time) { 
        log.Println("buildTraceWhere executed")
        var builder string
        //log.Println("min time: %s", query.StartTimeMin)

        if len(query.ServiceName) > 0 {
		builder = fmt.Sprintf("%s", query.ServiceName)
        }
        /* if len(query.OperationName) > 0 {
                builder = builder + fmt.Sprintf("operation_name = \"%s\", ", query.OperationName)
        } */

        if len(query.Tags) > 0 {
                for i, v := range query.Tags {
                        builder = builder + fmt.Sprintf("&\"%i=%v\"", i, v)
                }
        }

        // how many result of the traces do we want to show
        /* if query.NumTraces > 0 {
                builder = builder + fmt.Sprintf(" | limit = %d", query.NumTraces)
        } */

        // log our queries
        log.Println("builder: %s", builder)

        // here we include starttime min and max to pass to indexed timestamp
	return builder, query.StartTimeMin, query.StartTimeMax
}

// FindTraces retrieve traces that match the traceQuery
func (r *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
       log.Println("FindTraces executed")

       builder, _, _ := buildTraceWhere(query)

       m := make(map[string]bool)
       var traceIdsLow []string

       minDuration := time.Duration(query.DurationMin) / time.Nanosecond
       maxDuration := time.Duration(query.DurationMax) / time.Nanosecond

       tra, err := GetSearchTrace(builder, minDuration, maxDuration, uint32(query.NumTraces), query.StartTimeMin, query.StartTimeMax)
       traces := tra.Traces

       ret := make([]*model.Trace, 0, len(traces))
       if err != nil {
               return ret, err
       }
       grouping := make(map[model.TraceID]*model.Trace)

       for _, trace := range traces {
                       // get all trace ids
		       var traceIdLow string

                       if !m[traceIdLow] {
                               traceIdsLow = append(traceIdsLow, trace.TraceId)
                               m[traceIdLow] = true
                       }
       }

       // final query
       // now we get the real values
       for _, traceIDLow := range traceIdsLow {

               query, err := GetQuery(traceIDLow, query.StartTimeMin, query.StartTimeMax)
               if err != nil {
                       log.Println("Unable to retrieve related Spans")
               }

	       queries := query.Batches

               for _, trace := range queries {
                       var serviceName string
                       var processId string
                       var processTags map[string]interface{}

		       resourceAttributes := trace.Resource.ResourceAttributes

		       // resource array
		       for _, resourceAttribute := range resourceAttributes {

                               if resourceAttribute.Key == "service_name" {
                                       serviceName = resourceAttribute.Value.StringValue
		               }

			       // instrument library spans array
		               instrumentLibrarySpans := trace.InstrumentationLibrarySpans
			       for _, instrumentLibrary := range instrumentLibrarySpans {

				       spans := instrumentLibrary.Spans
				       // span array
		                       for _, span := range spans {

					       // span attribute array
					       for _, spanAttribute := range span.SpanAttributes {
						       // span attributes
					               j, err := json.Marshal(spanAttribute)
                                                       if err != nil {
                                                               log.Println("Cannot marshal data from spanAttribute: ", err)
                                                       }
                                                       err = json.Unmarshal(j, &processTags)
						       if err != nil {
                                                               log.Println("Cannot unmarshal data from spanAttribute: ", err)
						       }
                                                       processId = ""

                                                       modelSpan := toModelSpan(span, spanAttribute, resourceAttribute)
                                                       trace, found := grouping[modelSpan.TraceID]
                                                       if !found {
                                                               trace = &model.Trace{
                                                                       Spans:      make([]*model.Span, 0, len(spans)),
                                                                       ProcessMap: make([]model.Trace_ProcessMapping, 0, len(spans)),
                                                               }
                                                               grouping[modelSpan.TraceID] = trace
                                                       }
                                                       trace.Spans = append(trace.Spans, modelSpan)
                                                       procMap := model.Trace_ProcessMapping{
                                                               ProcessID: processId,
                                                               Process: model.Process{
                                                                       ServiceName: serviceName,
                                                                       Tags:        mapToModelKV(processTags),
                                                               },
                                                       }
                                                       trace.ProcessMap = append(trace.ProcessMap, procMap)
					       }
                                       }
		               }
                       }
               }
       }

       for _, trace := range grouping {
               ret = append(ret, trace)
       }

       return ret, err
}

// FindTraceIDs retrieve traceIDs that match the traceQuery
func (r *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) (ret []model.TraceID, err error) {
        builder, _, _ := buildTraceWhere(query)

        var fooLabelsWithName = builder

        spans, err := GetSpansRange(r, fooLabelsWithName, query.StartTimeMin, query.StartTimeMax, uint32(query.NumTraces))
        if err != nil {
                log.Println("Unable to get FindTraceIDs span!")
        }
        chunks := spans.Result.Stream

        var trace model.TraceID
        var traces []model.TraceID
        for _, chunk := range chunks {
                // we decode the logfmt data in values
                // please refactor this decoder out to common code
                for _, value := range chunk.SValues {

                        // query based on trace ID
                        d := logfmt.NewDecoder(strings.NewReader(value[1]))
                        for d.ScanRecord() {
                                for d.ScanKeyval() {
                                        if string(d.Key()) == "trace_id_low" {
                                                low, _ := strconv.ParseUint(string(d.Value()), 10, 64) 
                                                trace.Low = low
                                        }
                                        if string(d.Key()) == "trace_id_high" {
                                                high, _ := strconv.ParseUint(string(d.Value()), 10, 64)
                                                trace.High = high
                                        }
                                }
                        }
                        if d.Err() != nil {
                                log.Println("decoding logfmt error!", d.Err())
                        }
                        // end of decode
                        traces = append(traces, trace)

                }
        }

        return traces, err
}

// GetDependencies returns all inter-service dependencies
func (r *Reader) GetDependencies(context context.Context, endTs time.Time, lookback time.Duration) (ret []model.DependencyLink, err error) {
	/* err = r.db.Model((*SpanRef)(nil)).
		ColumnExpr("source_spans.service_name as parent").
		ColumnExpr("child_spans.service_name as child").
		ColumnExpr("count(*) as call_count").
		Join("JOIN spans AS source_spans ON source_spans.id = span_ref.source_span_id").
		Join("JOIN spans AS child_spans ON child_spans.id = span_ref.child_span_id").
		Group("source_spans.service_name").
		Group("child_spans.service_name").
		Select(&ret)

	return ret, err */
        return nil, err
}
