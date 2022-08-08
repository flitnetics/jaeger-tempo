package app

import (
        "github.com/jaegertracing/jaeger/model"
        "strconv"
        "time"
        "strings"
        "log"
        "fmt"
	"encoding/json"
        "github.com/go-logfmt/logfmt"
	"hash/fnv"
)

type whereBuilder struct {
	where  string
	params []interface{}
}

func (r *whereBuilder) andWhere(param interface{}, where string) {
	if len(r.where) > 0 {
		r.where += " AND "
	}
	r.where += where
	r.params = append(r.params, param)
}

func StrToMap(in string) map[string]interface{} {
    res := make(map[string]interface{})
    array := strings.Split(in, " ")
    temp := make([]string, 2)
    for _, val := range array {
        temp = strings.Split(string(val), ":")
        if len(temp) > 1 {
            res[temp[0]] = temp[1]
        } else {
            res[temp[0]] = ""
        }
    }
    return res
}

func toModelSpanNew(values string, chunk SpanData) *model.Span {
        var id model.SpanID
        var trace_id_low uint64
        var trace_id_high uint64
        var operation_name string
        var flags model.Flags
        var duration time.Duration
        var tags map[string]interface{}
        var process_tags map[string]interface{}
        var start_time time.Time
        var process_id string
        var service_name string

        d := logfmt.NewDecoder(strings.NewReader(values))
	for d.ScanRecord() {
                // iterate over all logfmt values
		for d.ScanKeyval() {
                        if string(d.Key()) == "span_id" {
                                convId, _ := strconv.ParseUint(string(d.Value()), 10, 32)
                                id = model.NewSpanID(convId)
                        }
                        if string(d.Key()) == "trace_id_low" {
                                trace_id_low, _ = strconv.ParseUint(string(d.Value()), 10, 32)
                        }
                        if string(d.Key()) == "trace_id_high" {
                                trace_id_high, _ = strconv.ParseUint(string(d.Value()), 10, 32)
                        }
                        if string(d.Key()) == "flags" {
                                convFlags, _ := strconv.ParseUint(string(d.Value()), 10, 64)
                                flags = model.Flags(convFlags)
                        }
                        if string(d.Key()) == "duration" {
                                duration, _ = time.ParseDuration(string(d.Value()))
                        }
                        if string(d.Key()) == "tags" {
                                tags = StrToMap(string(d.Value()))
                        }
                        if string(d.Key()) == "process_id" {
                                process_id = string(d.Value())
                        }
                        if string(d.Key()) == "process_tags" {
                                process_tags = StrToMap(string(d.Value()))
                        }
                        if string(d.Key()) == "start_time" {
                                start_time, _ = time.Parse(time.RFC3339, string(d.Value()))
                        }
                        if string(d.Key()) == "service_name" {
                                service_name = string(d.Value())
                        }
                        if string(d.Key()) == "operation_name" {
                                operation_name = string(d.Value())
                        }
		}
	}

        // from labels
        service_name = chunk.ServiceName
        operation_name = chunk.OperationName

        return &model.Span{
                SpanID:        id,
		TraceID:       model.TraceID{Low: trace_id_low, High: trace_id_high},
                OperationName: operation_name,
                Flags:         flags,
                Duration:      duration,
                Tags:          mapToModelKV(tags),
                StartTime:     start_time,
                ProcessID:     process_id,
                Process: &model.Process{
                        ServiceName: service_name,
                        Tags:        mapToModelKV(process_tags),
                },
                //Warnings:   warnings,
                References: make([]model.SpanRef, 0),
                Logs:       make([]model.Log, 0),
        }
}

func toModelSpan(chunk Spans, spanAttribute SpanAttributes, resourceAttribute ResourceAttributes) *model.Span {
        var id model.SpanID
        convId := hash(chunk.SpanId)
        id = model.NewSpanID(convId)

        var trace_id_low uint64
        trace_id_low = hash(chunk.TraceId)
        var trace_id_high uint64
        trace_id_high = hash(chunk.TraceId)

        var operation_name string
        operation_name = chunk.OperationName

        var flags model.Flags
        convFlags, _ := strconv.ParseUint(chunk.Flags, 10, 64)
        flags = model.Flags(convFlags)

        var duration time.Duration
        startTime, err := strconv.ParseInt(chunk.StartTimeUnixNano, 10, 64)
        if err != nil {
                log.Println("error converting starttime")
        }
	sTime := time.Unix(0, startTime)
        endTime, err := strconv.ParseInt(chunk.EndTimeUnixNano, 10, 64)
        if err != nil {
                log.Println("error converting endtime")
        }
	eTime := time.Unix(0, endTime)
	duration = eTime.Sub(sTime)

        var tags map[string]interface{}
        j, err := json.Marshal(spanAttribute)
        if err != nil {
                log.Println("Error marshalling tags: ", err)
        }
        json.Unmarshal(j, &tags)
        if err != nil {
                log.Println("Error unmarshalling tags: ", err)
        }

        var process_id string
        process_id = chunk.ProcessId

        var service_name string
	if resourceAttribute.Key == "service.name" {
                service_name = resourceAttribute.Value.StringValue
	}

        var process_tags map[string]interface{}
        pt, err := json.Marshal(resourceAttribute)
        if err != nil {
                log.Println("Error unmarshalling process tags: ", err)
        }
        err = json.Unmarshal(pt, &process_tags)
	if err != nil {
                log.Println("Error unmarshalling process tags: ", err)
	}

        /* var warnings string
        if (chunk.Metric[2].Name == "warnings") {
                warnings = chunk.Metric[2].Value
        } */

        var start_time int64
	start_time, err = strconv.ParseInt(chunk.StartTimeUnixNano, 10, 64)
	if err != nil {
                log.Println("Error parsing time: ", err)
	}
	start_unixtime := time.Unix(0, start_time) // start_time is nanosecond this case

	return &model.Span{
		SpanID:        id,
		TraceID:       model.TraceID{Low: trace_id_low, High: trace_id_high},
		OperationName: operation_name,
		Flags:         flags,
		StartTime:     start_unixtime,
		Duration:      duration,
		Tags:          mapToModelKV(tags),
		ProcessID:     process_id,
		Process: &model.Process{
			ServiceName: service_name,
			Tags:        mapToModelKV(process_tags),
		},
		//Warnings:   warnings,
		References: make([]model.SpanRef, 0),
		Logs:       make([]model.Log, 0),
	}
}

func mapToModelKV(input map[string]interface{}) []model.KeyValue {
	ret := make([]model.KeyValue, 0, len(input))
	var kv model.KeyValue
	for k, v := range input {
		if vStr, ok := v.(string); ok {
			kv = model.KeyValue{
				Key:   k,
				VType: model.ValueType_STRING,
				VStr:  vStr,
			}
			ret = append(ret, kv)
		} else if vBytes, ok := v.([]byte); ok {
			kv = model.KeyValue{
				Key:     k,
				VType:   model.ValueType_BINARY,
				VBinary: vBytes,
			}
			ret = append(ret, kv)
		} else if vBool, ok := v.(bool); ok {
			kv = model.KeyValue{
				Key:   k,
				VType: model.ValueType_BOOL,
				VBool: vBool,
			}
			ret = append(ret, kv)
		} else if vInt64, ok := v.(int64); ok {
			kv = model.KeyValue{
				Key:    k,
				VType:  model.ValueType_INT64,
				VInt64: vInt64,
			}
			ret = append(ret, kv)
		} else if vFloat64, ok := v.(float64); ok {
			kv = model.KeyValue{
				Key:      k,
				VType:    model.ValueType_FLOAT64,
				VFloat64: vFloat64,
			}
			ret = append(ret, kv)
		}
	}
	return ret
}

func mapModelKV(input []model.KeyValue) string {
        ret := ""
	var value interface{}
	for _, kv := range input {
		value = nil
		if kv.VType == model.ValueType_STRING {
			value = kv.VStr
		} else if kv.VType == model.ValueType_BOOL {
			value = strconv.FormatBool(kv.VBool)
		} else if kv.VType == model.ValueType_INT64 {
			value = strconv.FormatInt(int64(kv.VInt64), 10)
		} else if kv.VType == model.ValueType_FLOAT64 {
			value = strconv.FormatFloat(kv.VFloat64, 'E', -1, 64)
		} else if kv.VType == model.ValueType_BINARY {
			value = kv.VBinary
		}
		ret = ret + fmt.Sprintf("%s:%s", kv.Key, value) + " "
	}
        //log.Println(ret)
	return ret
}

func hash(s string) uint64 {
        h := fnv.New64a()
        h.Write([]byte(s))
        return h.Sum64()
}
