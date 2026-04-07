package fact_store

import (
	"encoding/json"
	"fmt"
	"github.com/baidubce/bce-sdk-go/util/log"
	integrationconfig "github.com/nebulaiq/nebulaiq_telemetry/server/ingester/agent_metrics/series/config"
	factStoreProto "github.com/nebulaiq/nebulaiq_telemetry/server/libs/fact_store"
	"github.com/nebulaiq/nebulaiq_telemetry/server/libs/utils"
	"github.com/op/go-logging"
	uuid "github.com/satori/go.uuid"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TODO: Implement correct Kafka Partitioning similar to Rust -- Need to fix num of partitions and use that as custom partitioner

var logger = logging.MustGetLogger("fact_store")

const Unknown = "unknown"

var factMetricBatchMap sync.Map // key: string, value: *BatchInfo

// BatchInfo holds the input and the last seen timestamp (as provided by input.Time).
type BatchInfo struct {
	FactName string
	Input    *integrationconfig.MetricDetailPlatformData
	LastSeen int64 // using input.Time (epoch seconds)
}

//var factIdsToDebug = make(map[string]bool)

type FactStore struct {
	config          *FactStoreConfig
	fsCache         *FsIdCache
	fsKafkaProducer *factStoreKafkaProducer
	isClosed        atomic.Bool
}

func NewFactStore(config *FactStoreConfig) (*FactStore, error) {
	logger.Infof("in FactStore")
	if !config.Enabled {
		logger.Infof("FactStore is not enabled in the configuration")
		return nil, nil
	}

	factIdsToDebugEnv := os.Getenv("FACT_IDS_TO_DEBUG")
	logger.Warningf("FACT_IDS_TO_DEBUG: %s", factIdsToDebugEnv)

	logger.Infof(
		"Starting fact store with Kafka endpoint: %v, Kafka topic: %s",
		config.AggEventKafka.Brokers,
		config.AggEventKafka.Topic,
	)

	kafkaProducer, err := newFactStoreKafkaProducer(config.AggEventKafka)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for fact store: %w", err)
	}

	factStore := &FactStore{
		config:          config,
		fsCache:         NewFsIdCache(),
		fsKafkaProducer: kafkaProducer,
	}

	return factStore, nil
}

// Todo: Add Single method for fact Id and fact Metric

func (f *FactStore) SendFactId(fact *FsFactId) {
	if f.isClosed.Load() {
		logger.Warning("FactStore is closed, ignoring SendFactId call")
		return
	}
	cache, ok := f.fsCache.FactIDCache[fact.FactType.String()]
	if !ok {
		return
	}
	_, found := cache.Get(fact.ID.String())
	if found {
		return // Already seen, skip
	}

	// Add to cache for deduplication
	cache.SetWithTTL(fact.ID.String(), nil, 1, 24*time.Hour)

	logger.Debugf("cached fact id %v", fact)
}

func (f *FactStore) sendFactMetric(factId *FsFactId, input *integrationconfig.MetricDetailPlatformData, meta map[string]interface{}) error {

	factIdStr := factId.ID.String()
	factName := factId.FactName
	factType := factId.FactType

	// --- End Global Map Update Logic ---
	if !strings.Contains(factName, "unknown") {
		// ----- TODO: remove this logic
		key := fmt.Sprintf("%s:%s", factIdStr, input.MetricName)
		currentTime := int64(input.Time)

		if val, ok := factMetricBatchMap.Load(key); ok {
			existing := val.(*BatchInfo)
			if existing.LastSeen > currentTime {
				if factType != factStoreProto.FactIdType_Node {
					logger.Warningf("Fact Name (%s) Existing input (last seen %d): %+v, Current input (time %d): %+v",
						factName, existing.LastSeen, existing.Input, currentTime, input)
				}
				return nil
			} else if currentTime > existing.LastSeen {
				// New input is more recent; update with current input.
				newBatchInfo := &BatchInfo{
					FactName: factName,
					Input:    input,
					LastSeen: currentTime,
				}
				factMetricBatchMap.Store(key, newBatchInfo)
			}
		} else {
			// Key does not exist, so add the current input.
			newBatchInfo := &BatchInfo{
				FactName: factName,
				Input:    input,
				LastSeen: currentTime,
			}
			factMetricBatchMap.Store(key, newBatchInfo)
		}
	}
	// --- End Global Map Update Logic ---

	var appMetric *factStoreProto.Metric

	if input.AggregationType == integrationconfig.GAUGE {
		value := input.Value
		appMetric = &factStoreProto.Metric{
			Name:       input.MetricName,
			MetricType: factStoreProto.MetricType_Gauge,
			MetricValue: &factStoreProto.MetricValue{
				MetricInnerValue: &factStoreProto.MetricValue_DoubleValue{
					DoubleValue: value,
				},
			},
		}

	} else if input.AggregationType == integrationconfig.HISTOGRAM {

		buckets := make([]*factStoreProto.HistogramBucket, len(input.HistogramValues.Buckets))
		for i, bucket := range input.HistogramValues.Buckets {
			buckets[i] = &factStoreProto.HistogramBucket{
				UpperBound: bucket.UpperBound,
				Value:      bucket.Value,
			}
		}

		appMetric = &factStoreProto.Metric{
			Name:       input.MetricName,
			MetricType: factStoreProto.MetricType_Histogram,
			MetricValue: &factStoreProto.MetricValue{
				MetricInnerValue: &factStoreProto.MetricValue_HistogramData{
					HistogramData: &factStoreProto.HistogramData{
						Buckets: buckets,
						Sum:     input.HistogramValues.Sum,
						Count:   input.HistogramValues.Count,
					},
				},
			},
		}
	} else if input.AggregationType == integrationconfig.COUNT {
		appMetric = &factStoreProto.Metric{
			Name:       input.MetricName,
			MetricType: factStoreProto.MetricType_Count,
			MetricValue: &factStoreProto.MetricValue{
				MetricInnerValue: &factStoreProto.MetricValue_DoubleValue{
					DoubleValue: input.Value,
				},
			},
		}
	} else if input.AggregationType == integrationconfig.COUNTER {
		appMetric = &factStoreProto.Metric{
			Name:       input.MetricName,
			MetricType: factStoreProto.MetricType_Counter,
			MetricValue: &factStoreProto.MetricValue{
				MetricInnerValue: &factStoreProto.MetricValue_DoubleValue{
					DoubleValue: input.Value,
				},
			},
		}
	}

	if appMetric == nil {
		return fmt.Errorf("invalid aggregation type for metric {%s} for type {%v}", input.MetricName, input.AggregationType)
	}

	// Build labels from all incoming tags
	labels := make(map[string]string)

	// Add CommonLabelValueList
	for _, lv := range input.CommonLabelValueList {
		if lv.Name != "" && lv.Value != "" {
			labels[lv.Name] = lv.Value
		}
	}

	// Add LabelValueList
	for _, lv := range input.LabelValueList {
		if lv.Name != "" && lv.Value != "" {
			labels[lv.Name] = lv.Value
		}
	}

	// Add meta fields
	for key, value := range meta {
		switch v := value.(type) {
		case string:
			if v != "" {
				labels[key] = v
			}
		case map[string]string:
			if len(v) > 0 {
				tagsJSON, err := json.Marshal(v)
				if err == nil {
					labels[key] = string(tagsJSON)
				}
			}
		}
	}

	// Known keys used in aggregation fact creation
	knownAggKeys := map[string]bool{
		"t": true, "c": true, "ns": true, "svc": true, "inst": true, "node": true,
		"tap_side_kind": true, "cluster": true, "namespace": true, "service_name": true,
		"pod_name": true, "node_name": true, "pod_group_name": true,
	}

	// Find unknown label keys (not used in agg facts)
	var unknownKeys []string
	for k := range labels {
		if !knownAggKeys[k] {
			unknownKeys = append(unknownKeys, k)
		}
	}

	// Build LabelData with labels and unknown keys
	labelData := &factStoreProto.LabelData{
		Labels:           labels,
		LabelKeys:        "",                             // Skip sorted keys (expensive)
		UnknownLabelKeys: strings.Join(unknownKeys, ","), // Keys not used in agg facts
	}

	// Build raw fact detail (preserves all data - uses factId passed in)
	rawFactDetail := &factStoreProto.FactDetail{
		Id:                 factIdStr,
		FactIdType:         factType,
		ElementType:        factStoreProto.ElementType_NodeType,
		FactName:           factName,
		FactKey:            extractFactKeyFromName(factName),
		NormalizedFactName: factName,
	}

	// Build aggregated fact details (fan-outs: service, instance, node)
	aggFactDetails := buildAggFactDetailsForMetric(input, factId)

	fsMetrics := factStoreProto.FactMetrics{
		Timestamp:      input.Time,
		Metrics:        []*factStoreProto.Metric{appMetric},
		MetricSource:   input.MetricSource,
		LabelData:      &factStoreProto.FactMetrics_Labels{Labels: labelData},
		RawFactDetail:  rawFactDetail,
		AggFactDetails: aggFactDetails,
	}

	msg := factStoreProto.FactMetricsBatch{
		Header: &factStoreProto.Header{
			Version: 1,
		},
		FactMetrics: []*factStoreProto.FactMetrics{&fsMetrics},
	}

	logger.Debugf(
		"sending fact metrics for raw_fact_id %s, fact_name %s, agg_count %d",
		factIdStr,
		factName,
		len(aggFactDetails),
	)

	err := f.fsKafkaProducer.send(factIdStr, &msg)
	return err
}

// extractFactKeyFromName extracts the key pattern from a fact_name
// Example: "c=prod|ns=default|svc=api|t=k8" -> "c|ns|svc|t"
func extractFactKeyFromName(factName string) string {
	parts := strings.Split(factName, "|")
	keys := make([]string, len(parts))
	for i, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		keys[i] = kv[0]
	}
	return strings.Join(keys, "|")
}

// buildAggFactDetailsForMetric builds aggregated fact details at service, instance, node levels
// It creates base facts without labels, plus additional facts with LabelValueList for fine-grained aggregation
func buildAggFactDetailsForMetric(input *integrationconfig.MetricDetailPlatformData, baseFactId *FsFactId) []*factStoreProto.FactDetail {
	var aggDetails []*factStoreProto.FactDetail

	tapKind := getTapKind(input)
	cluster := utils.DefaultIfEmptyStr(input.Cluster, Unknown)
	namespace := utils.DefaultIfEmptyStr(input.Namespace, Unknown)
	service := utils.DefaultIfEmptyStr(input.PodGroup, Unknown)
	podName := utils.DefaultIfEmptyStr(input.PodName, Unknown)
	nodeIp := utils.DefaultIfEmptyStr(input.NodeIp, Unknown)
	job := utils.DefaultIfEmptyStr(input.Job, Unknown)
	kind := utils.DefaultIfEmptyStr(input.Kind, Unknown)
	condition := utils.DefaultIfEmptyStr(input.Condition, Unknown)

	// Build label pairs from LabelValueList
	var labelPairs [][2]string
	for _, lv := range input.LabelValueList {
		if lv.Name != "" && lv.Value != "" {
			labelPairs = append(labelPairs, [2]string{lv.Name, lv.Value})
		}
	}
	hasLabels := len(labelPairs) > 0

	// Service level fact: c|ns|svc|t
	svcBasePairs := [][2]string{
		{"t", tapKind},
		{"c", cluster},
		{"ns", namespace},
		{"svc", service},
	}
	svcFactName := buildSortedFactNameFromPairs(svcBasePairs)
	svcFactId := utils.GenerateUUIDV5(svcFactName)
	aggDetails = append(aggDetails, &factStoreProto.FactDetail{
		Id:                 svcFactId.String(),
		FactIdType:         factStoreProto.FactIdType_Service,
		ElementType:        factStoreProto.ElementType_NodeType,
		FactName:           svcFactName,
		FactKey:            extractFactKeyFromName(svcFactName),
		NormalizedFactName: svcFactName,
	})

	// Service level fact with labels: c|ns|svc|t + labels
	if hasLabels {
		svcLabelPairs := append(append([][2]string{}, svcBasePairs...), labelPairs...)
		svcLabelFactName := buildSortedFactNameFromPairs(svcLabelPairs)
		svcLabelFactId := utils.GenerateUUIDV5(svcLabelFactName)
		aggDetails = append(aggDetails, &factStoreProto.FactDetail{
			Id:                 svcLabelFactId.String(),
			FactIdType:         factStoreProto.FactIdType_Service,
			ElementType:        factStoreProto.ElementType_NodeType,
			FactName:           svcLabelFactName,
			FactKey:            extractFactKeyFromName(svcLabelFactName),
			NormalizedFactName: svcLabelFactName,
		})
	}

	// Instance level fact: c|inst|ns|svc|t
	if podName != Unknown {
		instBasePairs := [][2]string{
			{"t", tapKind},
			{"c", cluster},
			{"ns", namespace},
			{"svc", service},
			{"inst", podName},
		}
		instFactName := buildSortedFactNameFromPairs(instBasePairs)
		instFactId := utils.GenerateUUIDV5(instFactName)
		aggDetails = append(aggDetails, &factStoreProto.FactDetail{
			Id:                 instFactId.String(),
			FactIdType:         factStoreProto.FactIdType_Instance,
			ElementType:        factStoreProto.ElementType_NodeType,
			FactName:           instFactName,
			FactKey:            extractFactKeyFromName(instFactName),
			NormalizedFactName: instFactName,
		})

		// Instance level fact with labels: c|inst|ns|svc|t + labels
		if hasLabels {
			instLabelPairs := append(append([][2]string{}, instBasePairs...), labelPairs...)
			instLabelFactName := buildSortedFactNameFromPairs(instLabelPairs)
			instLabelFactId := utils.GenerateUUIDV5(instLabelFactName)
			aggDetails = append(aggDetails, &factStoreProto.FactDetail{
				Id:                 instLabelFactId.String(),
				FactIdType:         factStoreProto.FactIdType_Instance,
				ElementType:        factStoreProto.ElementType_NodeType,
				FactName:           instLabelFactName,
				FactKey:            extractFactKeyFromName(instLabelFactName),
				NormalizedFactName: instLabelFactName,
			})
		}
	}

	// Node level fact: c|node|t
	if nodeIp != Unknown {
		nodeBasePairs := [][2]string{
			{"t", tapKind},
			{"c", cluster},
			{"node", nodeIp},
		}
		nodeFactName := buildSortedFactNameFromPairs(nodeBasePairs)
		nodeFactId := utils.GenerateUUIDV5(nodeFactName)
		aggDetails = append(aggDetails, &factStoreProto.FactDetail{
			Id:                 nodeFactId.String(),
			FactIdType:         factStoreProto.FactIdType_Node,
			ElementType:        factStoreProto.ElementType_NodeType,
			FactName:           nodeFactName,
			FactKey:            extractFactKeyFromName(nodeFactName),
			NormalizedFactName: nodeFactName,
		})

		// Node level fact with labels: c|node|t + labels
		if hasLabels {
			nodeLabelPairs := append(append([][2]string{}, nodeBasePairs...), labelPairs...)
			nodeLabelFactName := buildSortedFactNameFromPairs(nodeLabelPairs)
			nodeLabelFactId := utils.GenerateUUIDV5(nodeLabelFactName)
			aggDetails = append(aggDetails, &factStoreProto.FactDetail{
				Id:                 nodeLabelFactId.String(),
				FactIdType:         factStoreProto.FactIdType_Node,
				ElementType:        factStoreProto.ElementType_NodeType,
				FactName:           nodeLabelFactName,
				FactKey:            extractFactKeyFromName(nodeLabelFactName),
				NormalizedFactName: nodeLabelFactName,
			})
		}
	}

	// TODO: Add more fan-outs in future (cluster level, global service, etc.)

    // Job + Node level fact: c|job|node|t
    if job != Unknown && nodeIp != Unknown {
        jobNodeBasePairs := [][2]string{
            {"t", tapKind},
            {"c", cluster},
            {"node", nodeIp},
            {"job", job},
        }
        jobNodeFactName := buildSortedFactNameFromPairs(jobNodeBasePairs)
        jobNodeFactId := utils.GenerateUUIDV5(jobNodeFactName)
        aggDetails = append(aggDetails, &factStoreProto.FactDetail{
            Id:                 jobNodeFactId.String(),
            FactIdType:         factStoreProto.FactIdType_JobNode,
            ElementType:        factStoreProto.ElementType_NodeType,
            FactName:           jobNodeFactName,
            FactKey:            extractFactKeyFromName(jobNodeFactName),
            NormalizedFactName: jobNodeFactName,
        })

        if hasLabels {
            jobNodeLabelPairs := append(append([][2]string{}, jobNodeBasePairs...), labelPairs...)
            jobNodeLabelFactName := buildSortedFactNameFromPairs(jobNodeLabelPairs)
            jobNodeLabelFactId := utils.GenerateUUIDV5(jobNodeLabelFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 jobNodeLabelFactId.String(),
                FactIdType:         factStoreProto.FactIdType_JobNode,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           jobNodeLabelFactName,
                FactKey:            extractFactKeyFromName(jobNodeLabelFactName),
                NormalizedFactName: jobNodeLabelFactName,
            })
        }
    }

    // Job level fact: c|inst|job|ns|svc|t
    if job != Unknown && podName != Unknown {
        jobBasePairs := [][2]string{
            {"t", tapKind},
            {"c", cluster},
            {"ns", namespace},
            {"svc", service},
            {"inst", podName},
            {"job", job},
        }
        jobFactName := buildSortedFactNameFromPairs(jobBasePairs)
        jobFactId := utils.GenerateUUIDV5(jobFactName)
        aggDetails = append(aggDetails, &factStoreProto.FactDetail{
            Id:                 jobFactId.String(),
            FactIdType:         factStoreProto.FactIdType_Job,
            ElementType:        factStoreProto.ElementType_NodeType,
            FactName:           jobFactName,
            FactKey:            extractFactKeyFromName(jobFactName),
            NormalizedFactName: jobFactName,
        })

        if hasLabels {
            jobLabelPairs := append(append([][2]string{}, jobBasePairs...), labelPairs...)
            jobLabelFactName := buildSortedFactNameFromPairs(jobLabelPairs)
            jobLabelFactId := utils.GenerateUUIDV5(jobLabelFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 jobLabelFactId.String(),
                FactIdType:         factStoreProto.FactIdType_Job,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           jobLabelFactName,
                FactKey:            extractFactKeyFromName(jobLabelFactName),
                NormalizedFactName: jobLabelFactName,
            })
        }

        // Job + Kind level fact: c|inst|job|kind|ns|svc|t
        if kind != Unknown {
            jobKindBasePairs := [][2]string{
                {"t", tapKind},
                {"c", cluster},
                {"ns", namespace},
                {"svc", service},
                {"inst", podName},
                {"job", job},
                {"kind", kind},
            }
            jobKindFactName := buildSortedFactNameFromPairs(jobKindBasePairs)
            jobKindFactId := utils.GenerateUUIDV5(jobKindFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 jobKindFactId.String(),
                FactIdType:         factStoreProto.FactIdType_JobKind,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           jobKindFactName,
                FactKey:            extractFactKeyFromName(jobKindFactName),
                NormalizedFactName: jobKindFactName,
            })

            if hasLabels {
                jobKindLabelPairs := append(append([][2]string{}, jobKindBasePairs...), labelPairs...)
                jobKindLabelFactName := buildSortedFactNameFromPairs(jobKindLabelPairs)
                jobKindLabelFactId := utils.GenerateUUIDV5(jobKindLabelFactName)
                aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                    Id:                 jobKindLabelFactId.String(),
                    FactIdType:         factStoreProto.FactIdType_JobKind,
                    ElementType:        factStoreProto.ElementType_NodeType,
                    FactName:           jobKindLabelFactName,
                    FactKey:            extractFactKeyFromName(jobKindLabelFactName),
                    NormalizedFactName: jobKindLabelFactName,
                })
            }
        }

        // Job + Condition level fact: c|cond|inst|job|ns|svc|t
        if condition != Unknown {
            jobCondBasePairs := [][2]string{
                {"t", tapKind},
                {"c", cluster},
                {"ns", namespace},
                {"svc", service},
                {"inst", podName},
                {"job", job},
                {"cond", condition},
            }
            jobCondFactName := buildSortedFactNameFromPairs(jobCondBasePairs)
            jobCondFactId := utils.GenerateUUIDV5(jobCondFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 jobCondFactId.String(),
                FactIdType:         factStoreProto.FactIdType_JobCondition,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           jobCondFactName,
                FactKey:            extractFactKeyFromName(jobCondFactName),
                NormalizedFactName: jobCondFactName,
            })

            if hasLabels {
                jobCondLabelPairs := append(append([][2]string{}, jobCondBasePairs...), labelPairs...)
                jobCondLabelFactName := buildSortedFactNameFromPairs(jobCondLabelPairs)
                jobCondLabelFactId := utils.GenerateUUIDV5(jobCondLabelFactName)
                aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                    Id:                 jobCondLabelFactId.String(),
                    FactIdType:         factStoreProto.FactIdType_JobCondition,
                    ElementType:        factStoreProto.ElementType_NodeType,
                    FactName:           jobCondLabelFactName,
                    FactKey:            extractFactKeyFromName(jobCondLabelFactName),
                    NormalizedFactName: jobCondLabelFactName,
                })
            }
        }

        // Job + Kind + Condition level fact: c|cond|inst|job|kind|ns|svc|t
        if kind != Unknown && condition != Unknown {
            jobKindCondBasePairs := [][2]string{
                {"t", tapKind},
                {"c", cluster},
                {"ns", namespace},
                {"svc", service},
                {"inst", podName},
                {"job", job},
                {"kind", kind},
                {"cond", condition},
            }
            jobKindCondFactName := buildSortedFactNameFromPairs(jobKindCondBasePairs)
            jobKindCondFactId := utils.GenerateUUIDV5(jobKindCondFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 jobKindCondFactId.String(),
                FactIdType:         factStoreProto.FactIdType_JobKindCondition,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           jobKindCondFactName,
                FactKey:            extractFactKeyFromName(jobKindCondFactName),
                NormalizedFactName: jobKindCondFactName,
            })

            if hasLabels {
                jobKindCondLabelPairs := append(append([][2]string{}, jobKindCondBasePairs...), labelPairs...)
                jobKindCondLabelFactName := buildSortedFactNameFromPairs(jobKindCondLabelPairs)
                jobKindCondLabelFactId := utils.GenerateUUIDV5(jobKindCondLabelFactName)
                aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                    Id:                 jobKindCondLabelFactId.String(),
                    FactIdType:         factStoreProto.FactIdType_JobKindCondition,
                    ElementType:        factStoreProto.ElementType_NodeType,
                    FactName:           jobKindCondLabelFactName,
                    FactKey:            extractFactKeyFromName(jobKindCondLabelFactName),
                    NormalizedFactName: jobKindCondLabelFactName,
                })
            }
        }
    }

    // Instance-Nested Facts
    if podName != Unknown {
        // Service + Instance + Kind level fact: c|inst|kind|ns|svc|t
        if kind != Unknown {
            instKindBasePairs := [][2]string{
                {"t", tapKind},
                {"c", cluster},
                {"ns", namespace},
                {"svc", service},
                {"inst", podName},
                {"kind", kind},
            }
            instKindFactName := buildSortedFactNameFromPairs(instKindBasePairs)
            instKindFactId := utils.GenerateUUIDV5(instKindFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 instKindFactId.String(),
                FactIdType:         factStoreProto.FactIdType_InstanceKind,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           instKindFactName,
                FactKey:            extractFactKeyFromName(instKindFactName),
                NormalizedFactName: instKindFactName,
            })

            if hasLabels {
                instKindLabelPairs := append(append([][2]string{}, instKindBasePairs...), labelPairs...)
                instKindLabelFactName := buildSortedFactNameFromPairs(instKindLabelPairs)
                instKindLabelFactId := utils.GenerateUUIDV5(instKindLabelFactName)
                aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                    Id:                 instKindLabelFactId.String(),
                    FactIdType:         factStoreProto.FactIdType_InstanceKind,
                    ElementType:        factStoreProto.ElementType_NodeType,
                    FactName:           instKindLabelFactName,
                    FactKey:            extractFactKeyFromName(instKindLabelFactName),
                    NormalizedFactName: instKindLabelFactName,
                })
            }
        }

        // Service + Instance + Condition level fact: c|cond|inst|ns|svc|t
        if condition != Unknown {
            instCondBasePairs := [][2]string{
                {"t", tapKind},
                {"c", cluster},
                {"ns", namespace},
                {"svc", service},
                {"inst", podName},
                {"cond", condition},
            }
            instCondFactName := buildSortedFactNameFromPairs(instCondBasePairs)
            instCondFactId := utils.GenerateUUIDV5(instCondFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 instCondFactId.String(),
                FactIdType:         factStoreProto.FactIdType_InstanceCondition,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           instCondFactName,
                FactKey:            extractFactKeyFromName(instCondFactName),
                NormalizedFactName: instCondFactName,
            })

            if hasLabels {
                instCondLabelPairs := append(append([][2]string{}, instCondBasePairs...), labelPairs...)
                instCondLabelFactName := buildSortedFactNameFromPairs(instCondLabelPairs)
                instCondLabelFactId := utils.GenerateUUIDV5(instCondLabelFactName)
                aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                    Id:                 instCondLabelFactId.String(),
                    FactIdType:         factStoreProto.FactIdType_InstanceCondition,
                    ElementType:        factStoreProto.ElementType_NodeType,
                    FactName:           instCondLabelFactName,
                    FactKey:            extractFactKeyFromName(instCondLabelFactName),
                    NormalizedFactName: instCondLabelFactName,
                })
            }
        }

        // Service + Instance + Kind + Condition level fact: c|cond|inst|kind|ns|svc|t
        if kind != Unknown && condition != Unknown {
            instKindCondBasePairs := [][2]string{
                {"t", tapKind},
                {"c", cluster},
                {"ns", namespace},
                {"svc", service},
                {"inst", podName},
                {"kind", kind},
                {"cond", condition},
            }
            instKindCondFactName := buildSortedFactNameFromPairs(instKindCondBasePairs)
            instKindCondFactId := utils.GenerateUUIDV5(instKindCondFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 instKindCondFactId.String(),
                FactIdType:         factStoreProto.FactIdType_InstanceKindCondition,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           instKindCondFactName,
                FactKey:            extractFactKeyFromName(instKindCondFactName),
                NormalizedFactName: instKindCondFactName,
            })

            if hasLabels {
                instKindCondLabelPairs := append(append([][2]string{}, instKindCondBasePairs...), labelPairs...)
                instKindCondLabelFactName := buildSortedFactNameFromPairs(instKindCondLabelPairs)
                instKindCondLabelFactId := utils.GenerateUUIDV5(instKindCondLabelFactName)
                aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                    Id:                 instKindCondLabelFactId.String(),
                    FactIdType:         factStoreProto.FactIdType_InstanceKindCondition,
                    ElementType:        factStoreProto.ElementType_NodeType,
                    FactName:           instKindCondLabelFactName,
                    FactKey:            extractFactKeyFromName(instKindCondLabelFactName),
                    NormalizedFactName: instKindCondLabelFactName,
                })
            }
        }
    }

    // Kind level fact: c|kind|ns|t
    if kind != Unknown {
        kindBasePairs := [][2]string{
            {"t", tapKind},
            {"c", cluster},
            {"ns", namespace},
            {"kind", kind},
        }
        kindFactName := buildSortedFactNameFromPairs(kindBasePairs)
        kindFactId := utils.GenerateUUIDV5(kindFactName)
        aggDetails = append(aggDetails, &factStoreProto.FactDetail{
            Id:                 kindFactId.String(),
            FactIdType:         factStoreProto.FactIdType_Kind,
            ElementType:        factStoreProto.ElementType_NodeType,
            FactName:           kindFactName,
            FactKey:            extractFactKeyFromName(kindFactName),
            NormalizedFactName: kindFactName,
        })

        if hasLabels {
            kindLabelPairs := append(append([][2]string{}, kindBasePairs...), labelPairs...)
            kindLabelFactName := buildSortedFactNameFromPairs(kindLabelPairs)
            kindLabelFactId := utils.GenerateUUIDV5(kindLabelFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 kindLabelFactId.String(),
                FactIdType:         factStoreProto.FactIdType_Kind,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           kindLabelFactName,
                FactKey:            extractFactKeyFromName(kindLabelFactName),
                NormalizedFactName: kindLabelFactName,
            })
        }
    }

    // Condition level fact: c|cond|ns|t
    if condition != Unknown {
        condBasePairs := [][2]string{
            {"t", tapKind},
            {"c", cluster},
            {"ns", namespace},
            {"cond", condition},
        }
        condFactName := buildSortedFactNameFromPairs(condBasePairs)
        condFactId := utils.GenerateUUIDV5(condFactName)
        aggDetails = append(aggDetails, &factStoreProto.FactDetail{
            Id:                 condFactId.String(),
            FactIdType:         factStoreProto.FactIdType_Condition,
            ElementType:        factStoreProto.ElementType_NodeType,
            FactName:           condFactName,
            FactKey:            extractFactKeyFromName(condFactName),
            NormalizedFactName: condFactName,
        })

        if hasLabels {
            condLabelPairs := append(append([][2]string{}, condBasePairs...), labelPairs...)
            condLabelFactName := buildSortedFactNameFromPairs(condLabelPairs)
            condLabelFactId := utils.GenerateUUIDV5(condLabelFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 condLabelFactId.String(),
                FactIdType:         factStoreProto.FactIdType_Condition,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           condLabelFactName,
                FactKey:            extractFactKeyFromName(condLabelFactName),
                NormalizedFactName: condLabelFactName,
            })
        }
    }

    // Kind + Condition level fact: c|cond|kind|ns|t
    if kind != Unknown && condition != Unknown {
        kindCondBasePairs := [][2]string{
            {"t", tapKind},
            {"c", cluster},
            {"ns", namespace},
            {"kind", kind},
            {"cond", condition},
        }
        kindCondFactName := buildSortedFactNameFromPairs(kindCondBasePairs)
        kindCondFactId := utils.GenerateUUIDV5(kindCondFactName)
        aggDetails = append(aggDetails, &factStoreProto.FactDetail{
            Id:                 kindCondFactId.String(),
            FactIdType:         factStoreProto.FactIdType_KindCondition,
            ElementType:        factStoreProto.ElementType_NodeType,
            FactName:           kindCondFactName,
            FactKey:            extractFactKeyFromName(kindCondFactName),
            NormalizedFactName: kindCondFactName,
        })

        if hasLabels {
            kindCondLabelPairs := append(append([][2]string{}, kindCondBasePairs...), labelPairs...)
            kindCondLabelFactName := buildSortedFactNameFromPairs(kindCondLabelPairs)
            kindCondLabelFactId := utils.GenerateUUIDV5(kindCondLabelFactName)
            aggDetails = append(aggDetails, &factStoreProto.FactDetail{
                Id:                 kindCondLabelFactId.String(),
                FactIdType:         factStoreProto.FactIdType_KindCondition,
                ElementType:        factStoreProto.ElementType_NodeType,
                FactName:           kindCondLabelFactName,
                FactKey:            extractFactKeyFromName(kindCondLabelFactName),
                NormalizedFactName: kindCondLabelFactName,
            })
        }
    }

    return aggDetails
}

// buildSortedFactNameFromPairs builds a sorted fact_name from key-value pairs
func buildSortedFactNameFromPairs(pairs [][2]string) string {
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i][0] < pairs[j][0]
	})
	parts := make([]string, len(pairs))
	for i, pair := range pairs {
		parts[i] = fmt.Sprintf("%s=%s", pair[0], pair[1])
	}
	return strings.Join(parts, "|")
}

func (f *FactStore) testFactMetric(factId *FsFactId) error {

	//metric_value: Some(MetricValue {
	//	metric_inner_value: Some(MetricInnerValue::IntValue(val)),
	//}),
	value := int64(100)
	factIdStr := factId.ID.String()
	appMetric := factStoreProto.Metric{
		Name:       "metric1", // Todo: Metric Name
		MetricType: factStoreProto.MetricType_Gauge,
		MetricValue: &factStoreProto.MetricValue{
			MetricInnerValue: &factStoreProto.MetricValue_IntValue{
				IntValue: value,
			},
		},
	}

	// Create raw fact detail for the test metric
	rawFactDetail := &factStoreProto.FactDetail{
		FactIdType:  factId.FactType,
		ElementType: factStoreProto.ElementType_NodeType,
		FactName:    "test|" + factIdStr,
		FactKey:     factIdStr,
	}

	// Simple label data for testing
	labelData := &factStoreProto.LabelData{
		Labels: map[string]string{"test": "true"},
	}

	fsMetrics := factStoreProto.FactMetrics{
		// time in seconds
		Timestamp:      uint32(time.Now().Unix()),
		Metrics:        []*factStoreProto.Metric{&appMetric},
		MetricSource:   factStoreProto.MetricSource_METRIC,
		LabelData:      &factStoreProto.FactMetrics_Labels{Labels: labelData},
		RawFactDetail:  rawFactDetail,
		AggFactDetails: []*factStoreProto.FactDetail{},
	}

	msg := factStoreProto.FactMetricsBatch{
		Header: &factStoreProto.Header{
			Version: 1,
		},
		FactMetrics: []*factStoreProto.FactMetrics{&fsMetrics},
	}

	err := f.fsKafkaProducer.send(factIdStr, &msg)
	return err

}

func (f *FactStore) Close() error {
	logger.Info("Closing FactStore")
	f.isClosed.Store(true)
	f.fsKafkaProducer.Close()
	logger.Info("FactStore closed.")
	return nil
}

func (f *FactStore) ProcessInput(input *integrationconfig.MetricDetailPlatformData) {
	if input == nil {
		return
	}

	if input.MetricSource == factStoreProto.MetricSource_METRIC_SOURCE_UNSPECIFIED {
		input.MetricSource = factStoreProto.MetricSource_METRIC
	}

	if !(input.AggregationType == integrationconfig.GAUGE || input.AggregationType == integrationconfig.HISTOGRAM || input.AggregationType == integrationconfig.COUNT || input.AggregationType == integrationconfig.COUNTER) {
		// Todo: for latency support rrt for latency
		return
	}

	if input.AggregationType == integrationconfig.HISTOGRAM {
		if input.HistogramValues.Count == 0 || input.HistogramValues.Sum == 0 {
			// This is invalid histogram data
			//logger.Errorf("Invalid histogram data. Count and Sum cannot be zero for %+v", input)
			return
		}
	}

	// Todo: Pending for VM/External and instance as IP
	// Todo: Add Protocol or type as well. If redis / sql / Kafka / monogdb in meta data
	// Todo: Add port as well - in rust also

	tapKind := getTapKind(input)

	// At node level we need to add tags/labels
	// Handle Node(K8s Pod Node or VM Nodes) type
	if input.ResourceType == integrationconfig.KUBERNETES_NODE || input.ResourceType == integrationconfig.VM_NODE {
		if err := f.ProcessInputForNode(input); err != nil {
			log.Errorf("Failed to send fact metric for node : %s", err)
		}
		return
	}

	if err := f.ProcessInputForInstance(input, tapKind); err != nil {
		log.Errorf("Failed to send fact metric for instance : %s", err)
	}

}

func (f *FactStore) ProcessInputForInstance(input *integrationconfig.MetricDetailPlatformData, tapKind string) error {
	instanceBaseFactId, serviceBaseFactId, meta, err := buildBaseFacts(input, tapKind)
	if err != nil {
		return err
	}
	f.SendFactId(serviceBaseFactId)
	f.SendFactId(instanceBaseFactId)

	labelsFactId, err := buildLabelsFact(input, meta, instanceBaseFactId, LabelsFactTypeContextInstance)
	if err != nil {
		return err
	}

	var factIdToSend *FsFactId
	if labelsFactId == nil {
		factIdToSend = instanceBaseFactId
	} else {
		f.SendFactId(labelsFactId)
		factIdToSend = labelsFactId
	}

	if factIdToSend == nil {
		return nil
	}

	err = f.sendFactMetric(factIdToSend, input, meta)
	return err
}

func (f *FactStore) ProcessInputForNode(input *integrationconfig.MetricDetailPlatformData) error {
	tapKind := getTapKindForNode(input.ResourceType)
	nodeFactId, meta, err := buildNodeFact(input, tapKind)

	if err != nil {
		newErr := fmt.Errorf("failed to build node fact id for metric %s, resource type %s: %w", input.MetricName, integrationconfig.ResourceTypeString[input.ResourceType], err)
		return newErr
	}

	f.SendFactId(nodeFactId)

	labelsFactId, err := buildLabelsFact(input, meta, nodeFactId, LabelsFactTypeContextNode)
	if err != nil {
		return err
	}
	var factIdToSend *FsFactId

	if labelsFactId == nil {
		factIdToSend = nodeFactId
	} else {
		f.SendFactId(labelsFactId)
		factIdToSend = labelsFactId
	}

	if factIdToSend == nil {
		return nil
	}

	return f.sendFactMetric(factIdToSend, input, meta)
}

type FsFactId struct {
	ID        uuid.UUID
	FactType  factStoreProto.FactIdType
	FactName  string
	Meta      json.RawMessage
	CreatedAt time.Time
}

func buildBaseFacts(input *integrationconfig.MetricDetailPlatformData, tapKind string) (*FsFactId, *FsFactId, map[string]interface{}, error) {
	cluster := utils.DefaultIfEmptyStr(input.Cluster, Unknown)
	namespace := utils.DefaultIfEmptyStr(input.Namespace, Unknown)
	service := utils.DefaultIfEmptyStr(input.PodGroup, Unknown)
	podName := utils.DefaultIfEmptyStr(input.PodName, Unknown)

	// create base fact_id
	baseSrc := fmt.Sprintf(
		"t=%s|c=%s|ns=%s|svc=%s|inst=%s",
		tapKind,
		cluster,
		namespace,
		service,
		podName,
	)
	baseFactId := utils.GenerateUUIDV5(baseSrc)

	svcName := fmt.Sprintf(
		"t=%s|c=%s|ns=%s|svc=%s",
		tapKind,
		cluster,
		namespace,
		service,
	)
	svcFactId := utils.GenerateUUIDV5(svcName)
	serviceMeta := map[string]interface{}{
		"tap_side_kind":        tapKind,
		"cluster":              input.Cluster,
		"namespace":            input.Namespace,
		"service_name":         input.PodGroup,
		"pod_group_name":       input.PodGroup,
		"sample_metric":        input.MetricName,
		"service_base_context": svcFactId.String(),
	}
	serviceMetaJSON, err := json.Marshal(serviceMeta)
	if err != nil {
		logger.Errorf("Failed to marshal meta data for service fact: %v", err)
		return nil, nil, nil, err
	}

	meta := map[string]interface{}{
		"tap_side_kind":        tapKind,
		"cluster":              input.Cluster,
		"namespace":            input.Namespace,
		"service_name":         input.PodGroup,
		"node_name":            input.NodeName,
		"pod_group_name":       input.PodGroup,
		"pod_name":             input.PodName,
		"ip4":                  input.PodIp,
		"tags":                 convertLabelValueListToMap(input.CommonLabelValueList),
		"sample_metric":        input.MetricName,
		"service_base_context": svcFactId.String(),
		"node_ip":              input.NodeIp,
	}

	if input.Port != "" {
		meta["port"] = input.Port
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		logger.Errorf("Failed to marshal meta data: %v", err)
		return nil, nil, nil, err
	}

	instanceFactId := FsFactId{
		ID:        baseFactId,
		FactType:  factStoreProto.FactIdType_Instance,
		FactName:  baseSrc,
		Meta:      json.RawMessage(metaJSON),
		CreatedAt: time.Now(),
	}

	serviceFactId := FsFactId{
		ID:        svcFactId,
		FactType:  factStoreProto.FactIdType_Service,
		FactName:  svcName,
		Meta:      json.RawMessage(serviceMetaJSON),
		CreatedAt: time.Now(),
	}

	return &instanceFactId, &serviceFactId, meta, nil
}

func buildNodeFact(input *integrationconfig.MetricDetailPlatformData, tapKind string) (*FsFactId, map[string]interface{}, error) {
	cluster := utils.DefaultIfEmptyStr(input.Cluster, Unknown)
	if input.NodeIp == "" {
		return nil, nil, fmt.Errorf("node_ip is empty")
	}
	nodeIp := input.NodeIp
	nodeName := utils.DefaultIfEmptyStr(input.NodeName, Unknown)

	nodeSrc := fmt.Sprintf("t=%s|c=%s|node=%s", tapKind, cluster, nodeIp)
	nodeFactId := utils.GenerateUUIDV5(nodeSrc)

	meta := map[string]interface{}{
		"tap_side_kind": tapKind,
		"cluster":       input.Cluster,
		"node_name":     nodeName,
		"node_ip":       nodeIp,
		"tags":          convertLabelValueListToMap(input.CommonLabelValueList),
		"sample_metric": input.MetricName,
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		logger.Errorf("Failed to marshal meta data for node fact: %v", err)
		return nil, nil, err
	}

	factId := FsFactId{
		ID:        nodeFactId,
		FactType:  factStoreProto.FactIdType_Node,
		FactName:  nodeSrc,
		Meta:      json.RawMessage(metaJSON),
		CreatedAt: time.Now(),
	}

	return &factId, meta, nil
}

func buildLabelsFact(input *integrationconfig.MetricDetailPlatformData, meta map[string]interface{}, base *FsFactId, labelsFactTypeContext LabelsFactTypeContext) (*FsFactId, error) {
	if len(input.LabelValueList) == 0 {
		return nil, nil
	}

	stableSortLabelValueList(input.LabelValueList)
	labelsFactNamePartial := createLabelsFactName(input.LabelValueList)

	// Todo DEEPAK: Send it as diff field
	kubeContainerName := getLabelValueFromCommon(input, "kube_container_name", labelsFactTypeContext)
	labelsFactName := buildLabelFactName(kubeContainerName, base, labelsFactNamePartial, labelsFactTypeContext)

	if labelsFactName == "" {
		return nil, fmt.Errorf("labelsFactName is empty")
	}

	labelsFactId := utils.GenerateUUIDV5(labelsFactName)

	if labelsFactTypeContext == LabelsFactTypeContextNode {
		meta["node_base_context"] = base.ID.String()
	} else if labelsFactTypeContext == LabelsFactTypeContextInstance {
		meta["instance_base_context"] = base.ID.String()
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		logger.Errorf("Failed to marshal meta data for labels: %v", err)
		return nil, err
	}

	factType := factStoreProto.FactIdType_Instance
	if labelsFactTypeContext == LabelsFactTypeContextNode {
		factType = factStoreProto.FactIdType_Node
	}

	factId := FsFactId{
		ID:        labelsFactId,
		FactType:  factType,
		FactName:  labelsFactName,
		Meta:      json.RawMessage(metaJSON),
		CreatedAt: time.Now(),
	}

	return &factId, nil

}

func buildLabelFactName(kubeContainerName string, base *FsFactId, labelsFactNamePartial string, typeContext LabelsFactTypeContext) string {
	if kubeContainerName == "" || typeContext == LabelsFactTypeContextNode {
		return fmt.Sprintf("%s|%s", base.FactName, labelsFactNamePartial)
	}
	return fmt.Sprintf("%s|kube_container_name=%s|%s", base.FactName, kubeContainerName, labelsFactNamePartial)
}

func getLabelValueFromCommon(input *integrationconfig.MetricDetailPlatformData, labelName string, typeContext LabelsFactTypeContext) string {
	if typeContext == LabelsFactTypeContextNode {
		return ""
	}

	if input.CommonLabelValueList != nil {
		for _, label := range input.CommonLabelValueList {
			if label.Name == labelName {
				return label.Value
			}
		}
	}
	return ""
}

func convertLabelValueListToMap(labelValueList []integrationconfig.MetricLabelValue) map[string]string {
	result := make(map[string]string)
	for _, label := range labelValueList {
		result[label.Name] = label.Value
	}
	return result
}

func stableSortLabelValueList(labelValueList []integrationconfig.MetricLabelValue) {
	sort.SliceStable(labelValueList, func(i, j int) bool {
		return labelValueList[i].Name < labelValueList[j].Name
	})
}

func createLabelsFactName(labelValueList []integrationconfig.MetricLabelValue) string {
	var builder strings.Builder
	for i, label := range labelValueList {
		builder.WriteString(label.Name)
		builder.WriteString("=")
		builder.WriteString(label.Value)
		if i < len(labelValueList)-1 {
			builder.WriteString("|")
		}
	}
	return builder.String()
}

func getTapKind(input *integrationconfig.MetricDetailPlatformData) string {
	switch input.Type {
	case integrationconfig.VM:
		return "vm"
	case integrationconfig.K8S:
		return "k8"
	case integrationconfig.EXTERNAL:
		return "external"
	default:
		return "unknown"
	}
}

func getTapKindForNode(input integrationconfig.ResourceType) string {
	switch input {
	case integrationconfig.KUBERNETES_NODE:
		return "k8"
	case integrationconfig.VM_NODE:
		return "vm"
	default:
		return "unknown"

	}
}

// LabelsFactTypeContext represents the context type for Labels Fact.
type LabelsFactTypeContext int

const (
	LabelsFactTypeContextNode LabelsFactTypeContext = iota
	LabelsFactTypeContextInstance
)
