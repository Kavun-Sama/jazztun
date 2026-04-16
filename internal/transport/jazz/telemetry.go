package jazz

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	telemetryFlushDelay = 1500 * time.Millisecond
	telemetryBatchSize  = 8
)

type clientMetricEvent struct {
	Type   string         `json:"type"`
	Values any            `json:"values,omitempty"`
	Date   int64          `json:"date"`
	Meta   map[string]any `json:"meta,omitempty"`
}

type latencyValue struct {
	Name  string `json:"name"`
	Value int64  `json:"value,omitempty"`
}

type clientMetricsReporter struct {
	api             *APIClient
	log             *slog.Logger
	participantName string
	startedAt       time.Time

	mu            sync.Mutex
	roomID        string
	meetingID     string
	participantID string
	queue         []clientMetricEvent
	flushTimer    *time.Timer
	closed        bool
}

func newClientMetricsReporter(api *APIClient, logger *slog.Logger, participantName, roomID string) *clientMetricsReporter {
	if logger == nil {
		logger = slog.Default()
	}
	r := &clientMetricsReporter{
		api:             api,
		log:             logger.With(slog.String("component", "jazz/telemetry")),
		participantName: participantName,
		roomID:          roomID,
		startedAt:       time.Now(),
	}
	return r
}

func (r *clientMetricsReporter) Close() {
	r.queueEvent("siem.exitConference", nil)

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	timer := r.flushTimer
	r.flushTimer = nil
	r.mu.Unlock()

	if timer != nil {
		timer.Stop()
	}

	r.flushNow()
}

func (r *clientMetricsReporter) SetSession(meetingID, participantID string) {
	r.mu.Lock()
	r.meetingID = meetingID
	r.participantID = participantID
	r.mu.Unlock()
}

func (r *clientMetricsReporter) ReportUserName() {
	r.queueEvent("siem.userName", map[string]any{"name": r.participantName})
}

func (r *clientMetricsReporter) ReportOpenVC() {
	r.queueEvent("firebase.openVc", map[string]any{
		"userRole":   "member",
		"micType":    "Microphone (Realtek(R) Audio)",
		"spType":     "Speakers (Realtek(R) Audio)",
		"camEnabled": false,
		"micEnabled": false,
	})
}

func (r *clientMetricsReporter) ReportOpenConference() {
	r.queueEvent("siem.openConference", map[string]any{
		"userRole":   "member",
		"micType":    "Microphone (Realtek(R) Audio)",
		"spType":     "Speakers (Realtek(R) Audio)",
		"camEnabled": false,
		"micEnabled": false,
	})
}

func (r *clientMetricsReporter) ReportChatOpen() {
	r.queueEvent("firebase.chatOpenChat", nil)
	r.queueEvent("siem.chatButtonClick", nil)
}

func (r *clientMetricsReporter) ReportMicOff() {
	r.queueEvent("deviceMicState", map[string]any{"mode": "off"})
}

func (r *clientMetricsReporter) ReportLatency(name string, value int64) {
	r.queueEvent("client.jn.latency", []latencyValue{{Name: name, Value: value}})
}

func (r *clientMetricsReporter) ReportLatencyGroup(values ...latencyValue) {
	if len(values) == 0 {
		return
	}
	r.queueEvent("client.jn.latency", values)
}

func (r *clientMetricsReporter) ReportMediaSessionStart(iceConnected int64) {
	r.queueEvent("client.mediaSessionStart", map[string]any{
		"transportConnected": 0,
		"roomConnected":      0,
		"mediaSessionTime":   r.elapsed(),
		"iceConnected":       iceConnected,
	})
}

func (r *clientMetricsReporter) ReportWebRTC(values []map[string]any) {
	if len(values) == 0 {
		return
	}
	r.queueEvent("client.jn.webrtc", values)
}

func (r *clientMetricsReporter) queueEvent(eventType string, values any) {
	if r == nil || r.api == nil {
		return
	}

	event := clientMetricEvent{
		Type:   eventType,
		Values: values,
		Date:   time.Now().UnixMilli(),
		Meta:   r.baseMeta(eventType),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}

	r.queue = append(r.queue, event)
	if len(r.queue) >= telemetryBatchSize {
		go r.flushLocked()
		return
	}

	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(telemetryFlushDelay, r.flushNow)
	}
}

func (r *clientMetricsReporter) flushNow() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.flushLocked()
}

func (r *clientMetricsReporter) flushLocked() {
	if len(r.queue) == 0 {
		if r.flushTimer != nil {
			r.flushTimer.Stop()
			r.flushTimer = nil
		}
		return
	}

	batch := append([]clientMetricEvent(nil), r.queue...)
	r.queue = nil
	if r.flushTimer != nil {
		r.flushTimer.Stop()
		r.flushTimer = nil
	}

	go func(events []clientMetricEvent) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if err := r.api.SendClientMetrics(ctx, events); err != nil {
			r.log.Debug("send client metrics", "error", err, "events", len(events))
		}
	}(batch)
}

func (r *clientMetricsReporter) elapsed() int64 {
	if r == nil {
		return 0
	}
	return time.Since(r.startedAt).Milliseconds()
}

func (r *clientMetricsReporter) baseMeta(eventType string) map[string]any {
	r.mu.Lock()
	roomID := r.roomID
	meetingID := r.meetingID
	participantID := r.participantID
	r.mu.Unlock()

	meta := map[string]any{
		"os":                        "Windows NT 10.0",
		"osName":                    "Windows",
		"surface":                   "web",
		"osVersion":                 "NT 10.0",
		"platform":                  "Windows",
		"browser":                   "Firefox",
		"browser_version":           "135.0",
		"version":                   "26.21.7",
		"clientId":                  r.api.ClientID(),
		"userName":                  r.participantName,
		"license_type":              "",
		"project_id":                "",
		"userRole":                  "member",
		"roomId":                    roomID,
		"authType":                  "ANONYMOUS",
		"maxWebinarViewersCapacity": "0",
		"meetingId":                 meetingID,
		"jazzNext":                  "true",
		"participantId":             participantID,
		"conferenceType":            "conference",
	}

	if eventType == "client.jn.latency" || eventType == "client.jn.webrtc" {
		meta["client_name"] = "Jazz"
		meta["client_id"] = r.api.ClientID()
		meta["room_id"] = roomID
		meta["meeting_id"] = meetingID
		meta["participant_id"] = participantID
	}

	return meta
}

func marshalClientMetrics(events []clientMetricEvent) ([]byte, error) {
	lines := make([][]byte, 0, len(events))
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			return nil, fmt.Errorf("marshal client metric %q: %w", event.Type, err)
		}
		lines = append(lines, line)
	}
	return []byte(joinLines(lines)), nil
}

func joinLines(lines [][]byte) string {
	if len(lines) == 0 {
		return ""
	}
	size := 0
	for _, line := range lines {
		size += len(line) + 1
	}
	buf := make([]byte, 0, size-1)
	for i, line := range lines {
		if i > 0 {
			buf = append(buf, '\n')
		}
		buf = append(buf, line...)
	}
	return string(buf)
}
