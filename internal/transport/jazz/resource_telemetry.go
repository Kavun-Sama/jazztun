package jazz

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	announcementURL           = "https://salutejazz.ru/.well-known/announcement.json"
	metricsPromResourceURL    = "https://metrics.prom.third-party-app.sberdevices.ru/jazz-s2b-app-web-resources"
	clickbeatResourceURL      = "https://clickbeat.sberdevices.ru/amplitude/telemetry/jazz-s2b-app-web-resources"
	resourceTelemetryDelay    = 3 * time.Second
	resourceTelemetryInterval = 5 * time.Minute
)

type resourceMetric struct {
	SessionID             string `json:"sessionId"`
	Hostname              string `json:"hostname"`
	Project               string `json:"project"`
	Path                  string `json:"path"`
	Name                  string `json:"name"`
	EntryType             string `json:"entryType"`
	StartTime             int64  `json:"startTime"`
	Duration              int64  `json:"duration"`
	InitiatorType         string `json:"initiatorType"`
	NextHopProtocol       string `json:"nextHopProtocol"`
	FetchStart            int64  `json:"fetchStart"`
	DomainLookupStart     int64  `json:"domainLookupStart"`
	DomainLookupEnd       int64  `json:"domainLookupEnd"`
	ConnectStart          int64  `json:"connectStart"`
	ConnectEnd            int64  `json:"connectEnd"`
	SecureConnectionStart int64  `json:"secureConnectionStart"`
	RequestStart          int64  `json:"requestStart"`
	ResponseStart         int64  `json:"responseStart"`
	ResponseEnd           int64  `json:"responseEnd"`
	TransferSize          int64  `json:"transferSize"`
	EncodedBodySize       int64  `json:"encodedBodySize"`
	DecodedBodySize       int64  `json:"decodedBodySize"`
	Metadata              string `json:"metadata"`
}

type browserResourceReporter struct {
	api       *APIClient
	log       *slog.Logger
	roomURL   string
	sessionID string
	startedAt time.Time
	stopCh    chan struct{}
	cookieA   string
	cookieB   string
}

func newBrowserResourceReporter(api *APIClient, logger *slog.Logger, roomURL string) *browserResourceReporter {
	if logger == nil {
		logger = slog.Default()
	}
	r := &browserResourceReporter{
		api:       api,
		log:       logger.With(slog.String("component", "jazz/resource-telemetry")),
		roomURL:   roomURL,
		sessionID: uuid.New().String(),
		startedAt: time.Now(),
		stopCh:    make(chan struct{}),
		cookieA:   randomHex(16),
		cookieB:   randomHex(16),
	}
	go r.loop()
	return r
}

func (r *browserResourceReporter) Close() {
	if r == nil {
		return
	}
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
}

func (r *browserResourceReporter) loop() {
	timer := time.NewTimer(resourceTelemetryDelay)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			r.reportOnce()
			timer.Reset(resourceTelemetryInterval)
		case <-r.stopCh:
			return
		}
	}
}

func (r *browserResourceReporter) reportOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	metric, err := r.fetchAnnouncement(ctx)
	if err != nil {
		r.log.Debug("fetch announcement", "error", err)
		return
	}
	if err := r.api.SendPromResourceMetric(ctx, metric); err != nil {
		r.log.Debug("send prom resource metric", "error", err)
	}
	if err := r.api.SendClickbeatResourceMetric(ctx, metric, r.cookieA, r.cookieB); err != nil {
		r.log.Debug("send clickbeat resource metric", "error", err)
	}
}

func (r *browserResourceReporter) fetchAnnouncement(ctx context.Context) (*resourceMetric, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", announcementURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Referer", r.roomURL)
	req.Header.Set("DNT", "1")
	req.Header.Set("Sec-GPC", "1")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")

	resp, err := r.api.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Milliseconds()
	startMs := time.Since(r.startedAt).Milliseconds()
	if startMs < 0 {
		startMs = 0
	}
	encoded := int64(len(body))
	transfer := encoded + 300
	responseStart := startMs + max(1, elapsed/2)
	responseEnd := startMs + max(1, elapsed)

	return &resourceMetric{
		SessionID:             r.sessionID,
		Hostname:              "salutejazz.ru",
		Project:               "jazz-s2b-app-web",
		Path:                  r.roomURL,
		Name:                  announcementURL,
		EntryType:             "resource",
		StartTime:             startMs,
		Duration:              max(1, elapsed),
		InitiatorType:         "fetch",
		NextHopProtocol:       "h2",
		FetchStart:            startMs,
		DomainLookupStart:     startMs + 1,
		DomainLookupEnd:       startMs + 1,
		ConnectStart:          startMs + 1,
		ConnectEnd:            startMs + 1,
		SecureConnectionStart: startMs + 1,
		RequestStart:          startMs + 1,
		ResponseStart:         responseStart,
		ResponseEnd:           responseEnd,
		TransferSize:          transfer,
		EncodedBodySize:       encoded,
		DecodedBodySize:       encoded,
		Metadata:              "{}",
	}, nil
}

func (c *APIClient) SendPromResourceMetric(ctx context.Context, metric *resourceMetric) error {
	return c.sendThirdPartyMetric(ctx, metricsPromResourceURL, metric, "text/plain;charset=UTF-8", false, "", "")
}

func (c *APIClient) SendClickbeatResourceMetric(ctx context.Context, metric *resourceMetric, cookieA, cookieB string) error {
	return c.sendThirdPartyMetric(ctx, clickbeatResourceURL, metric, "application/json", true, cookieA, cookieB)
}

func (c *APIClient) sendThirdPartyMetric(ctx context.Context, target string, metric *resourceMetric, contentType string, withCookies bool, cookieA, cookieB string) error {
	payload, err := json.Marshal([]*resourceMetric{metric})
	if err != nil {
		return fmt.Errorf("marshal resource metric: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Origin", origin)
	req.Header.Set("Referer", origin+"/")
	req.Header.Set("DNT", "1")
	req.Header.Set("Sec-GPC", "1")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	if withCookies {
		req.Header.Set("Cookie",
			fmt.Sprintf("e97163bb34ffae5c26a70011050a367f=%s; ecfe106684f479d674c0510065cafd9d=%s", cookieA, cookieB))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("send third-party metric: %w", err)
	}
	defer resp.Body.Close()

	if target == clickbeatResourceURL {
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("clickbeat status %d: %s", resp.StatusCode, body)
		}
		return nil
	}
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("prom status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func randomHex(size int) string {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return uuid.New().String()
	}
	return hex.EncodeToString(raw)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
