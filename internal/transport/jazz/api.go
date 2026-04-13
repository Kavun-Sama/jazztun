package jazz

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

const (
	baseURL   = "https://bk.salutejazz.ru"
	origin    = "https://salutejazz.ru"
	userAgent = "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0"
	jazzUA    = "osName=Linux;osVersion=6.1;appName=jazz;appVersion=26.21.7;surface=WEB;browserName=Firefox;browserVersion=135.0"
)

// APIClient handles Jazz REST API calls.
type APIClient struct {
	clientID string
	http     *http.Client
	log      *slog.Logger
}

// NewAPIClient creates a new Jazz API client.
func NewAPIClient(logger *slog.Logger) *APIClient {
	if logger == nil {
		logger = slog.Default()
	}
	return &APIClient{
		clientID: uuid.New().String(),
		http:     &http.Client{},
		log:      logger.With(slog.String("component", "jazz/api")),
	}
}

// CreateRoomResponse is the response from create-meeting.
type CreateRoomResponse struct {
	RoomID   string `json:"roomId"`
	Password string `json:"password"`
	Domain   string `json:"domain"`
	URL      string `json:"url"`
}

// PreconnectResponse is the response from preconnect.
type PreconnectResponse struct {
	ConnectorURL    string `json:"connectorUrl"`
	RoomTitle       string `json:"roomTitle"`
	RoomType        string `json:"roomType"`
	ParticipantRole string `json:"participantRole"`
	Permissions     struct {
		CanEditOwnName bool `json:"canEditOwnName"`
		CanShareMicro  bool `json:"canShareMicrophone"`
		CanShareCamera bool `json:"canShareCamera"`
	} `json:"preconnectPermissions"`
}

// CreateRoom creates a new Jazz meeting room.
func (c *APIClient) CreateRoom() (*CreateRoomResponse, error) {
	body := map[string]any{
		"title":                               "Video meeting",
		"guestEnabled":                        true,
		"lobbyEnabled":                        false,
		"serverVideoRecordAutoStartEnabled":   false,
		"sipEnabled":                          false,
		"moderatorEmails":                     []string{},
		"summarizationEnabled":                false,
		"room3dEnabled":                       false,
		"room3dScene":                         "XRLobby",
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", baseURL+"/room/create-meeting", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create room request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("create room: status %d: %s", resp.StatusCode, respBody)
	}

	var result CreateRoomResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	c.log.Info("room created", "roomId", result.RoomID, "url", result.URL)
	return &result, nil
}

// Preconnect gets the WebSocket connector URL for a room.
func (c *APIClient) Preconnect(roomID, password string) (*PreconnectResponse, error) {
	body := map[string]any{
		"password": password,
		"jazzNextMigration": map[string]any{
			"b2bBaseRoomSupport":                true,
			"demoRoomBaseSupport":               true,
			"demoRoomVersionSupport":            2,
			"mediaWithoutAutoSubscribeSupport":  true,
			"webinarSpeakerSupport":             true,
			"webinarViewerSupport":              true,
			"sdkRoomSupport":                    true,
			"sberclassRoomSupport":              true,
		},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", baseURL+"/room/"+roomID+"/preconnect", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("preconnect request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("preconnect: status %d: %s", resp.StatusCode, respBody)
	}

	var result PreconnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	c.log.Info("preconnect ok", "connectorUrl", result.ConnectorURL)
	return &result, nil
}

// setHeaders sets the required Jazz API headers on a request.
func (c *APIClient) setHeaders(req *http.Request) {
	req.Header.Set("X-Jazz-AuthType", "ANONYMOUS")
	req.Header.Set("X-Client-AuthType", "ANONYMOUS")
	req.Header.Set("X-Jazz-ClientId", c.clientID)
	req.Header.Set("X-Jazz-Token", "")
	req.Header.Set("X-Jazz-UA", jazzUA)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Origin", origin)
	req.Header.Set("Referer", origin+"/")
	req.Header.Set("Content-Type", "application/json")
}

// ParseRoomURL parses a Jazz invite URL into roomID and password.
// Accepts formats:
//   - https://salutejazz.ru/{roomId}?psw={encoded}
//   - {roomId} (just the room ID)
func ParseRoomURL(rawURL string) (roomID, password string, err error) {
	// If it's just a room ID (no slashes, no ://)
	if !strings.Contains(rawURL, "/") && !strings.Contains(rawURL, ":") {
		return rawURL, "", nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("parse URL: %w", err)
	}

	// Extract roomId from path
	path := strings.TrimPrefix(u.Path, "/")
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		return "", "", fmt.Errorf("no room ID in URL")
	}

	roomID = path

	// Extract and decode password if present
	psw := u.Query().Get("psw")
	if psw != "" {
		password, err = DecodePsw(psw)
		if err != nil {
			return "", "", fmt.Errorf("decode psw: %w", err)
		}
	}

	return roomID, password, nil
}

// DecodePsw decodes the psw parameter from a Jazz invite URL.
func DecodePsw(psw string) (string, error) {
	for len(psw)%4 != 0 {
		psw += "="
	}
	enc, err := base64.StdEncoding.DecodeString(psw)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	if len(enc) < 9 {
		return "", fmt.Errorf("encoded password too short: %d bytes", len(enc))
	}
	key := []byte("sberdevi")
	pwd := make([]byte, 8)
	for i := range pwd {
		pwd[i] = enc[i+1] ^ key[i]
	}
	return string(pwd), nil
}

// ClientID returns the session client ID.
func (c *APIClient) ClientID() string {
	return c.clientID
}
