package jazz

import (
	"strings"
	"testing"
)

func TestEncodeDecodePswRoundTrip(t *testing.T) {
	psw, err := EncodePsw("ABCDEFGH")
	if err != nil {
		t.Fatal(err)
	}

	got, err := DecodePsw(psw)
	if err != nil {
		t.Fatal(err)
	}
	if got != "ABCDEFGH" {
		t.Fatalf("got %q, want %q", got, "ABCDEFGH")
	}
}

func TestParseRoomList(t *testing.T) {
	roomURL, err := BuildRoomURL("room-a", "ABCDEFGH")
	if err != nil {
		t.Fatal(err)
	}
	roomURL2, err := BuildRoomURL("room-b", "12345678")
	if err != nil {
		t.Fatal(err)
	}

	rooms, err := ParseRoomList(roomURL + "," + roomURL2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rooms) != 2 {
		t.Fatalf("got %d rooms, want 2", len(rooms))
	}
	if rooms[0].RoomID != "room-a" || rooms[0].Password != "ABCDEFGH" {
		t.Fatalf("unexpected first room: %+v", rooms[0])
	}
	if rooms[1].RoomID != "room-b" || rooms[1].Password != "12345678" {
		t.Fatalf("unexpected second room: %+v", rooms[1])
	}
}

func TestJoinRoomURLs(t *testing.T) {
	joined := JoinRoomURLs([]RoomSpec{
		{RoomID: "room-a", Password: "ABCDEFGH"},
		{RoomID: "room-b", Password: "12345678"},
	})

	parts := strings.Split(joined, ",")
	if len(parts) != 2 {
		t.Fatalf("got %d room URLs, want 2", len(parts))
	}

	for _, part := range parts {
		if !strings.HasPrefix(part, "https://salutejazz.ru/") {
			t.Fatalf("unexpected room URL %q", part)
		}
	}
}
