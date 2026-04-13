package jazz

import "testing"

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

func TestBuildAndParseRoomURL(t *testing.T) {
	roomURL, err := BuildRoomURL("room-a", "ABCDEFGH")
	if err != nil {
		t.Fatal(err)
	}

	roomID, password, err := ParseRoomURL(roomURL)
	if err != nil {
		t.Fatal(err)
	}
	if roomID != "room-a" || password != "ABCDEFGH" {
		t.Fatalf("got roomID=%q password=%q", roomID, password)
	}
}
