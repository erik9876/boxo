package sphinx

import (
	"bytes"
	"testing"
	"time"
)

func TestKeyManagerSealedRecordRoundtrip(t *testing.T) {
	idPriv, pid := newIdentity(t)
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	if km.PeerID() != pid {
		t.Errorf("PeerID = %s, want %s", km.PeerID(), pid)
	}
	if km.PrivateKey() == nil {
		t.Error("PrivateKey returned nil")
	}

	raw, err := km.SealedRecord()
	if err != nil {
		t.Fatalf("SealedRecord: %v", err)
	}
	rec, err := ConsumeKeyRecord(raw)
	if err != nil {
		t.Fatalf("ConsumeKeyRecord on own sealed record: %v", err)
	}
	if rec.PeerID != pid {
		t.Errorf("record PeerID = %s, want %s", rec.PeerID, pid)
	}
	if !bytes.Equal(rec.SphinxPublicKey, km.PublicKey().Bytes()) {
		t.Error("record public key differs from manager public key")
	}
}

func TestKeyManagerResealsPastThreshold(t *testing.T) {
	idPriv, _ := newIdentity(t)
	km, err := NewKeyManager(idPriv, time.Hour)
	if err != nil {
		t.Fatalf("NewKeyManager: %v", err)
	}
	t0 := time.Now()
	km.now = func() time.Time { return t0 }

	raw1, err := km.SealedRecord()
	if err != nil {
		t.Fatalf("SealedRecord: %v", err)
	}
	rec1, err := ConsumeKeyRecord(raw1)
	if err != nil {
		t.Fatalf("ConsumeKeyRecord: %v", err)
	}

	// Within the refresh threshold (half the TTL) the cached bytes are reused
	km.now = func() time.Time { return t0.Add(29 * time.Minute) }
	raw2, err := km.SealedRecord()
	if err != nil {
		t.Fatalf("SealedRecord within threshold: %v", err)
	}
	if !bytes.Equal(raw1, raw2) {
		t.Error("SealedRecord re-sealed before the refresh threshold")
	}

	// Past the threshold a new record with higher Seq and later Expiry is sealed
	km.now = func() time.Time { return t0.Add(31 * time.Minute) }
	raw3, err := km.SealedRecord()
	if err != nil {
		t.Fatalf("SealedRecord past threshold: %v", err)
	}
	rec3, err := ConsumeKeyRecord(raw3)
	if err != nil {
		t.Fatalf("ConsumeKeyRecord on re-sealed record: %v", err)
	}
	if rec3.Seq <= rec1.Seq {
		t.Errorf("re-sealed Seq = %d, want > %d", rec3.Seq, rec1.Seq)
	}
	if !rec3.Expiry.After(rec1.Expiry) {
		t.Errorf("re-sealed Expiry = %v, want after %v", rec3.Expiry, rec1.Expiry)
	}
	if !bytes.Equal(rec3.SphinxPublicKey, rec1.SphinxPublicKey) {
		t.Error("re-sealing changed the public key; only Seq/Expiry may change")
	}
}
