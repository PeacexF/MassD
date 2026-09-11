package record

import "testing"

func TestInsertSQL(t *testing.T) {
	r := New("t").Set("a", 1).Set("b", 2).OnConflict(Ignore)
	got := r.InsertSQL(2)
	want := "INSERT OR IGNORE INTO t (a,b) VALUES (?,?),(?,?)"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if New("t").Set("a", 1).OnConflict(Replace).InsertSQL(1) != "INSERT OR REPLACE INTO t (a) VALUES (?)" {
		t.Fatal("replace form is wrong")
	}
}

func TestSignature(t *testing.T) {
	a := New("t").Set("x", 1)
	b := New("t").Set("x", 2)
	c := New("t").Set("y", 1)
	d := New("t").Set("x", 1).OnConflict(Ignore)

	if a.Signature() != b.Signature() {
		t.Fatal("same shape should share a signature")
	}
	if a.Signature() == c.Signature() {
		t.Fatal("different columns must not share a signature")
	}
	if a.Signature() == d.Signature() {
		t.Fatal("different conflict modes must not share a signature")
	}
}

func TestSetIf(t *testing.T) {
	r := New("t").Set("a", 1).SetIf(false, "b", 2).SetIf(true, "c", 3)
	if len(r.Cols) != 2 || r.Cols[1] != "c" {
		t.Fatalf("unexpected columns: %v", r.Cols)
	}
}
