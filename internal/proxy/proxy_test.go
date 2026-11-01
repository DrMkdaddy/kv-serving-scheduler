package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DrMkdaddy/kv-serving-scheduler/internal/sched"
)

func fakeUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/tokenize", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		text, _ := body["content"].(string)
		tokens := strings.Fields(text)
		json.NewEncoder(w).Encode(map[string]any{"tokens": tokens})
	})
	mux.HandleFunc("/completion", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if body["id_slot"] == nil {
			http.Error(w, "missing id_slot", 400)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"content\":\"tok%d \"}\n\n", i)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(2 * time.Millisecond)
		}
		fmt.Fprint(w, "data: {\"timings\":{\"predicted_n\":3},\"stop\":true}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestProxy(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	upstream := fakeUpstream(t)
	s, err := New(Config{
		Listen:   "127.0.0.1:0",
		Upstream: upstream.URL,
		Scheduler: sched.SchedulerConfig{
			Slots: 1, BlockSize: 4, TotalBlocks: 4, MaxQueue: 4,
			Policy: "fcfs", Preempt: "recompute", Victim: "largest",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv
}

func TestCompletionPath(t *testing.T) {
	_, srv := newTestProxy(t)
	body := `{"prompt":"one two three","n_predict":3,"temperature":0,"tenant":"t1"}`
	resp, err := http.Post(srv.URL+"/completion", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), "tok0") {
		t.Fatalf("missing content: %s", raw)
	}
	if resp.Header.Get("X-Scheduler-Slot") == "" {
		t.Fatal("missing slot header")
	}
	if resp.Header.Get("X-Scheduler-TTFT-Ms") == "" {
		t.Fatal("missing ttft header")
	}
}

func TestStatsAfterCompletion(t *testing.T) {
	_, srv := newTestProxy(t)
	body := `{"prompt":"one two three","n_predict":3}`
	resp, err := http.Post(srv.URL+"/completion", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	statsResp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer statsResp.Body.Close()
	var stats map[string]any
	if err := json.NewDecoder(statsResp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	metrics := stats["metrics"].(map[string]any)
	if metrics["completed"].(float64) != 1 {
		t.Fatalf("completed = %v", metrics["completed"])
	}
	if metrics["requests"].(float64) != 1 {
		t.Fatalf("requests = %v", metrics["requests"])
	}
}

func TestRejectOversized(t *testing.T) {
	_, srv := newTestProxy(t)
	body := `{"prompt":"one two three four five six seven eight nine ten","n_predict":100}`
	resp, err := http.Post(srv.URL+"/completion", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

func TestInjectSlotForcesStream(t *testing.T) {
	out, err := injectSlot([]byte(`{"prompt":"x","stream":false}`), 3)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	json.Unmarshal(out, &body)
	if body["id_slot"].(float64) != 3 {
		t.Fatalf("id_slot = %v", body["id_slot"])
	}
	if body["stream"].(bool) != true {
		t.Fatal("stream was not forced on")
	}
}
