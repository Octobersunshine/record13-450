package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/net"
)

type InterfaceStats struct {
	LastBytesSent uint64
	LastBytesRecv uint64
	LastTime      time.Time
	UploadRate    float64
	DownloadRate  float64
}

type BandwidthMonitor struct {
	mu        sync.RWMutex
	stats     map[string]*InterfaceStats
	interval  time.Duration
}

type BandwidthResponse struct {
	Interface    string  `json:"interface"`
	UploadBps    float64 `json:"upload_bps"`
	DownloadBps  float64 `json:"download_bps"`
	UploadKBps   float64 `json:"upload_kbps"`
	DownloadKBps float64 `json:"download_kbps"`
	UploadMBps   float64 `json:"upload_mbps"`
	DownloadMBps float64 `json:"download_mbps"`
	Timestamp    string  `json:"timestamp"`
}

func NewBandwidthMonitor(interval time.Duration) *BandwidthMonitor {
	return &BandwidthMonitor{
		stats:    make(map[string]*InterfaceStats),
		interval: interval,
	}
}

func (bm *BandwidthMonitor) Start() {
	ticker := time.NewTicker(bm.interval)
	defer ticker.Stop()

	bm.updateOnce()
	for range ticker.C {
		bm.updateOnce()
	}
}

func (bm *BandwidthMonitor) updateOnce() {
	counters, err := net.IOCounters(true)
	if err != nil {
		log.Printf("获取网卡流量失败: %v", err)
		return
	}

	now := time.Now()

	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, counter := range counters {
		prev, exists := bm.stats[counter.Name]
		if !exists {
			bm.stats[counter.Name] = &InterfaceStats{
				LastBytesSent: counter.BytesSent,
				LastBytesRecv: counter.BytesRecv,
				LastTime:      now,
			}
			continue
		}

		elapsed := now.Sub(prev.LastTime).Seconds()
		if elapsed > 0 {
			var sentDelta, recvDelta uint64

			if counter.BytesSent >= prev.LastBytesSent {
				sentDelta = counter.BytesSent - prev.LastBytesSent
			}
			if counter.BytesRecv >= prev.LastBytesRecv {
				recvDelta = counter.BytesRecv - prev.LastBytesRecv
			}

			prev.UploadRate = float64(sentDelta) / elapsed
			prev.DownloadRate = float64(recvDelta) / elapsed
		}

		prev.LastBytesSent = counter.BytesSent
		prev.LastBytesRecv = counter.BytesRecv
		prev.LastTime = now
	}
}

func (bm *BandwidthMonitor) GetAll() []BandwidthResponse {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	now := time.Now().Format(time.RFC3339)
	result := make([]BandwidthResponse, 0, len(bm.stats))

	for name, stat := range bm.stats {
		result = append(result, BandwidthResponse{
			Interface:    name,
			UploadBps:    stat.UploadRate,
			DownloadBps:  stat.DownloadRate,
			UploadKBps:   stat.UploadRate / 1024,
			DownloadKBps: stat.DownloadRate / 1024,
			UploadMBps:   stat.UploadRate / 1024 / 1024,
			DownloadMBps: stat.DownloadRate / 1024 / 1024,
			Timestamp:    now,
		})
	}

	return result
}

func (bm *BandwidthMonitor) GetByName(name string) (*BandwidthResponse, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	stat, exists := bm.stats[name]
	if !exists {
		return nil, false
	}

	now := time.Now().Format(time.RFC3339)
	return &BandwidthResponse{
		Interface:    name,
		UploadBps:    stat.UploadRate,
		DownloadBps:  stat.DownloadRate,
		UploadKBps:   stat.UploadRate / 1024,
		DownloadKBps: stat.DownloadRate / 1024,
		UploadMBps:   stat.UploadRate / 1024 / 1024,
		DownloadMBps: stat.DownloadRate / 1024 / 1024,
		Timestamp:    now,
	}, true
}

func handleBandwidthAll(bm *BandwidthMonitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		data := bm.GetAll()
		resp := map[string]interface{}{
			"code":    0,
			"message": "success",
			"data":    data,
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func handleBandwidthByName(bm *BandwidthMonitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		name := strings.TrimPrefix(r.URL.Path, "/bandwidth/")
		name = strings.TrimSpace(name)
		if name == "" {
			http.Error(w, "网卡名称不能为空", http.StatusBadRequest)
			return
		}

		data, exists := bm.GetByName(name)
		if !exists {
			resp := map[string]interface{}{
				"code":    404,
				"message": fmt.Sprintf("网卡 %s 不存在", name),
			}
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(resp)
			return
		}

		resp := map[string]interface{}{
			"code":    0,
			"message": "success",
			"data":    data,
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func handleInterfaces(bm *BandwidthMonitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		interfaces, err := net.Interfaces()
		if err != nil {
			resp := map[string]interface{}{
				"code":    500,
				"message": err.Error(),
			}
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(resp)
			return
		}

		type IfaceInfo struct {
			Index        int      `json:"index"`
			Name         string   `json:"name"`
			MTU          int      `json:"mtu"`
			HardwareAddr string   `json:"hardware_addr"`
			Flags        []string `json:"flags"`
			Addrs        []string `json:"addrs"`
		}

		data := make([]IfaceInfo, 0, len(interfaces))
		for _, iface := range interfaces {
			addrs := make([]string, 0, len(iface.Addrs))
			for _, addr := range iface.Addrs {
				addrs = append(addrs, addr.Addr)
			}
			flags := make([]string, 0, len(iface.Flags))
			for _, flag := range iface.Flags {
				flags = append(flags, flag.String())
			}
			data = append(data, IfaceInfo{
				Index:        iface.Index,
				Name:         iface.Name,
				MTU:          iface.MTU,
				HardwareAddr: iface.HardwareAddr,
				Flags:        flags,
				Addrs:        addrs,
			})
		}

		resp := map[string]interface{}{
			"code":    0,
			"message": "success",
			"data":    data,
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func main() {
	interval := 1 * time.Second
	port := ":8080"

	bm := NewBandwidthMonitor(interval)
	go bm.Start()

	time.Sleep(interval + 200*time.Millisecond)

	mux := http.NewServeMux()

	mux.HandleFunc("/bandwidth", handleBandwidthAll(bm))
	mux.HandleFunc("/bandwidth/", handleBandwidthByName(bm))
	mux.HandleFunc("/interfaces", handleInterfaces(bm))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		resp := map[string]interface{}{
			"code":    0,
			"message": "带宽监控 API",
			"endpoints": []string{
				"GET /bandwidth          - 获取所有网卡的瞬时带宽",
				"GET /bandwidth/{name}   - 获取指定网卡的瞬时带宽",
				"GET /interfaces         - 获取所有网卡信息",
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	log.Printf("带宽监控服务已启动，监听端口 %s", port)
	log.Printf("采样间隔: %v", interval)
	log.Printf("API 端点:")
	log.Printf("  GET %s/bandwidth          - 获取所有网卡的瞬时带宽", port)
	log.Printf("  GET %s/bandwidth/{{name}} - 获取指定网卡的瞬时带宽", port)
	log.Printf("  GET %s/interfaces         - 获取所有网卡信息", port)

	if err := http.ListenAndServe(port, mux); err != nil {
		log.Fatalf("服务器启动失败: %v", err)
	}
}
