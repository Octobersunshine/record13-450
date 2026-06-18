package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
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
	Active        bool
}

type PeakRecord struct {
	UploadBps    float64   `json:"upload_bps"`
	DownloadBps  float64   `json:"download_bps"`
	UploadKBps   float64   `json:"upload_kbps"`
	DownloadKBps float64   `json:"download_kbps"`
	UploadMBps   float64   `json:"upload_mbps"`
	DownloadMBps float64   `json:"download_mbps"`
	Timestamp    time.Time `json:"timestamp"`
}

type InterfacePeaks struct {
	CurrentSession *PeakRecord   `json:"current_session"`
	HourlyHistory  []*PeakRecord `json:"hourly_history"`
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func newPeakRecord(uploadBps, downloadBps float64, ts time.Time) *PeakRecord {
	return &PeakRecord{
		UploadBps:    uploadBps,
		DownloadBps:  downloadBps,
		UploadKBps:   uploadBps / 1024,
		DownloadKBps: downloadBps / 1024,
		UploadMBps:   uploadBps / 1024 / 1024,
		DownloadMBps: downloadBps / 1024 / 1024,
		Timestamp:    ts,
	}
}

func (bm *BandwidthMonitor) updatePeakLocked(name string, uploadBps, downloadBps float64, ts time.Time) {
	peak, exists := bm.peaks[name]
	if !exists {
		return
	}

	if peak.CurrentSession == nil ||
		uploadBps > peak.CurrentSession.UploadBps ||
		downloadBps > peak.CurrentSession.DownloadBps {
		peak.CurrentSession = newPeakRecord(uploadBps, downloadBps, ts)
	}

	minuteKey := ts.Unix() / 60

	hourly := make([]*PeakRecord, 0, len(peak.HourlyHistory)+1)
	cutoffMinute := minuteKey - 59
	foundCurrentMinute := false

	for _, r := range peak.HourlyHistory {
		rMinute := r.Timestamp.Unix() / 60
		if rMinute < cutoffMinute {
			continue
		}
		if rMinute == minuteKey {
			if uploadBps > r.UploadBps || downloadBps > r.DownloadBps {
				hourly = append(hourly, newPeakRecord(
					max(r.UploadBps, uploadBps),
					max(r.DownloadBps, downloadBps),
					ts,
				))
			} else {
				hourly = append(hourly, r)
			}
			foundCurrentMinute = true
		} else {
			hourly = append(hourly, r)
		}
	}

	if !foundCurrentMinute {
		hourly = append(hourly, newPeakRecord(uploadBps, downloadBps, ts))
	}

	sort.Slice(hourly, func(i, j int) bool {
		return hourly[i].Timestamp.Before(hourly[j].Timestamp)
	})

	peak.HourlyHistory = hourly
}

type BandwidthMonitor struct {
	mu           sync.RWMutex
	stats        map[string]*InterfaceStats
	peaks        map[string]*InterfacePeaks
	interval     time.Duration
	activeNames  []string
	peakDuration time.Duration
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

type AllBandwidthResponse struct {
	Interfaces []BandwidthResponse `json:"interfaces"`
	Total      BandwidthResponse   `json:"total"`
}

func isLoopback(name string) bool {
	lower := strings.ToLower(name)
	return lower == "lo" || lower == "loopback" || strings.HasPrefix(lower, "loopback")
}

func discoverActiveInterfaces() ([]string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("获取网卡列表失败: %w", err)
	}

	var names []string
	for _, iface := range interfaces {
		if isLoopback(iface.Name) {
			continue
		}
		isUp := false
		for _, flag := range iface.Flags {
			if flag.String() == "up" {
				isUp = true
				break
			}
		}
		if !isUp {
			continue
		}
		if len(iface.Addrs) == 0 {
			continue
		}
		names = append(names, iface.Name)
	}

	sort.Strings(names)
	return names, nil
}

func NewBandwidthMonitor(interval time.Duration) *BandwidthMonitor {
	return &BandwidthMonitor{
		stats:        make(map[string]*InterfaceStats),
		peaks:        make(map[string]*InterfacePeaks),
		interval:     interval,
		peakDuration: 1 * time.Hour,
	}
}

func (bm *BandwidthMonitor) Start() {
	ticker := time.NewTicker(bm.interval)
	defer ticker.Stop()

	bm.refreshInterfaces()
	bm.updateOnce()

	for range ticker.C {
		bm.refreshInterfaces()
		bm.updateOnce()
		bm.cleanupStale()
	}
}

func (bm *BandwidthMonitor) refreshInterfaces() {
	names, err := discoverActiveInterfaces()
	if err != nil {
		log.Printf("刷新网卡列表失败: %v", err)
		return
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	newSet := make(map[string]bool, len(names))
	for _, name := range names {
		newSet[name] = true
		if _, exists := bm.stats[name]; !exists {
			bm.stats[name] = &InterfaceStats{Active: true}
		} else {
			bm.stats[name].Active = true
		}
		if _, exists := bm.peaks[name]; !exists {
			bm.peaks[name] = &InterfacePeaks{
				HourlyHistory: make([]*PeakRecord, 0),
			}
		}
	}

	for name, stat := range bm.stats {
		if !newSet[name] {
			stat.Active = false
		}
	}

	bm.activeNames = names
}

func (bm *BandwidthMonitor) updateOnce() {
	counters, err := net.IOCounters(true)
	if err != nil {
		log.Printf("获取网卡流量失败: %v", err)
		return
	}

	now := time.Now()

	counterMap := make(map[string]net.IOCountersStat, len(counters))
	for _, c := range counters {
		counterMap[c.Name] = c
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	for name, stat := range bm.stats {
		counter, found := counterMap[name]
		if !found {
			continue
		}

		if stat.LastTime.IsZero() {
			stat.LastBytesSent = counter.BytesSent
			stat.LastBytesRecv = counter.BytesRecv
			stat.LastTime = now
			continue
		}

		elapsed := now.Sub(stat.LastTime).Seconds()
		if elapsed > 0 {
			var sentDelta, recvDelta uint64

			if counter.BytesSent >= stat.LastBytesSent {
				sentDelta = counter.BytesSent - stat.LastBytesSent
			} else {
				sentDelta = counter.BytesSent
			}
			if counter.BytesRecv >= stat.LastBytesRecv {
				recvDelta = counter.BytesRecv - stat.LastBytesRecv
			} else {
				recvDelta = counter.BytesRecv
			}

			stat.UploadRate = float64(sentDelta) / elapsed
			stat.DownloadRate = float64(recvDelta) / elapsed
		}

		stat.LastBytesSent = counter.BytesSent
		stat.LastBytesRecv = counter.BytesRecv
		stat.LastTime = now

		bm.updatePeakLocked(name, stat.UploadRate, stat.DownloadRate, now)
	}
}

func (bm *BandwidthMonitor) cleanupStale() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for name, stat := range bm.stats {
		if !stat.Active {
			delete(bm.stats, name)
			delete(bm.peaks, name)
			log.Printf("移除已离线网卡: %s", name)
		}
	}
}

func toResponse(name string, uploadRate, downloadRate float64, now string) BandwidthResponse {
	return BandwidthResponse{
		Interface:    name,
		UploadBps:    uploadRate,
		DownloadBps:  downloadRate,
		UploadKBps:   uploadRate / 1024,
		DownloadKBps: downloadRate / 1024,
		UploadMBps:   uploadRate / 1024 / 1024,
		DownloadMBps: downloadRate / 1024 / 1024,
		Timestamp:    now,
	}
}

func (bm *BandwidthMonitor) GetAll() AllBandwidthResponse {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	now := time.Now().Format(time.RFC3339)
	result := make([]BandwidthResponse, 0, len(bm.activeNames))

	var totalUpload, totalDownload float64

	for _, name := range bm.activeNames {
		stat, exists := bm.stats[name]
		if !exists {
			continue
		}
		resp := toResponse(name, stat.UploadRate, stat.DownloadRate, now)
		result = append(result, resp)
		totalUpload += stat.UploadRate
		totalDownload += stat.DownloadRate
	}

	return AllBandwidthResponse{
		Interfaces: result,
		Total:      toResponse("total", totalUpload, totalDownload, now),
	}
}

func (bm *BandwidthMonitor) GetByName(name string) (*BandwidthResponse, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	stat, exists := bm.stats[name]
	if !exists || !stat.Active {
		return nil, false
	}

	now := time.Now().Format(time.RFC3339)
	resp := toResponse(name, stat.UploadRate, stat.DownloadRate, now)
	return &resp, true
}

func (bm *BandwidthMonitor) GetInterfaceNames() []string {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	names := make([]string, len(bm.activeNames))
	copy(names, bm.activeNames)
	return names
}

type AllPeaksResponse struct {
	Interfaces map[string]*InterfacePeaks `json:"interfaces"`
	Total      *InterfacePeaks            `json:"total"`
}

func (bm *BandwidthMonitor) GetAllPeaks() AllPeaksResponse {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	result := make(map[string]*InterfacePeaks, len(bm.activeNames))

	var totalPeak *InterfacePeaks

	for _, name := range bm.activeNames {
		peak, exists := bm.peaks[name]
		if !exists {
			continue
		}
		result[name] = peak
	}

	if len(bm.activeNames) > 0 {
		totalPeak = &InterfacePeaks{
			HourlyHistory: make([]*PeakRecord, 0),
		}

		minuteMap := make(map[int64]*PeakRecord)
		var maxUpload, maxDownload float64
		var maxUploadTime, maxDownloadTime time.Time

		for _, name := range bm.activeNames {
			peak, exists := bm.peaks[name]
			if !exists {
				continue
			}
			if peak.CurrentSession != nil {
				if peak.CurrentSession.UploadBps > maxUpload {
					maxUpload = peak.CurrentSession.UploadBps
					maxUploadTime = peak.CurrentSession.Timestamp
				}
				if peak.CurrentSession.DownloadBps > maxDownload {
					maxDownload = peak.CurrentSession.DownloadBps
					maxDownloadTime = peak.CurrentSession.Timestamp
				}
			}
			for _, r := range peak.HourlyHistory {
				minute := r.Timestamp.Unix() / 60
				if existing, ok := minuteMap[minute]; ok {
					existing.UploadBps += r.UploadBps
					existing.DownloadBps += r.DownloadBps
					existing.UploadKBps += r.UploadKBps
					existing.DownloadKBps += r.DownloadKBps
					existing.UploadMBps += r.UploadMBps
					existing.DownloadMBps += r.DownloadMBps
				} else {
					rec := *r
					minuteMap[minute] = &rec
				}
			}
		}

		if maxUpload > 0 || maxDownload > 0 {
			peakTime := maxUploadTime
			if maxDownloadTime.After(peakTime) {
				peakTime = maxDownloadTime
			}
			totalPeak.CurrentSession = newPeakRecord(maxUpload, maxDownload, peakTime)
		}

		for _, r := range minuteMap {
			totalPeak.HourlyHistory = append(totalPeak.HourlyHistory, r)
		}
		sort.Slice(totalPeak.HourlyHistory, func(i, j int) bool {
			return totalPeak.HourlyHistory[i].Timestamp.Before(totalPeak.HourlyHistory[j].Timestamp)
		})
	}

	return AllPeaksResponse{
		Interfaces: result,
		Total:      totalPeak,
	}
}

func (bm *BandwidthMonitor) GetPeaksByName(name string) (*InterfacePeaks, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	peak, exists := bm.peaks[name]
	if !exists {
		return nil, false
	}

	stat, statExists := bm.stats[name]
	if !statExists || !stat.Active {
		return nil, false
	}

	return peak, true
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
			resp := map[string]interface{}{
				"code":    400,
				"message": "网卡名称不能为空，可用的网卡: " + strings.Join(bm.GetInterfaceNames(), ", "),
			}
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(resp)
			return
		}

		data, exists := bm.GetByName(name)
		if !exists {
			resp := map[string]interface{}{
				"code":    404,
				"message": fmt.Sprintf("网卡 %s 不存在，可用的网卡: %s", name, strings.Join(bm.GetInterfaceNames(), ", ")),
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

func handleBandwidthPeaksAll(bm *BandwidthMonitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		data := bm.GetAllPeaks()
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

func handleBandwidthPeaksByName(bm *BandwidthMonitor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		name := strings.TrimPrefix(r.URL.Path, "/bandwidth/peaks/")
		name = strings.TrimSpace(name)
		if name == "" {
			resp := map[string]interface{}{
				"code":    400,
				"message": "网卡名称不能为空，可用的网卡: " + strings.Join(bm.GetInterfaceNames(), ", "),
			}
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(resp)
			return
		}

		data, exists := bm.GetPeaksByName(name)
		if !exists {
			resp := map[string]interface{}{
				"code":    404,
				"message": fmt.Sprintf("网卡 %s 不存在，可用的网卡: %s", name, strings.Join(bm.GetInterfaceNames(), ", ")),
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

	mux.HandleFunc("/bandwidth/peaks/", handleBandwidthPeaksByName(bm))
	mux.HandleFunc("/bandwidth/peaks", handleBandwidthPeaksAll(bm))
	mux.HandleFunc("/bandwidth", handleBandwidthAll(bm))
	mux.HandleFunc("/bandwidth/", handleBandwidthByName(bm))
	mux.HandleFunc("/interfaces", handleInterfaces(bm))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		resp := map[string]interface{}{
			"code":    0,
			"message": "带宽监控 API",
			"endpoints": []string{
				"GET /bandwidth              - 获取所有网卡的瞬时带宽",
				"GET /bandwidth/{name}       - 获取指定网卡的瞬时带宽",
				"GET /bandwidth/peaks        - 获取所有网卡的峰值带宽（近1小时）",
				"GET /bandwidth/peaks/{name} - 获取指定网卡的峰值带宽（近1小时）",
				"GET /interfaces             - 获取所有网卡信息",
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	log.Printf("带宽监控服务已启动，监听端口 %s", port)
	log.Printf("采样间隔: %v", interval)
	log.Printf("峰值缓存: 近1小时（按分钟聚合）")
	log.Printf("API 端点:")
	log.Printf("  GET %s/bandwidth              - 获取所有网卡的瞬时带宽", port)
	log.Printf("  GET %s/bandwidth/{{name}}     - 获取指定网卡的瞬时带宽", port)
	log.Printf("  GET %s/bandwidth/peaks        - 获取所有网卡的峰值带宽", port)
	log.Printf("  GET %s/bandwidth/peaks/{{name}} - 获取指定网卡的峰值带宽", port)
	log.Printf("  GET %s/interfaces             - 获取所有网卡信息", port)

	if err := http.ListenAndServe(port, mux); err != nil {
		log.Fatalf("服务器启动失败: %v", err)
	}
}
