package model

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
)

var group2model2channels map[string]map[string][]int // enabled channel
var channelsIDM map[int]*Channel                     // all channels include disabled
var channelSyncLock sync.RWMutex

// ─── Model health for auto-fallback routing ─────────────────────────────

// ModelFallbackPair defines a fallback route from one model to another.
type ModelFallbackPair struct {
	FallbackModel  string  `json:"fallback"`
	ThresholdRatio float64 `json:"threshold_ratio"` // 0.0-1.0: switch when primary healthy ratio < this
}

// Fallback config — hardcoded pairs for now. Future: DB options.
var modelFallbackMap = map[string]ModelFallbackPair{
	"deepseek-ai/deepseek-v4-pro": {
		FallbackModel:  "deepseek-ai/deepseek-v4-flash",
		ThresholdRatio: 0.5, // route to flash when <50% pro channels healthy
	},
}

// ModelHealthSnapshot captures per-model aggregate health from the last cache sync.
type ModelHealthSnapshot struct {
	TotalChannels   int     // total channels for this model (all statuses)
	HealthyChannels int     // status=1 AND not soft-degraded
	HealthyRatio    float64 // HealthyChannels / TotalChannels (or 0)
	MedianLatencyMs int     // median response_time of healthy channels
}

var modelHealthMap map[string]*ModelHealthSnapshot // model name → health snapshot
var modelHealthLock sync.RWMutex

// ─── Per-model relay latency tracking (sliding window) ─────────────────
// Updated on each relay completion; used by auto-fallback routing.

const modelLatencyWindowSize = 20 // last N relay response times per model

var modelLatenciesLock sync.Mutex
var modelLatencies = make(map[string]*modelLatencyWindow)

type modelLatencyWindow struct {
	samples []int // relay duration in seconds, up to windowSize
	idx     int
}

// RecordModelRelayLatency records a relay duration for a model.
func RecordModelRelayLatency(modelName string, durationSec int) {
	modelLatenciesLock.Lock()
	defer modelLatenciesLock.Unlock()

	w, ok := modelLatencies[modelName]
	if !ok {
		w = &modelLatencyWindow{samples: make([]int, 0, modelLatencyWindowSize)}
		modelLatencies[modelName] = w
	}

	if len(w.samples) < modelLatencyWindowSize {
		w.samples = append(w.samples, durationSec)
	} else {
		w.samples[w.idx] = durationSec
		w.idx = (w.idx + 1) % modelLatencyWindowSize
	}
}

// GetModelRelayLatencyP50 returns the median of recent relay durations in seconds.
// Returns 0 if not enough samples.
func GetModelRelayLatencyP50(modelName string) int {
	modelLatenciesLock.Lock()
	defer modelLatenciesLock.Unlock()

	w, ok := modelLatencies[modelName]
	if !ok || len(w.samples) < 3 {
		return 0
	}

	sorted := make([]int, len(w.samples))
	copy(sorted, w.samples)
	sort.Ints(sorted)
	return sorted[len(sorted)/2]
}

// GetModelHealth returns the cached health snapshot for a model. Thread-safe.
func GetModelHealth(modelName string) *ModelHealthSnapshot {
	modelHealthLock.RLock()
	defer modelHealthLock.RUnlock()
	return modelHealthMap[modelName]
}

// GetModelFallback returns the fallback pair for a model, if configured.
func GetModelFallback(modelName string) (ModelFallbackPair, bool) {
	pair, ok := modelFallbackMap[modelName]
	return pair, ok
}

// ─── Per-channel rate limiting (NVIDIA free tier: 40 RPM per key) ──────
// Token bucket: each key refills at 40/60 = 0.667 tokens/sec, max burst=1.
// Unlike a flat cooldown (which locks ALL keys after a uniform burst), token
// buckets refill continuously → keys stagger back online → no dead zone.
// Scales linearly: 80 keys → 80 × 40 RPM = 3200 req/min total capacity.

const nvTokensPerKeyPerSec = 40.0 / 60.0 // 0.667 tokens/sec = 1 token / 1.5s

type tokenBucket struct {
	tokens     float64   // available tokens (capped at burstSize)
	lastRefill time.Time // last refill timestamp
}

var channelTokenBuckets sync.Map // channelId (int) → *tokenBucket

func tryConsumeChannelToken(channelId int) bool {
	const burstSize = 1.0 // max tokens per key (1 = 40 RPM)

	val, _ := channelTokenBuckets.LoadOrStore(channelId, &tokenBucket{
		tokens:     burstSize, // start full
		lastRefill: time.Now(),
	})
	bucket := val.(*tokenBucket)

	elapsed := time.Since(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * nvTokensPerKeyPerSec
	if bucket.tokens > burstSize {
		bucket.tokens = burstSize
	}
	bucket.lastRefill = time.Now()

	if bucket.tokens >= 1.0 {
		bucket.tokens -= 1.0
		return true
	}
	// Save updated lastRefill even when no token consumed (prevents clock drift)
	return false
}

// hasChannelToken checks if a channel has enough tokens WITHOUT consuming.
// Use this to filter candidate channels during selection.
func hasChannelToken(channelId int) bool {
	val, _ := channelTokenBuckets.LoadOrStore(channelId, &tokenBucket{
		tokens:     1.0,
		lastRefill: time.Now(),
	})
	bucket := val.(*tokenBucket)
	elapsed := time.Since(bucket.lastRefill).Seconds()
	// calculate projected tokens but don't save — just check
	projected := bucket.tokens + elapsed*nvTokensPerKeyPerSec
	if projected > 1.0 {
		projected = 1.0
	}
	return projected >= 1.0
}

// consumeChannelToken consumes one token from a channel's bucket.
// Call AFTER the channel has been selected.
func consumeChannelToken(channelId int) {
	val, _ := channelTokenBuckets.LoadOrStore(channelId, &tokenBucket{
		tokens:     1.0,
		lastRefill: time.Now(),
	})
	bucket := val.(*tokenBucket)

	elapsed := time.Since(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * nvTokensPerKeyPerSec
	if bucket.tokens > 1.0 {
		bucket.tokens = 1.0
	}
	bucket.lastRefill = time.Now()

	if bucket.tokens >= 1.0 {
		bucket.tokens -= 1.0
	}
}

type ChannelTokenBucketInfo struct {
	ChannelId int     `json:"channel_id"`
	Tokens    float64 `json:"tokens"`
	CoolDownS float64 `json:"cooldown_s"` // estimated seconds until next token
}

func GetTokenBucketStatus() []ChannelTokenBucketInfo {
	var result []ChannelTokenBucketInfo
	channelTokenBuckets.Range(func(key, value interface{}) bool {
		channelId := key.(int)
		bucket := value.(*tokenBucket)
		elapsed := time.Since(bucket.lastRefill).Seconds()
		tokens := bucket.tokens + elapsed*nvTokensPerKeyPerSec
		if tokens > 1.0 {
			tokens = 1.0
		}
		cooldown := 0.0
		if tokens < 1.0 {
			cooldown = (1.0 - tokens) / nvTokensPerKeyPerSec
		}
		result = append(result, ChannelTokenBucketInfo{
			ChannelId: channelId,
			Tokens:    math.Round(tokens*1000) / 1000,
			CoolDownS: math.Round(cooldown*100) / 100,
		})
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].ChannelId < result[j].ChannelId })
	return result
}

func InitChannelCache() {
	if !common.MemoryCacheEnabled {
		return
	}
	newChannelId2channel := make(map[int]*Channel)
	var channels []*Channel
	DB.Find(&channels)
	for _, channel := range channels {
		newChannelId2channel[channel.Id] = channel
	}
	var abilities []*Ability
	DB.Find(&abilities)
	groups := make(map[string]bool)
	for _, ability := range abilities {
		groups[ability.Group] = true
	}
	newGroup2model2channels := make(map[string]map[string][]int)
	for group := range groups {
		newGroup2model2channels[group] = make(map[string][]int)
	}
	for _, channel := range channels {
		if channel.Status == common.ChannelStatusAutoDisabled {
			// Auto-recover expired 429 cooldowns and timeout bans inline so
			// recovery doesn't depend on the automatic channel test cron.
			if TryRecover429Cooldown(channel) {
				// recovered — proceed to add to pool
			} else if TryRecoverTimeoutDisabled(channel) {
				// recovered — proceed to add to pool
			}
		}
		if channel.Status != common.ChannelStatusEnabled {
			continue // skip disabled channels
		}
		groups := strings.Split(channel.Group, ",")
		for _, group := range groups {
			models := strings.Split(channel.Models, ",")
			for _, model := range models {
				if _, ok := newGroup2model2channels[group][model]; !ok {
					newGroup2model2channels[group][model] = make([]int, 0)
				}
				newGroup2model2channels[group][model] = append(newGroup2model2channels[group][model], channel.Id)
			}
		}
	}

	// sort by priority
	for group, model2channels := range newGroup2model2channels {
		for model, channels := range model2channels {
			sort.Slice(channels, func(i, j int) bool {
				return newChannelId2channel[channels[i]].GetPriority() > newChannelId2channel[channels[j]].GetPriority()
			})
			newGroup2model2channels[group][model] = channels
		}
	}

	// ─── Compute per-model health for auto-fallback routing ──────────────
	newModelHealth := make(map[string]*ModelHealthSnapshot)
	modelStats := make(map[string]*struct{ total, healthy int; lats []int })
	for _, ch := range newChannelId2channel {
		models := strings.Split(ch.Models, ",")
		for _, m := range models {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			if modelStats[m] == nil {
				modelStats[m] = &struct{ total, healthy int; lats []int }{}
			}
			modelStats[m].total++
			if ch.Status == common.ChannelStatusEnabled {
				_, _, _, isSoftDegraded := ch.GetSoftDegradeInfo()
				if !isSoftDegraded {
					modelStats[m].healthy++
				}
				modelStats[m].lats = append(modelStats[m].lats, ch.ResponseTime)
			}
		}
	}
	for m, s := range modelStats {
		ratio := float64(0)
		if s.total > 0 {
			ratio = float64(s.healthy) / float64(s.total)
		}
		medianMs := 0
		if len(s.lats) > 0 {
			sort.Ints(s.lats)
			medianMs = s.lats[len(s.lats)/2]
		}
		newModelHealth[m] = &ModelHealthSnapshot{
			TotalChannels:   s.total,
			HealthyChannels: s.healthy,
			HealthyRatio:    ratio,
			MedianLatencyMs: medianMs,
		}
	}
	modelHealthLock.Lock()
	modelHealthMap = newModelHealth
	modelHealthLock.Unlock()

	channelSyncLock.Lock()
	group2model2channels = newGroup2model2channels
	//channelsIDM = newChannelId2channel
	for i, channel := range newChannelId2channel {
		if channel.ChannelInfo.IsMultiKey {
			channel.Keys = channel.GetKeys()
			if channel.ChannelInfo.MultiKeyMode == constant.MultiKeyModePolling {
				if oldChannel, ok := channelsIDM[i]; ok {
					// 存在旧的渠道，如果是多key且轮询，保留轮询索引信息
					if oldChannel.ChannelInfo.IsMultiKey && oldChannel.ChannelInfo.MultiKeyMode == constant.MultiKeyModePolling {
						channel.ChannelInfo.MultiKeyPollingIndex = oldChannel.ChannelInfo.MultiKeyPollingIndex
					}
				}
			}
		}
	}
	channelsIDM = newChannelId2channel
	channelSyncLock.Unlock()
	common.SysLog("channels synced from database")
}

func SyncChannelCache(frequency int) {
	for {
		time.Sleep(time.Duration(frequency) * time.Second)
		common.SysLog("syncing channels from database")
		InitChannelCache()
	}
}

// computeLatencyWeight maps channel response_time (milliseconds) to a weight value
// used for latency-aware weighted random channel selection.
// Faster channels get higher weight. Very slow channels (>20s) get weight=1
// (virtually excluded unless the pool is exhausted).
// Untested channels (response_time=0) start at weight=100 (neutral).
func computeLatencyWeight(responseTime int) int {
	if responseTime <= 0 {
		return 100 // untested channel: default neutral weight
	}
	rt := responseTime
	if rt < 500 {
		rt = 500
	}
	// Channels slower than 20s: weight=1 (absolute minimum, only in fallback)
	if rt > 20000 {
		return 1
	}
	w := 100000 / rt
	if w < 1 {
		w = 1
	}
	if w > 200 {
		w = 200
	}
	return w
}

// MIN_HEALTHY_POOL_429: minimum number of enabled channels for a model-group
// below which 429 errors will NOT trigger auto-disable. Prevents cascading pool
// collapse when a burst of concurrent requests triggers 429s across many channels.
const MIN_HEALTHY_POOL_429 = 20

func GetRandomSatisfiedChannel(group string, model string, retry int) (*Channel, error) {
	return GetRandomSatisfiedChannelExcluding(group, model, retry, nil)
}

func GetRandomSatisfiedChannelExcluding(group string, model string, retry int, excluded map[int]struct{}) (*Channel, error) {
	// if memory cache is disabled, get channel directly from database
	if !common.MemoryCacheEnabled {
		return GetChannelExcluding(group, model, retry, excluded)
	}

	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	// First, try to find channels with the exact model name.
	channels := group2model2channels[group][model]

	// If no channels found, try to find channels with the normalized model name.
	if len(channels) == 0 {
		normalizedModel := ratio_setting.FormatMatchingModelName(model)
		channels = group2model2channels[group][normalizedModel]
	}
	if len(excluded) > 0 && len(channels) > 0 {
		filtered := make([]int, 0, len(channels))
		for _, channelId := range channels {
			if _, skip := excluded[channelId]; skip {
				continue
			}
			filtered = append(filtered, channelId)
		}
		channels = filtered
	}

	if len(channels) == 0 {
		return nil, nil
	}

	if len(channels) == 1 {
		if channel, ok := channelsIDM[channels[0]]; ok {
			return channel, nil
		}
		return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channels[0])
	}

	uniquePriorities := make(map[int]bool)
	for _, channelId := range channels {
		if channel, ok := channelsIDM[channelId]; ok {
			uniquePriorities[int(channel.GetPriority())] = true
		} else {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channelId)
		}
	}
	var sortedUniquePriorities []int
	for priority := range uniquePriorities {
		sortedUniquePriorities = append(sortedUniquePriorities, priority)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sortedUniquePriorities)))

	if retry >= len(uniquePriorities) {
		retry = len(uniquePriorities) - 1
	}
	targetPriority := int64(sortedUniquePriorities[retry])

	// get the priority for the given retry number
	var sumWeight = 0
	var targetChannels []*Channel
	var rateLimitedChannels []*Channel
	for _, channelId := range channels {
		if channel, ok := channelsIDM[channelId]; ok {
			if channel.GetPriority() == targetPriority {
				if !hasChannelToken(channel.Id) {
					rateLimitedChannels = append(rateLimitedChannels, channel)
				} else {
					sumWeight += channel.GetWeight()
					targetChannels = append(targetChannels, channel)
				}
			}
		} else {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channelId)
		}
	}

	// If all channels are rate-limited (burst > pool capacity), fall back to all channels
	if len(targetChannels) == 0 && len(rateLimitedChannels) > 0 {
		targetChannels = rateLimitedChannels
		for _, ch := range targetChannels {
			sumWeight += ch.GetWeight()
		}
	}

	if len(targetChannels) == 0 {
		return nil, errors.New(fmt.Sprintf("no channel found, group: %s, model: %s, priority: %d", group, model, targetPriority))
	}

	// Per-channel effective weights for weighted random selection.
	// When all channels have weight=0 (default), we use latency-based weighting.
	channelWeights := make(map[int]int, len(targetChannels))
	smoothingFactor := 1
	smoothingAdjustment := 0

	if sumWeight == 0 {
		// All channels at default weight: use latency-based weighting.
		// Fast channels (low response_time) get higher selection probability.
		for _, channel := range targetChannels {
			w := computeLatencyWeight(channel.ResponseTime)
			channelWeights[channel.Id] = w
			sumWeight += w
		}
		// No base offset needed — weights already encode the distribution.
		smoothingAdjustment = 0
	} else if sumWeight/len(targetChannels) < 10 {
		// when the average weight is less than 10, set smoothing factor to 100
		smoothingFactor = 100
	}

	// Calculate the total weight of all channels up to endIdx
	totalWeight := sumWeight * smoothingFactor

	// Generate a random value in the range [0, totalWeight)
	randomWeight := rand.Intn(totalWeight)

	// Find a channel based on its weight
	for _, channel := range targetChannels {
		w := channel.GetWeight()
		if effectiveW, ok := channelWeights[channel.Id]; ok {
			w = effectiveW
		}
		randomWeight -= w*smoothingFactor + smoothingAdjustment
		if randomWeight < 0 {
			consumeChannelToken(channel.Id)
			return channel, nil
		}
	}
	// return null if no channel is not found
	return nil, errors.New("channel not found")
}

// CountEnabledChannels returns the number of enabled channels for a given group and model.
// Uses the in-memory cache; returns 0 if memory cache is disabled.
func CountEnabledChannels(group string, model string) int {
	if !common.MemoryCacheEnabled {
		return 0
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()
	channels := group2model2channels[group][model]
	if len(channels) == 0 {
		// Try normalized model name
		normalizedModel := ratio_setting.FormatMatchingModelName(model)
		channels = group2model2channels[group][normalizedModel]
	}
	return len(channels)
}

func CacheGetChannel(id int) (*Channel, error) {
	if !common.MemoryCacheEnabled {
		return GetChannelById(id, true)
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	c, ok := channelsIDM[id]
	if !ok {
		return nil, fmt.Errorf("渠道# %d，已不存在", id)
	}
	return c, nil
}

func CacheGetChannelInfo(id int) (*ChannelInfo, error) {
	if !common.MemoryCacheEnabled {
		channel, err := GetChannelById(id, true)
		if err != nil {
			return nil, err
		}
		return &channel.ChannelInfo, nil
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	c, ok := channelsIDM[id]
	if !ok {
		return nil, fmt.Errorf("渠道# %d，已不存在", id)
	}
	return &c.ChannelInfo, nil
}

func CacheUpdateChannelStatus(id int, status int) {
	if !common.MemoryCacheEnabled {
		return
	}
	channelSyncLock.Lock()
	defer channelSyncLock.Unlock()
	if channel, ok := channelsIDM[id]; ok {
		channel.Status = status
	}
	if status != common.ChannelStatusEnabled {
		// delete the channel from group2model2channels
		for group, model2channels := range group2model2channels {
			for model, channels := range model2channels {
				for i, channelId := range channels {
					if channelId == id {
						// remove the channel from the slice
						group2model2channels[group][model] = append(channels[:i], channels[i+1:]...)
						break
					}
				}
			}
		}
	}
}

func CacheUpdateChannel(channel *Channel) {
	if !common.MemoryCacheEnabled {
		return
	}
	channelSyncLock.Lock()
	defer channelSyncLock.Unlock()
	if channel == nil {
		return
	}

	println("CacheUpdateChannel:", channel.Id, channel.Name, channel.Status, channel.ChannelInfo.MultiKeyPollingIndex)

	println("before:", channelsIDM[channel.Id].ChannelInfo.MultiKeyPollingIndex)
	channelsIDM[channel.Id] = channel
	println("after :", channelsIDM[channel.Id].ChannelInfo.MultiKeyPollingIndex)
}
