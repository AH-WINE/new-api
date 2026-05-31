package service

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/types"
)

// ─── 429 cooldown 管理 ──────────────────────────────────────
//   429 不依赖 probe 成功来恢复，而是按时间窗口自动恢复。
//   恢复后真实请求又 429 → backoff 递增 cooldown。

var (
	channel429Cooldown sync.Map // channelId (int) → cooldown 到期时间 (time.Time)
	channel429Backoff  sync.Map // channelId (int) → backoff 级别 (int: 0/1/2/3)
)

// 429 cooldown backoff 阶梯（秒）
var cooldown429Steps = []time.Duration{15 * time.Second, 45 * time.Second, 90 * time.Second}

const channel429ReasonPrefix = "429 rate-limit"

type cooldown429Record struct {
	level    int
	duration time.Duration
	expireAt time.Time
}

func clamp429Level(level int) int {
	if level < 0 {
		return 0
	}
	if level >= len(cooldown429Steps) {
		return len(cooldown429Steps) - 1
	}
	return level
}

func next429BackoffLevel(level int) int {
	return clamp429Level(level + 1)
}

func store429Cooldown(channelId int, level int, expireAt time.Time) {
	level = clamp429Level(level)
	channel429Cooldown.Store(channelId, expireAt)
	channel429Backoff.Store(channelId, next429BackoffLevel(level))
}

func record429Ban(channelId int) cooldown429Record {
	levelRaw, _ := channel429Backoff.Load(channelId)
	level, _ := levelRaw.(int)
	level = clamp429Level(level)
	duration := cooldown429Steps[level]
	expireAt := time.Now().Add(duration)

	store429Cooldown(channelId, level, expireAt)
	common.SysLog(fmt.Sprintf("通道 #%d 429 cooldown: level=%d, expire=%s", channelId, level, expireAt.Format(time.RFC3339)))
	return cooldown429Record{level: level, duration: duration, expireAt: expireAt}
}

func format429CooldownReason(record cooldown429Record) string {
	return fmt.Sprintf("%s (cooldown=%s, expire=%s, level=%d)", channel429ReasonPrefix, record.duration.String(), record.expireAt.UTC().Format(time.RFC3339), record.level)
}

// Record429Ban 记录一次 429 ban，返回 cooldown 到期时间。
// backoffLevel 0 = 第一次 429（90s），之后递增，最大 2（240s）。
func Record429Ban(channelId int) time.Time {
	return record429Ban(channelId).expireAt
}

// Record429BanWithReason 记录 429 ban，并返回可持久化到 channel.status_reason 的原因。
func Record429BanWithReason(channelId int) (time.Time, string) {
	record := record429Ban(channelId)
	return record.expireAt, format429CooldownReason(record)
}

func is429CooldownReason(reason string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(reason)), channel429ReasonPrefix)
}

func parse429ReasonField(reason string, key string) string {
	marker := key + "="
	idx := strings.Index(reason, marker)
	if idx < 0 {
		return ""
	}
	value := reason[idx+len(marker):]
	end := len(value)
	for _, sep := range []string{",", ")", " "} {
		if sepIdx := strings.Index(value, sep); sepIdx >= 0 && sepIdx < end {
			end = sepIdx
		}
	}
	return strings.TrimSpace(value[:end])
}

func parse429LegacyCooldownDuration(reason string) time.Duration {
	marker := "cooldown "
	idx := strings.Index(reason, marker)
	if idx < 0 {
		return 0
	}
	value := reason[idx+len(marker):]
	end := len(value)
	for _, sep := range []string{",", ")", " "} {
		if sepIdx := strings.Index(value, sep); sepIdx >= 0 && sepIdx < end {
			end = sepIdx
		}
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value[:end]))
	if err != nil {
		return 0
	}
	return duration
}

func parse429StatusTime(value interface{}) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case int32:
		return int64(typed)
	case float64:
		return int64(typed)
	case float32:
		return int64(typed)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed
	default:
		return 0
	}
}

func infer429LevelFromDuration(duration time.Duration) int {
	for idx, step := range cooldown429Steps {
		if duration == step {
			return idx
		}
	}
	return 0
}

func parse429CooldownFromInfo(reason string, statusTime int64) (time.Time, int, bool) {
	if expireRaw := parse429ReasonField(reason, "expire"); expireRaw != "" {
		if expireAt, err := time.Parse(time.RFC3339, expireRaw); err == nil {
			level := 0
			if levelRaw := parse429ReasonField(reason, "level"); levelRaw != "" {
				if parsed, err := strconv.Atoi(levelRaw); err == nil {
					level = clamp429Level(parsed)
				}
			}
			return expireAt, level, true
		}
	}

	duration := time.Duration(0)
	if durationRaw := parse429ReasonField(reason, "cooldown"); durationRaw != "" {
		duration, _ = time.ParseDuration(durationRaw)
	}
	if duration == 0 {
		duration = parse429LegacyCooldownDuration(reason)
	}
	if duration == 0 || statusTime <= 0 {
		return time.Time{}, 0, false
	}
	return time.Unix(statusTime, 0).Add(duration), infer429LevelFromDuration(duration), true
}

// Get429CooldownState checks whether an auto-disabled channel is in a persisted 429 cooldown.
// It restores in-memory cooldown state from channel.other_info so recovery survives process restarts.
func Get429CooldownState(channel *model.Channel) (known bool, expired bool) {
	if channel == nil || channel.Status != common.ChannelStatusAutoDisabled {
		return false, false
	}
	if expireRaw, ok := channel429Cooldown.Load(channel.Id); ok {
		if expireAt, ok := expireRaw.(time.Time); ok {
			return true, !time.Now().Before(expireAt)
		}
	}

	info := channel.GetOtherInfo()
	reason, _ := info["status_reason"].(string)
	if !is429CooldownReason(reason) {
		return false, false
	}
	statusTime := parse429StatusTime(info["status_time"])
	expireAt, level, ok := parse429CooldownFromInfo(reason, statusTime)
	if !ok {
		return true, true
	}
	store429Cooldown(channel.Id, level, expireAt)
	return true, !time.Now().Before(expireAt)
}

// Has429Cooldown 检查 channel 是否有已记录的 429 cooldown。
func Has429Cooldown(channelId int) bool {
	_, ok := channel429Cooldown.Load(channelId)
	return ok
}

// Is429CooldownExpired 检查 channel 的 429 cooldown 是否已到期。
func Is429CooldownExpired(channelId int) bool {
	expireRaw, ok := channel429Cooldown.Load(channelId)
	if !ok {
		return false // 无记录不等于 429 cooldown 到期，避免失败 probe 误恢复
	}
	expire, _ := expireRaw.(time.Time)
	return time.Now().After(expire)
}

// Reset429Backoff 恢复成功后重置 backoff 级别。
func Reset429Backoff(channelId int) {
	channel429Cooldown.Delete(channelId)
	channel429Backoff.Delete(channelId)
}

// Cooldown429Duration 返回当前 429 cooldown 时长（用于日志）。
func Cooldown429Duration() time.Duration {
	return cooldown429Steps[0]
}

func formatNotifyType(channelId int, status int) string {
	return fmt.Sprintf("%s_%d_%d", dto.NotifyTypeChannelUpdate, channelId, status)
}

// disable & notify
func DisableChannel(channelError types.ChannelError, reason string) {
	common.SysLog(fmt.Sprintf("通道「%s」（#%d）发生错误，准备禁用，原因：%s", channelError.ChannelName, channelError.ChannelId, reason))

	// 检查是否启用自动禁用功能
	if !channelError.AutoBan {
		common.SysLog(fmt.Sprintf("通道「%s」（#%d）未启用自动禁用功能，跳过禁用操作", channelError.ChannelName, channelError.ChannelId))
		return
	}

	success := model.UpdateChannelStatus(channelError.ChannelId, channelError.UsingKey, common.ChannelStatusAutoDisabled, reason)
	if success {
		subject := fmt.Sprintf("通道「%s」（#%d）已被禁用", channelError.ChannelName, channelError.ChannelId)
		content := fmt.Sprintf("通道「%s」（#%d）已被禁用，原因：%s", channelError.ChannelName, channelError.ChannelId, reason)
		NotifyRootUser(formatNotifyType(channelError.ChannelId, common.ChannelStatusAutoDisabled), subject, content)
	}
}

func HardDisableChannel(channelError types.ChannelError, reason string) {
	common.SysLog(fmt.Sprintf("通道「%s」（#%d）发生硬错误，准备永久隔离，原因：%s", channelError.ChannelName, channelError.ChannelId, reason))

	if !channelError.AutoBan {
		common.SysLog(fmt.Sprintf("通道「%s」（#%d）未启用自动禁用功能，跳过永久隔离", channelError.ChannelName, channelError.ChannelId))
		return
	}

	success := model.UpdateChannelStatus(channelError.ChannelId, channelError.UsingKey, common.ChannelStatusQuarantined, reason)
	if success {
		Reset429Backoff(channelError.ChannelId)
		subject := fmt.Sprintf("通道「%s」（#%d）已被永久隔离", channelError.ChannelName, channelError.ChannelId)
		content := fmt.Sprintf("通道「%s」（#%d）已被永久隔离，原因：%s", channelError.ChannelName, channelError.ChannelId, reason)
		NotifyRootUser(formatNotifyType(channelError.ChannelId, common.ChannelStatusQuarantined), subject, content)
	}
}

func EnableChannel(channelId int, usingKey string, channelName string) {
	success := model.UpdateChannelStatus(channelId, usingKey, common.ChannelStatusEnabled, "")
	if success {
		Reset429Backoff(channelId)
		subject := fmt.Sprintf("通道「%s」（#%d）已被启用", channelName, channelId)
		content := fmt.Sprintf("通道「%s」（#%d）已被启用", channelName, channelId)
		NotifyRootUser(formatNotifyType(channelId, common.ChannelStatusEnabled), subject, content)
	}
}

// Is429ChannelError 判断错误是否为 429 rate-limit。
func Is429ChannelError(err *types.NewAPIError) bool {
	if err == nil {
		return false
	}
	return err.StatusCode == 429
}

func ShouldHardDisableChannel(err *types.NewAPIError) bool {
	if err == nil || Is429ChannelError(err) {
		return false
	}

	switch err.GetErrorCode() {
	case types.ErrorCodeChannelNoAvailableKey, types.ErrorCodeChannelInvalidKey:
		return true
	}

	lowerMessage := strings.ToLower(err.ErrorWithStatusCode())
	hardFailureMarkers := []string{
		"authorization failed",
		"permission denied",
		"not authorized",
		"invalid api key",
		"invalid key",
		"invalid token",
		"security token",
		"no enabled keys",
		"no keys available",
	}
	for _, marker := range hardFailureMarkers {
		if strings.Contains(lowerMessage, marker) {
			return true
		}
	}
	return false
}

func ShouldDisableChannel(err *types.NewAPIError) bool {
	if !common.AutomaticDisableChannelEnabled {
		return false
	}
	if err == nil {
		return false
	}
	// 429 不走永久 ban，走 cooldown 时间窗恢复
	if Is429ChannelError(err) {
		return false
	}
	if types.IsChannelError(err) {
		return true
	}
	if types.IsSkipRetryError(err) {
		return false
	}
	if operation_setting.ShouldDisableByStatusCode(err.StatusCode) {
		return true
	}

	lowerMessage := strings.ToLower(err.Error())
	search, _ := AcSearch(lowerMessage, operation_setting.AutomaticDisableKeywords, true)
	return search
}

func ShouldEnableChannel(newAPIError *types.NewAPIError, status int) bool {
	if !common.AutomaticEnableChannelEnabled {
		return false
	}
	if status != common.ChannelStatusAutoDisabled {
		return false
	}
	// 路径A：probe 成功（原有逻辑）
	if newAPIError == nil {
		return true
	}
	// 路径B：429 错误不做永久禁用，允许恢复
	return false
}

// ShouldEnableChannelWithCooldown 支持 429 cooldown 时间窗恢复。
// channelId 用于检查 cooldown 是否到期。
func ShouldEnableChannelWithCooldown(channelId int, newAPIError *types.NewAPIError, status int) bool {
	if !common.AutomaticEnableChannelEnabled {
		return false
	}
	if status != common.ChannelStatusAutoDisabled {
		return false
	}
	// 路径A：probe 成功（原有逻辑）
	if newAPIError == nil {
		return true
	}
	// 429 cooldown 恢复不依赖 probe 成功，也不能在 probe 返回 429 时误恢复。
	// 自动测试循环会在发起 probe 前检查 cooldown 是否到期并直接恢复。
	// 这里拿到 429 说明本次 probe 已失败，必须保持禁用状态。
	return false
}

// EnableChannel429Cooldown 启用 429 cooldown 恢复的 channel。
func EnableChannel429Cooldown(channelId int, channelName string) {
	success := model.UpdateChannelStatus(channelId, "", common.ChannelStatusEnabled, "")
	if success {
		Reset429Backoff(channelId)
		common.SysLog(fmt.Sprintf("通道「%s」（#%d）429 cooldown 到期，已自动恢复", channelName, channelId))
		subject := fmt.Sprintf("通道「%s」（#%d）429 冷却到期已恢复", channelName, channelId)
		content := fmt.Sprintf("通道「%s」（#%d）429 cooldown 到期，已自动恢复为启用状态", channelName, channelId)
		NotifyRootUser(formatNotifyType(channelId, common.ChannelStatusEnabled), subject, content)
	}
}

// ─── context_cap 自适应学习 ──────────────────────────────────
//   通道的 context_cap 非固定——NV NIM 实例可能重分配导致 cap 变化。
//   本模块实现 自动标记（Learn）机制：
//     - 400 错误含 "context length of N tokens" → 标记 other_info.context_cap = N
//     - 若标记成功则更新内存缓存
//   自动清除（Unlearn）由外部探测定时负责（周期性重测并更新 SQL）。

var contextCapErrorRe = regexp.MustCompile(`context length\D*(\d+)`)

// ExtractContextCapFromError 从 400 错误中提取 context cap 值。
// 返回 0 表示该错误不是 context-length 限制错误。
func ExtractContextCapFromError(err *types.NewAPIError) int {
	if err == nil || err.StatusCode != 400 {
		return 0
	}
	matches := contextCapErrorRe.FindStringSubmatch(err.Error())
	if len(matches) < 2 {
		return 0
	}
	cap, parseErr := strconv.Atoi(matches[1])
	if parseErr != nil || cap <= 0 {
		return 0
	}
	return cap
}

// AutoMarkContextCap 自动学习并标记通道的 context_cap。
// 仅当新 cap 值与当前不同时更新。仅向下标记（发现更小 cap），上修由探测负责。
func AutoMarkContextCap(channelId int, capValue int) {
	if !common.MemoryCacheEnabled {
		return
	}
	channel, err := model.CacheGetChannel(channelId)
	if err != nil || channel == nil {
		return
	}
	oldCap := channel.GetContextCap()
	if oldCap == capValue {
		return
	}
	// 只允许收紧（发现更小 cap）；放宽由外部探测定时负责
	if oldCap > 0 && capValue > oldCap {
		return
	}
	info := channel.GetOtherInfo()
	info["context_cap"] = float64(capValue)
	channel.SetOtherInfo(info)
	if saveErr := channel.SaveWithoutKey(); saveErr != nil {
		common.SysLog(fmt.Sprintf("context_cap 自动标记失败: channel_id=%d, cap=%d, error=%v", channelId, capValue, saveErr))
		return
	}
	model.CacheUpdateChannel(channel)
	common.SysLog(fmt.Sprintf("context_cap 自动学习: 通道 #%d 标记为 %d（原=%d）", channelId, capValue, oldCap))
}

// AutoClearContextCapIfSucceeded 在请求成功且 prompt 超过标记的 cap 时清除标记。
// 表示 NV 端 cap 已放宽（如从 262K 升级到 1M）。
func AutoClearContextCapIfSucceeded(channelId int, promptTokens int) {
	if !common.MemoryCacheEnabled || promptTokens <= 0 {
		return
	}
	channel, err := model.CacheGetChannel(channelId)
	if err != nil || channel == nil {
		return
	}
	oldCap := channel.GetContextCap()
	if oldCap <= 0 {
		return
	}
	// 请求的 prompt 必须超过标记 cap 10K+ 才算真正的放宽
	if promptTokens <= oldCap+10000 {
		return
	}
	info := channel.GetOtherInfo()
	delete(info, "context_cap")
	channel.SetOtherInfo(info)
	if saveErr := channel.SaveWithoutKey(); saveErr != nil {
		common.SysLog(fmt.Sprintf("context_cap 自动清除失败: channel_id=%d, error=%v", channelId, saveErr))
		return
	}
	model.CacheUpdateChannel(channel)
	common.SysLog(fmt.Sprintf("context_cap 自动清除: 通道 #%d 原标记=%d，实际通过 %d tokens — 标记已移除", channelId, oldCap, promptTokens))
}
