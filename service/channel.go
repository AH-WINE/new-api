package service

import (
	"fmt"
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
// wine 设计的三层恢复架构第二层：
//   429 不依赖 probe 成功来恢复，而是按时间窗口自动恢复。
//   恢复后真实请求又 429 → backoff 递增 cooldown。

var (
	channel429Cooldown sync.Map // channelId (int) → cooldown 到期时间 (time.Time)
	channel429Backoff  sync.Map // channelId (int) → backoff 级别 (int: 0/1/2/3)
)

// 429 cooldown backoff 阶梯（秒）
var cooldown429Steps = []time.Duration{90 * time.Second, 150 * time.Second, 240 * time.Second}

// Record429Ban 记录一次 429 ban，返回 cooldown 到期时间。
// backoffLevel 0 = 第一次 429（90s），之后递增，最大 2（240s）。
func Record429Ban(channelId int) time.Time {
	levelRaw, _ := channel429Backoff.Load(channelId)
	level, _ := levelRaw.(int)
	if level >= len(cooldown429Steps) {
		level = len(cooldown429Steps) - 1
	}
	cooldown := cooldown429Steps[level]
	expireAt := time.Now().Add(cooldown)

	channel429Cooldown.Store(channelId, expireAt)
	if level+1 < len(cooldown429Steps) {
		channel429Backoff.Store(channelId, level+1)
	}
	common.SysLog(fmt.Sprintf("通道 #%d 429 cooldown: level=%d, expire=%s", channelId, level, expireAt.Format(time.RFC3339)))
	return expireAt
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
