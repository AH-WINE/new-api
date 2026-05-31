package service

import (
	"fmt"
	"net/http"
	"one-api/common"
	"one-api/dto"
	"one-api/model"
	"one-api/setting/operation_setting"
	"strconv"
	"strings"
)

func formatNotifyType(channelId int, status int) string {
	return fmt.Sprintf("%s_%d_%d", dto.NotifyTypeChannelUpdate, channelId, status)
}

// disable & notify
func DisableChannel(channelId int, channelName string, reason string) {
	success := model.UpdateChannelStatusById(channelId, common.ChannelStatusAutoDisabled, reason)
	if success {
		subject := fmt.Sprintf("通道「%s」（#%d）已被禁用", channelName, channelId)
		content := fmt.Sprintf("通道「%s」（#%d）已被禁用，原因：%s", channelName, channelId, reason)
		NotifyRootUser(formatNotifyType(channelId, common.ChannelStatusAutoDisabled), subject, content)
	}
}

func EnableChannel(channelId int, channelName string) {
	success := model.UpdateChannelStatusById(channelId, common.ChannelStatusEnabled, "")
	if success {
		subject := fmt.Sprintf("通道「%s」（#%d）已被启用", channelName, channelId)
		content := fmt.Sprintf("通道「%s」（#%d）已被启用", channelName, channelId)
		NotifyRootUser(formatNotifyType(channelId, common.ChannelStatusEnabled), subject, content)
	}
}

func IsRateLimitError(err *dto.OpenAIErrorWithStatusCode) bool {
	if err == nil || err.LocalError {
		return false
	}
	if err.StatusCode == http.StatusTooManyRequests {
		return true
	}
	haystack := strings.ToLower(strings.Join([]string{
		fmt.Sprint(err.StatusCode),
		fmt.Sprint(err.Error.Code),
		err.Error.Type,
		err.Error.Message,
	}, " "))
	return isRateLimitText(haystack)
}

func isRateLimitText(text string) bool {
	text = strings.ToLower(text)
	if strings.TrimSpace(text) == "" {
		return false
	}
	for _, token := range []string{
		"429",
		"too many requests",
		"rate limit",
		"rate_limit",
		"rate-limit",
		"rate_limited",
		"rate limit reached",
		"rate_limit_exceeded",
		"requests per min",
		"tokens per min",
		"please try again in",
		"上游负载已饱和",
	} {
		if strings.Contains(text, token) {
			return true
		}
	}
	return false
}

func channelStatusReason(channel *model.Channel) string {
	if channel == nil {
		return ""
	}
	info := channel.GetOtherInfo()
	if value, ok := info["status_reason"]; ok {
		return fmt.Sprint(value)
	}
	return ""
}

func channelStatusTime(channel *model.Channel) int64 {
	if channel == nil {
		return 0
	}
	info := channel.GetOtherInfo()
	value, ok := info["status_time"]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(value)), 10, 64)
		return parsed
	}
}

func IsRateLimitAutoDisabledChannel(channel *model.Channel) bool {
	if channel == nil || channel.Status != common.ChannelStatusAutoDisabled {
		return false
	}
	return isRateLimitText(channelStatusReason(channel))
}

func ShouldEnableRateLimitChannelWithoutProbe(channel *model.Channel, now int64) bool {
	if !common.AutomaticEnableChannelEnabled || !IsRateLimitAutoDisabledChannel(channel) {
		return false
	}
	cooldown := int64(common.ChannelRateLimitCooldownSeconds)
	if cooldown <= 0 {
		return true
	}
	statusTime := channelStatusTime(channel)
	if statusTime <= 0 {
		return true
	}
	return now-statusTime >= cooldown
}

func ShouldSkipRateLimitChannelTest(channel *model.Channel, now int64) bool {
	if !IsRateLimitAutoDisabledChannel(channel) {
		return false
	}
	return !ShouldEnableRateLimitChannelWithoutProbe(channel, now)
}

func ShouldDisableChannel(channelType int, err *dto.OpenAIErrorWithStatusCode) bool {
	if !common.AutomaticDisableChannelEnabled {
		return false
	}
	if err == nil {
		return false
	}
	if err.LocalError {
		return false
	}
	if IsRateLimitError(err) {
		return true
	}
	if err.StatusCode == http.StatusUnauthorized {
		return true
	}
	if err.StatusCode == http.StatusForbidden {
		switch channelType {
		case common.ChannelTypeGemini:
			return true
		}
	}
	switch err.Error.Code {
	case "invalid_api_key":
		return true
	case "account_deactivated":
		return true
	case "billing_not_active":
		return true
	}
	switch err.Error.Type {
	case "insufficient_quota":
		return true
	case "insufficient_user_quota":
		return true
	// https://docs.anthropic.com/claude/reference/errors
	case "authentication_error":
		return true
	case "permission_error":
		return true
	case "forbidden":
		return true
	}

	lowerMessage := strings.ToLower(err.Error.Message)
	search, _ := AcSearch(lowerMessage, operation_setting.AutomaticDisableKeywords, true)
	if search {
		return true
	}

	return false
}

func ShouldEnableChannel(err error, openaiWithStatusErr *dto.OpenAIErrorWithStatusCode, status int) bool {
	if !common.AutomaticEnableChannelEnabled {
		return false
	}
	if err != nil {
		return false
	}
	if openaiWithStatusErr != nil {
		return false
	}
	if status != common.ChannelStatusAutoDisabled {
		return false
	}
	return true
}
