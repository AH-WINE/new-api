package service

import (
	"net/http"
	"one-api/common"
	"one-api/dto"
	"one-api/model"
	"testing"
)

func TestShouldDisableChannelTreats429AsTemporaryAutoBan(t *testing.T) {
	oldAutomaticDisable := common.AutomaticDisableChannelEnabled
	common.AutomaticDisableChannelEnabled = true
	t.Cleanup(func() { common.AutomaticDisableChannelEnabled = oldAutomaticDisable })

	err := &dto.OpenAIErrorWithStatusCode{
		StatusCode: http.StatusTooManyRequests,
		Error: dto.OpenAIError{
			Message: "bad response status code 429",
		},
	}

	if !ShouldDisableChannel(common.ChannelTypeOpenAI, err) {
		t.Fatalf("expected 429 upstream error to auto-disable channel for cooldown")
	}
}

func TestRateLimitAutoDisabledChannelWaitsUntilCooldownExpires(t *testing.T) {
	oldCooldown := common.ChannelRateLimitCooldownSeconds
	oldAutomaticEnable := common.AutomaticEnableChannelEnabled
	common.ChannelRateLimitCooldownSeconds = 90
	common.AutomaticEnableChannelEnabled = true
	t.Cleanup(func() {
		common.ChannelRateLimitCooldownSeconds = oldCooldown
		common.AutomaticEnableChannelEnabled = oldAutomaticEnable
	})

	channel := &model.Channel{Status: common.ChannelStatusAutoDisabled}
	channel.SetOtherInfo(map[string]interface{}{
		"status_reason": "bad response status code 429",
		"status_time":   float64(1000),
	})

	if !ShouldSkipRateLimitChannelTest(channel, 1089) {
		t.Fatalf("expected channel to skip probe before cooldown expires")
	}
	if ShouldEnableRateLimitChannelWithoutProbe(channel, 1089) {
		t.Fatalf("did not expect channel to enable before cooldown expires")
	}

	if ShouldSkipRateLimitChannelTest(channel, 1090) {
		t.Fatalf("expected channel to stop skipping probe at cooldown boundary")
	}
	if !ShouldEnableRateLimitChannelWithoutProbe(channel, 1090) {
		t.Fatalf("expected channel to enable without probe after cooldown expires")
	}
}

func TestRateLimitCooldownDoesNotAffectNonRateLimitAutoDisabledChannel(t *testing.T) {
	oldAutomaticEnable := common.AutomaticEnableChannelEnabled
	common.AutomaticEnableChannelEnabled = true
	t.Cleanup(func() { common.AutomaticEnableChannelEnabled = oldAutomaticEnable })

	channel := &model.Channel{Status: common.ChannelStatusAutoDisabled}
	channel.SetOtherInfo(map[string]interface{}{
		"status_reason": "Authorization failed",
		"status_time":   float64(1000),
	})

	if ShouldSkipRateLimitChannelTest(channel, 2000) {
		t.Fatalf("non-rate-limit channel should still be probe-tested")
	}
	if ShouldEnableRateLimitChannelWithoutProbe(channel, 2000) {
		t.Fatalf("non-rate-limit channel must not be enabled by rate-limit cooldown")
	}
}
