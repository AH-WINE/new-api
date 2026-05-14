package service

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/types"
	"github.com/stretchr/testify/require"
)

func TestRetryParamExcludesFailedChannels(t *testing.T) {
	param := &RetryParam{}

	require.False(t, param.IsChannelExcluded(42))

	param.ExcludeChannel(42)
	require.True(t, param.IsChannelExcluded(42))
	require.False(t, param.IsChannelExcluded(43))
}

func TestShouldHardDisableChannelForAuthorizationFailures(t *testing.T) {
	err := types.WithOpenAIError(types.OpenAIError{
		Message: "Authorization failed",
		Type:    "upstream_error",
		Code:    "forbidden",
	}, http.StatusForbidden)

	require.True(t, ShouldHardDisableChannel(err))
}

func TestShouldHardDisableChannelForNoAvailableKey(t *testing.T) {
	err := types.NewErrorWithStatusCode(errors.New("no enabled keys"), types.ErrorCodeChannelNoAvailableKey, http.StatusInternalServerError)

	require.True(t, ShouldHardDisableChannel(err))
}

func TestShouldHardDisableChannelDoesNotTreat429AsHardFailure(t *testing.T) {
	err := types.NewOpenAIError(errors.New("bad response status code 429"), types.ErrorCodeBadResponse, http.StatusTooManyRequests)

	require.False(t, ShouldHardDisableChannel(err))
}

func TestHardDisableChannelPersistsQuarantinedStatus(t *testing.T) {
	require.NoError(t, model.DB.Exec("DELETE FROM channels").Error)
	require.NoError(t, model.DB.Exec("DELETE FROM abilities").Error)
	priority := int64(0)
	weight := uint(0)
	channel := model.Channel{Id: 77, Name: "bad-key", Status: common.ChannelStatusEnabled, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight}
	require.NoError(t, model.DB.Create(&channel).Error)
	require.NoError(t, model.DB.Create(&model.Ability{Group: "default", Model: "model-a", ChannelId: 77, Enabled: true, Priority: &priority, Weight: 0}).Error)
	t.Cleanup(func() {
		model.DB.Exec("DELETE FROM abilities")
		model.DB.Exec("DELETE FROM channels")
	})

	HardDisableChannel(*types.NewChannelError(77, 0, "bad-key", false, "", true), "Authorization failed")

	var reloaded model.Channel
	require.NoError(t, model.DB.First(&reloaded, 77).Error)
	require.Equal(t, common.ChannelStatusQuarantined, reloaded.Status)
	info := reloaded.GetOtherInfo()
	require.Equal(t, "Authorization failed", info["status_reason"])
	var ability model.Ability
	require.NoError(t, model.DB.First(&ability, "channel_id = ?", 77).Error)
	require.False(t, ability.Enabled)
}

func reset429CooldownStateForTest() {
	channel429Cooldown.Range(func(key, _ any) bool {
		channel429Cooldown.Delete(key)
		return true
	})
	channel429Backoff.Range(func(key, _ any) bool {
		channel429Backoff.Delete(key)
		return true
	})
}

func Test429CooldownStateRestoresFromPersistedReasonAfterRestart(t *testing.T) {
	reset429CooldownStateForTest()
	defer reset429CooldownStateForTest()

	_, reason := Record429BanWithReason(88)
	reset429CooldownStateForTest() // simulate process restart: in-memory cooldown maps are empty

	channel := model.Channel{Id: 88, Status: common.ChannelStatusAutoDisabled}
	channel.SetOtherInfo(map[string]interface{}{
		"status_reason": reason,
		"status_time":   time.Now().Unix(),
	})

	known, expired := Get429CooldownState(&channel)

	require.True(t, known)
	require.False(t, expired)
	require.True(t, Has429Cooldown(88))
}

func Test429CooldownStateTreatsExpiredLegacyReasonAsExpired(t *testing.T) {
	reset429CooldownStateForTest()
	defer reset429CooldownStateForTest()

	channel := model.Channel{Id: 89, Status: common.ChannelStatusAutoDisabled}
	channel.SetOtherInfo(map[string]interface{}{
		"status_reason": "429 rate-limit (cooldown 1m30s)",
		"status_time":   time.Now().Add(-3 * time.Minute).Unix(),
	})

	known, expired := Get429CooldownState(&channel)

	require.True(t, known)
	require.True(t, expired)
}

func Test429CooldownStateIgnoresNon429AutoDisabledReason(t *testing.T) {
	reset429CooldownStateForTest()
	defer reset429CooldownStateForTest()

	channel := model.Channel{Id: 90, Status: common.ChannelStatusAutoDisabled}
	channel.SetOtherInfo(map[string]interface{}{
		"status_reason": "Authorization failed",
		"status_time":   time.Now().Unix(),
	})

	known, expired := Get429CooldownState(&channel)

	require.False(t, known)
	require.False(t, expired)
}
