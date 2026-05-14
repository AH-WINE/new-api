package service

import (
	"errors"
	"net/http"
	"testing"

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
