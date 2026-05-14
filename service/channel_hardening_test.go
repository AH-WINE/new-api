package service

import (
	"errors"
	"net/http"
	"testing"

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
