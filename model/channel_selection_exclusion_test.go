package model

import (
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/stretchr/testify/require"
)

func resetChannelSelectionTestTables(t *testing.T) {
	t.Helper()
	require.NoError(t, DB.Exec("DELETE FROM abilities").Error)
	require.NoError(t, DB.Exec("DELETE FROM channels").Error)
	t.Cleanup(func() {
		DB.Exec("DELETE FROM abilities")
		DB.Exec("DELETE FROM channels")
	})
}

func insertSelectionTestChannels(t *testing.T) {
	t.Helper()
	priority := int64(0)
	weight := uint(0)
	channels := []Channel{
		{Id: 1, Name: "bad", Status: common.ChannelStatusEnabled, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight},
		{Id: 2, Name: "good", Status: common.ChannelStatusEnabled, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight},
	}
	require.NoError(t, DB.Create(&channels).Error)
	abilities := []Ability{
		{Group: "default", Model: "model-a", ChannelId: 1, Enabled: true, Priority: &priority, Weight: 0},
		{Group: "default", Model: "model-a", ChannelId: 2, Enabled: true, Priority: &priority, Weight: 0},
	}
	require.NoError(t, DB.Create(&abilities).Error)
}

func TestGetChannelExcludingSkipsExcludedChannelFromDB(t *testing.T) {
	resetChannelSelectionTestTables(t)
	oldMemoryCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = false
	t.Cleanup(func() { common.MemoryCacheEnabled = oldMemoryCache })
	insertSelectionTestChannels(t)

	channel, err := GetChannelExcluding("default", "model-a", 0, map[int]struct{}{1: {}})

	require.NoError(t, err)
	require.NotNil(t, channel)
	require.Equal(t, 2, channel.Id)
}

func TestGetRandomSatisfiedChannelExcludingSkipsExcludedChannelFromCache(t *testing.T) {
	resetChannelSelectionTestTables(t)
	oldMemoryCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = true
	t.Cleanup(func() { common.MemoryCacheEnabled = oldMemoryCache })
	insertSelectionTestChannels(t)
	InitChannelCache()

	channel, err := GetRandomSatisfiedChannelExcluding("default", "model-a", 0, map[int]struct{}{1: {}}, 0)

	require.NoError(t, err)
	require.NotNil(t, channel)
	require.Equal(t, 2, channel.Id)
}

func TestUpdateChannelStatusPersistsWhenCacheAlreadyMarkedUnavailable(t *testing.T) {
	resetChannelSelectionTestTables(t)
	oldMemoryCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = true
	t.Cleanup(func() { common.MemoryCacheEnabled = oldMemoryCache })
	priority := int64(0)
	weight := uint(0)
	channel := Channel{Id: 10, Name: "cache-first", Status: common.ChannelStatusEnabled, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight}
	require.NoError(t, DB.Create(&channel).Error)
	require.NoError(t, DB.Create(&Ability{Group: "default", Model: "model-a", ChannelId: 10, Enabled: true, Priority: &priority, Weight: 0}).Error)
	InitChannelCache()
	CacheUpdateChannelStatus(10, common.ChannelStatusAutoDisabled)

	updated := UpdateChannelStatus(10, "", common.ChannelStatusAutoDisabled, "429 cooldown")

	require.True(t, updated)
	var reloaded Channel
	require.NoError(t, DB.First(&reloaded, 10).Error)
	require.Equal(t, common.ChannelStatusAutoDisabled, reloaded.Status)
	var ability Ability
	require.NoError(t, DB.First(&ability, "channel_id = ?", 10).Error)
	require.False(t, ability.Enabled)
}

func TestQuarantinedChannelIsExcludedFromSelectionAndKeepsAbilitiesDisabled(t *testing.T) {
	resetChannelSelectionTestTables(t)
	oldMemoryCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = false
	t.Cleanup(func() { common.MemoryCacheEnabled = oldMemoryCache })
	priority := int64(0)
	weight := uint(0)
	channels := []Channel{
		{Id: 21, Name: "quarantined", Status: common.ChannelStatusQuarantined, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight},
		{Id: 22, Name: "enabled", Status: common.ChannelStatusEnabled, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight},
	}
	require.NoError(t, DB.Create(&channels).Error)
	require.NoError(t, DB.Create(&Ability{Group: "default", Model: "model-a", ChannelId: 21, Enabled: false, Priority: &priority, Weight: 0}).Error)
	require.NoError(t, DB.Create(&Ability{Group: "default", Model: "model-a", ChannelId: 22, Enabled: true, Priority: &priority, Weight: 0}).Error)

	channel, err := GetChannel("default", "model-a", 0)

	require.NoError(t, err)
	require.NotNil(t, channel)
	require.Equal(t, 22, channel.Id)
}

func TestUpdateChannelStatusCanPersistQuarantineAndDisableAbilities(t *testing.T) {
	resetChannelSelectionTestTables(t)
	oldMemoryCache := common.MemoryCacheEnabled
	common.MemoryCacheEnabled = true
	t.Cleanup(func() { common.MemoryCacheEnabled = oldMemoryCache })
	priority := int64(0)
	weight := uint(0)
	channel := Channel{Id: 23, Name: "to-quarantine", Status: common.ChannelStatusEnabled, Models: "model-a", Group: "default", Priority: &priority, Weight: &weight}
	require.NoError(t, DB.Create(&channel).Error)
	require.NoError(t, DB.Create(&Ability{Group: "default", Model: "model-a", ChannelId: 23, Enabled: true, Priority: &priority, Weight: 0}).Error)
	InitChannelCache()

	updated := UpdateChannelStatus(23, "", common.ChannelStatusQuarantined, "Authorization failed")

	require.True(t, updated)
	var reloaded Channel
	require.NoError(t, DB.First(&reloaded, 23).Error)
	require.Equal(t, common.ChannelStatusQuarantined, reloaded.Status)
	info := reloaded.GetOtherInfo()
	require.Equal(t, "Authorization failed", info["status_reason"])
	var ability Ability
	require.NoError(t, DB.First(&ability, "channel_id = ?", 23).Error)
	require.False(t, ability.Enabled)
}
