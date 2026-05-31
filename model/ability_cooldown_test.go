package model

import (
	"one-api/common"
	"reflect"
	"testing"
)

func TestChannelAutoEnableModelListParsesAndTrims(t *testing.T) {
	old := common.ChannelAutoEnableModels
	common.ChannelAutoEnableModels = " deepseek-ai/deepseek-v4-pro,deepseek-ai/deepseek-v4-flash ,, "
	t.Cleanup(func() { common.ChannelAutoEnableModels = old })

	got := ChannelAutoEnableModelList()
	want := []string{"deepseek-ai/deepseek-v4-pro", "deepseek-ai/deepseek-v4-flash"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ChannelAutoEnableModelList() = %#v, want %#v", got, want)
	}
}
