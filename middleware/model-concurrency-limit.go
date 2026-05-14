package middleware

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/QuantumNous/new-api/common"
)

type modelConcurrencyLimiter struct {
	limit   int64
	current int64
}

var (
	modelConcurrencyOnce   sync.Once
	modelConcurrencyLimits map[string]*modelConcurrencyLimiter
)

func loadModelConcurrencyLimits() {
	modelConcurrencyLimits = map[string]*modelConcurrencyLimiter{}
	raw := strings.TrimSpace(os.Getenv("MODEL_CONCURRENCY_LIMITS"))
	if raw == "" {
		return
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sep := strings.LastIndex(part, ":")
		if sep < 0 {
			sep = strings.LastIndex(part, "=")
		}
		if sep <= 0 || sep >= len(part)-1 {
			common.SysError(fmt.Sprintf("invalid MODEL_CONCURRENCY_LIMITS item: %s", part))
			continue
		}
		model := strings.TrimSpace(part[:sep])
		limit, err := strconv.ParseInt(strings.TrimSpace(part[sep+1:]), 10, 64)
		if err != nil || limit <= 0 || model == "" {
			common.SysError(fmt.Sprintf("invalid MODEL_CONCURRENCY_LIMITS item: %s", part))
			continue
		}
		modelConcurrencyLimits[model] = &modelConcurrencyLimiter{limit: limit}
	}
	if len(modelConcurrencyLimits) > 0 {
		common.SysLog(fmt.Sprintf("model concurrency limits enabled: %s", raw))
	}
}

func acquireModelConcurrency(model string) (release func(), rejected bool, current int64, limit int64) {
	modelConcurrencyOnce.Do(loadModelConcurrencyLimits)
	limiter := modelConcurrencyLimits[model]
	if limiter == nil {
		return nil, false, 0, 0
	}
	current = atomic.AddInt64(&limiter.current, 1)
	if current > limiter.limit {
		atomic.AddInt64(&limiter.current, -1)
		return nil, true, current - 1, limiter.limit
	}
	return func() {
		atomic.AddInt64(&limiter.current, -1)
	}, false, current, limiter.limit
}
