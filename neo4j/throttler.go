package neo4j

import (
	"math/rand"
	"time"
)

type throttler time.Duration

func (t throttler) next() throttler {
	delay := time.Duration(t)
	const delayJitter = 0.2
	jitter := float64(delay) * delayJitter
	return throttler(delay - time.Duration(jitter) + time.Duration(2*jitter*rand.Float64()))
}

func (t throttler) delay() time.Duration {
	return time.Duration(t)
}
