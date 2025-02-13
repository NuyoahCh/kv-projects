// Package utils
// @Author NuyoahCh
// @Date 2025/2/12 23:13
// @Desc
package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestKey(i)))
	}
}

func TestRandomValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(RandomValue(10)))
	}
}
