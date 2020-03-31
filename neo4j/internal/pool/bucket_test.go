package pool

import (
	"testing"
)

func TestBucket(ot *testing.T) {
	ot.Run("reg/unreg/size", func(t *testing.T) {
		b := &bucket{}
		if b.size != 0 {
			t.Error("not 0")
		}

		// Register should increase size
		c1 := &fakeConn{}
		b.reg(c1)
		if b.size != 1 {
			t.Error("not 1")
		}
		c2 := &fakeConn{}
		b.reg(c2)
		if b.size != 2 {
			t.Error("not 2")
		}

		// Unregister should decrease size
		b.unreg(c2)
		if b.size != 1 {
			t.Error("not 1")
		}
		b.unreg(c1)
		if b.size != 0 {
			t.Error("not 0")
		}
	})

	ot.Run("get/ret", func(t *testing.T) {
		b := &bucket{}
		c1 := &fakeConn{}
		b.reg(c1)
		b.ret(c1)

		c2 := b.get()
		if c2 == nil {
			t.Error("did not get")
		}
		c3 := b.get()
		if c3 != nil {
			t.Error("did get")
		}
		b.ret(c2)
		c3 = b.get()
		if c3 == nil {
			t.Error("did not get")
		}
	})
}
