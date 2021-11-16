package consumer

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"strings"
)

const (
	START = iota
	CLOSE
	ERROR
	OK
)

type config struct {
	name      string
	addr      string
	topic     []string
	group     string
	assignor  string // consumer group partition分配策略：range, roundrobin, sticky
	offset    string // oldest or newest
	buffer    int
	heartbeat int
	thread    int

	handler Handler // 预留字段，kafka消费者调用其他组件处理消息
}

func newConfig(L *lua.LState) *config {
	tab := L.CheckTable(1)
	cfg := &config{}

	tab.ForEach(func(key lua.LValue, val lua.LValue) {
		switch key.String() {
		case "name":
			cfg.name = val.String()

		case "addr":
			cfg.addr = auxlib.CheckSockets(val, L)

		case "topic":
			cfg.topic = auxlib.CheckSlice(val)
		case "group":
			cfg.group = auxlib.LValueToStr(val, "null")

		case "assignor":
			cfg.assignor = auxlib.LValueToStr(val, "range")

		case "offset":
			cfg.heartbeat = auxlib.LValueToInt(val, 5)

		case "thread":
			cfg.thread = auxlib.LValueToInt(val, 1)

		case "buffer":
			cfg.buffer = auxlib.LValueToInt(val, 4096)

		case "heartbeat":
			cfg.heartbeat = auxlib.LValueToInt(val, 10)

		case "handler":
			//goto
			return

		default:
			L.RaiseError("invalid consumer %s key", key.String())
			return
		}
	})

	if e := cfg.verify(); e != nil {
		return nil
	}
	return cfg
}

func (cfg *config) verify() error {
	if e := auxlib.Name(cfg.name); e != nil {
		return e
	}

	if cfg.topic == nil {
		return errors.New("not found topic")
	}

	return nil
}

func (cfg *config) Ass() sarama.BalanceStrategy {
	switch cfg.assignor {
	case "sticky":
		return sarama.BalanceStrategySticky
	case "round_robin":
		return sarama.BalanceStrategyRoundRobin
	default:
		return sarama.BalanceStrategyRange
	}
}

func (cfg *config) Offset() int64 {

	if cfg.offset == "oldest" {
		return sarama.OffsetOldest
	}

	return sarama.OffsetNewest
}

func (cfg *config) Addr() []string {
	return strings.Split(cfg.addr, ",")
}

type Handler interface {
	Handle([]byte) error // 处理消费数据的接口
}
