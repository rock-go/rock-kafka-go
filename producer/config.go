package producer

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/node"
	"strings"
)

const (
	INIT = iota + 1
	START
	CLOSE
	ERROR
	OK
)

type config struct {
	name        string
	key         string //not used
	addr        string // 192.168.1.1:9092,192.168.1.2:9092
	timeout     int    // not used
	topic       string
	num         int // 每个线程每次发送的数据条数
	flush       int // 强制发送数据间隔时长
	buffer      int // 缓冲区大小
	thread      int
	limit       int
	require_ack int

	compression int // 压缩方式, GZIP,LZ4,None,Snappy,ZSTD
	heartbeat   int // 心跳检测周期
}

func def() *config {
	return &config{
		key: node.ID(),
		num: 100,
		flush: 5,
		buffer: 4096,
		thread: 5,
		limit: 0,
		require_ack: 1,
		compression: 1,//gzip
		heartbeat: 5,
	}
}

func newConfig(L *lua.LState) *config {
	tab := L.CheckTable(1)
	cfg := def()
	tab.Range(func(key string, val lua.LValue) {
		cfg.setField(L , key , val)
	})

	if e := cfg.verify(); e != nil {
		L.RaiseError("v", e)
		return nil
	}

	return cfg
}

func (cfg *config) setField(L *lua.LState , key string , val lua.LValue) {
	switch key {
	case "name":
		cfg.name = val.String()
	case "addr":
		cfg.addr = val.String()
	case "topic":
		cfg.topic = val.String()
	case "compression":
		cfg.compression = auxlib.LValueToInt(val, 1)

	case "timeout":
		cfg.timeout = auxlib.LValueToInt(val, 1)
	case "num":
		cfg.num = auxlib.LValueToInt(val, 10)
	case "flush":
		cfg.flush = auxlib.LValueToInt(val, 5)
	case "buffer":
		cfg.buffer = auxlib.LValueToInt(val, 4096)
	case "thread":
		cfg.thread = auxlib.LValueToInt(val, 5)
	case "limit":
		cfg.limit = auxlib.LValueToInt(val, 0)
	case "heartbeat":
		cfg.heartbeat = auxlib.LValueToInt(val, 5)

	default:
		L.RaiseError("not found kafka producer %s config", key)
		return
	}
}

func (cfg *config) verify() error {
	if e := auxlib.Name(cfg.name); e != nil {
		return e
	}

	if cfg.Compression() == -1 {
		return errors.New("invalid compress codec")
	}

	return nil
}

func (cfg *config) Compression() sarama.CompressionCodec {
	v := sarama.CompressionCodec(cfg.compression)

	switch v {
	case sarama.CompressionNone,
		sarama.CompressionGZIP,
		sarama.CompressionLZ4,
		sarama.CompressionSnappy,
		sarama.CompressionZSTD:
		return v

	default:
		return -1
	}
}

func (cfg *config) Addr() []string {
	return strings.Split(cfg.addr, ",")
}
