package consumer

import (
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"reflect"
)

var kafkaConsumerTypeOf = reflect.TypeOf((*Consumer)(nil)).String()

func (c *Consumer) start(L *lua.LState) int {
	if c.IsRun() {
		L.RaiseError("%s is running", c.Name())
		return 0
	}

	err := c.Start()
	if err != nil {
		L.RaiseError("%s Start error: %v", c.Name(), err)
	}

	return 0
}

func (c *Consumer) Line(L *lua.LState) int {
	line := <-c.buffer
	out := lua.CheckPrinter(L)
	if out != nil {
		out.Println(lua.B2S(line))
	}
	L.Push(lua.LString(line))
	return 1
}

func (c *Consumer) close(L *lua.LState) int {
	err := c.Close()
	if err != nil {
		L.RaiseError("%s Close error: %v", c.Name(), err)
	}

	return 0
}

func (c *Consumer) json(L *lua.LState) int {
	data, err := c.ToJson()
	if err != nil {
		L.RaiseError("%s get json config error: %v", c.Name(), err)
		return 0
	}

	L.Push(lua.LString(data))
	return 1
}

func (c *Consumer) Index(L *lua.LState, key string) lua.LValue {
	if key == "start" {
		return L.NewFunction(c.start)
	}
	if key == "close" {
		return L.NewFunction(c.close)
	}
	if key == "json" {
		return L.NewFunction(c.json)
	}

	if key == "line" {
		return L.NewFunction(c.Line)
	}

	return lua.LNil
}

func (c *Consumer) NewIndex(L *lua.LState, key string, val lua.LValue) {
	switch key {
	case "addr":
		c.cfg.addr = lua.CheckString(L, val)
	case "topic":
		c.cfg.topic = auxlib.CheckSlice(val)
	case "group":
		c.cfg.group = lua.CheckString(L, val)
	case "assignor":
		c.cfg.assignor = lua.CheckString(L, val)
	case "offset":
		c.cfg.offset = lua.CheckString(L, val)
	case "heartbeat":
		c.cfg.heartbeat = lua.CheckInt(L, val)
	case "thread":
		c.cfg.thread = lua.CheckInt(L, val)
	case "buffer":
		c.cfg.buffer = lua.CheckInt(L, val)
	default:
		L.RaiseError("invalid field name: %s", key)
	}
}

func newLuaConsumer(L *lua.LState) int {
	cfg := newConfig(L)

	// 新建
	proc := L.NewProc(cfg.name, kafkaConsumerTypeOf)
	if proc.IsNil() {
		proc.Set(newConsumer(cfg))
	} else {
		proc.Value.(*Consumer).cfg = cfg
	}

	L.Push(proc)
	return 1
}

func LuaInjectApi(G lua.UserKV) {
	G.Set("consumer", lua.NewFunction(newLuaConsumer))
}
