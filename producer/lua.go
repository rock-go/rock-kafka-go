package producer

import (
	"github.com/rock-go/rock/lua"
	"reflect"
)

var (
	KFK_PRODUCER = reflect.TypeOf((*Producer)(nil)).String()
)

func checkLuaPrinter(L *lua.LState) lua.Printer {
	if L.B == nil {
		return nil
	}

	printer, ok := L.B.(lua.Printer)
	if ok {
		return printer
	}

	return nil
}

func (p *Producer) LPush(L *lua.LState) int {
	out := checkLuaPrinter(L)

	n := L.GetTop()
	if n <= 0 {
		return 0
	}

	for i := 1; i <= n; i++ {
		v := L.Get(i)
		p.Push(v.String())

		if out != nil {
			out.Printf("%s push succeed", v)
		}

	}
	return 0
}

func (p *Producer) LTotal(L *lua.LState) int {
	v := p.Total()
	out := lua.CheckPrinter(L)
	if out != nil {
		out.Printf("%s total: %d", p.Name(), v)
	}
	L.Push(lua.LNumber(v))
	return 1
}

func (p *Producer) Index(L *lua.LState, key string) lua.LValue {
	if key == "push" {
		return L.NewFunction(p.LPush)
	}
	if key == "total" {
		return L.NewFunction(p.LTotal)
	}

	return lua.LNil
}

func (p *Producer) NewIndex(L *lua.LState , key string , val lua.LValue) {
	if key == "thread" { p.cfg.thread = lua.CheckInt(L , val) }
}

func newLuaProducer(L *lua.LState) int {
	cfg := newConfig(L)

	proc := L.NewProc(cfg.name, KFK_PRODUCER)
	if proc.IsNil() {
		proc.Set(newProducer(cfg))
	} else {
		proc.Value.(*Producer).cfg = cfg
	}

	L.Push(proc)
	return 1
}

func LuaInjectApi(uv lua.UserKV) {
	uv.Set("producer", lua.NewFunction(newLuaProducer))
}
