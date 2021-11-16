package producer

import (
	"github.com/Shopify/sarama"
	"github.com/rock-go/rock/lua"
)

func (p *Producer) Header(out lua.Printer) {
	out.Printf("type: %s", p.Type())
	out.Printf("uptime: %s", p.uptime.Format("2006-01-02 15:04:06"))
	out.Printf("version: v1.29.1")
	out.Println("")
}

func (p *Producer) Show(out lua.Printer) {
	p.Header(out)
	out.Printf("name = %s", p.cfg.name)
	out.Printf("addr = %s", p.cfg.addr)
	out.Printf("topic = %s", p.cfg.topic)
	out.Printf("thread = %d", p.cfg.thread)
	out.Printf("timeout = %d", p.cfg.timeout)
	out.Printf("key = %s", p.cfg.key)
	out.Printf("num = %d", p.cfg.num)
	out.Printf("flush = %d", p.cfg.flush)
	out.Printf("limit = %d", p.cfg.limit)
	out.Printf("compression = %s", sarama.CompressionCodec(p.cfg.compression).String())
}

func (p *Producer) Help(out lua.Printer) {
	p.Header(out)
	out.Println(".push( string )  发送数据")
}
