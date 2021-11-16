package consumer

import "github.com/rock-go/rock/lua"

func (c *Consumer) Header(out lua.Printer) {
	out.Printf("type: %s", c.Type())
	out.Printf("uptime: %s", c.Uptime.Format("2006-01-02 15:04:05"))
	out.Printf("statue: %s", c.Status.String())
	out.Println("version: v1.0.0")
	out.Println("")
}

func (c *Consumer) Show(out lua.Printer) {
	c.Header(out)

	out.Printf("name: %s", c.cfg.name)
	out.Printf("addr: %s", c.cfg.addr)
	out.Printf("topic: %s", c.cfg.topic)
	out.Printf("group: %s", c.cfg.group)
	out.Printf("assignor: %s", c.cfg.assignor)
	out.Printf("offset: %s", c.cfg.offset)
	out.Printf("heartbeat: %d", c.cfg.heartbeat)
	out.Printf("buffer: %d", c.cfg.buffer)
	out.Printf("Thread: %d", c.cfg.thread)
	out.Println("")
}

func (c *Consumer) Help(out lua.Printer) {
	c.Header(out)
	out.Println(".start() 启动")
	out.Println(".close() 关闭")
}
