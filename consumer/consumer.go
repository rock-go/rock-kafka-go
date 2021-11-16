package consumer

import (
	"context"
	"github.com/rock-go/rock/json"
	"github.com/rock-go/rock/logger"
	"github.com/rock-go/rock/lua"
	"time"
)

type Consumer struct {
	lua.Super

	cfg  *config
	recv uint64

	thread []Thread

	ctx    context.Context
	cancel context.CancelFunc
	buffer chan []byte
}

func newConsumer(cfg *config) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}

	c.Status = lua.INIT
	c.TypeOf = kafkaConsumerTypeOf

	return c
}

func (c *Consumer) Start() error {
	c.thread = make([]Thread, c.cfg.thread)
	c.buffer = make(chan []byte, c.cfg.buffer)

	for i := 0; i < c.cfg.thread; i++ {
		c.thread[i] = NewConsumerThread(i, c)
		go c.thread[i].Start()
	}

	go c.Heartbeat()
	return nil
}

func (c *Consumer) State() lua.LightUserDataStatus {
	if c.thread == nil {
		return lua.CLOSE
	}

	inactive := 0
	for _, v := range c.thread {
		if v.status != OK {
			inactive++
		}
	}

	if inactive == c.cfg.thread {
		return lua.CLOSE
	}

	return lua.RUN
}

func (c *Consumer) Ping() {
	for id, t := range c.thread {
		switch t.status {
		case OK:
			continue
		case CLOSE:
			logger.Infof("%s kafka consumer Thread.id= %d Close", t.cfg.name, id)
			c.Close()
		case ERROR:
			go c.thread[id].Start()
		}
	}
}

func (c *Consumer) Heartbeat() {
	tk := time.NewTicker(time.Second * time.Duration(c.cfg.heartbeat))
	defer tk.Stop()

	for {
		select {
		case <-c.ctx.Done():
			logger.Errorf("%s kafka consumer heartbeat exit", c.cfg.name)
			return
		case <-tk.C:
			c.Ping()
			logger.Debugf("%s kafka consumed %d messages", c.cfg.name, c.recv)
		}
	}
}

func (c *Consumer) Close() error {
	c.cancel()

	for _, t := range c.thread {
		t.Close()
	}

	close(c.buffer)
	c.Status = lua.CLOSE
	return nil
}

func (c *Consumer) Reload() {
	logger.Errorf("%s kafka consumer Close for reloading", c.cfg.name)
	c.Close()

	if err := c.Start(); err != nil {
		logger.Errorf("%s kafka consumer Start error for reload: %v", c.cfg.name, err)
	}
}

func (c *Consumer) Name() string {
	return c.cfg.name
}

func (c *Consumer) ToJson() ([]byte, error) {
	buff := json.NewBuffer()
	buff.Tab("kafka")
	buff.KV("type" , "consumer")
	buff.KV("name", c.cfg.name)
	buff.KV("addr", c.cfg.addr)
	//buff.WriteKV("topic", cfg.cfg.topic, false)
	buff.KV("group", c.cfg.group)
	buff.KV("assignor", c.cfg.assignor)
	buff.KV("offset", c.cfg.offset)
	buff.KI("heartbeat", c.cfg.heartbeat)
	buff.KI("Thread", c.cfg.thread)
	buff.End("}")

	return buff.Bytes(), nil
}

// GetBuffer 返回buffer地址
func (c *Consumer) GetBuffer() *chan []byte {
	return &c.buffer
}

func (c *Consumer) GetName() string {
	return c.cfg.name
}
