package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/rock-go/rock/logger"
	"sync/atomic"
	"time"
)

type Thread struct {
	cfg *config

	id     int
	status int

	consumer sarama.ConsumerGroup

	msgChan *chan []byte

	ctx    context.Context
	cancel context.CancelFunc
	total  *uint64

	tk *time.Ticker
}

func NewConsumerThread(id int, cons *Consumer) Thread {
	ct := Thread{
		cfg:     cons.cfg,
		id:      id,
		msgChan: &cons.buffer,
		status:  START,
		total:   &cons.recv,
	}
	ct.ctx, ct.cancel = context.WithCancel(cons.ctx)

	return ct
}

func (t *Thread) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (t *Thread) Cleanup(session sarama.ConsumerGroupSession) error {

	return nil
}

func (t *Thread) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger.Debugf("the current consumer thread id is %d", t.id)
	for message := range claim.Messages() {
		select {
		case <-t.ctx.Done():
			return nil
		default:
			if t.isBlock() {
				return nil
			}

			*(t.msgChan) <- message.Value
			atomic.AddUint64(t.total, 1)
			session.MarkMessage(message, "")
		}
	}
	return nil
}

// 判断缓冲区是否阻塞, true:阻塞，且接收到退出指令；false:未阻塞或阻塞但未收到退出指令，进入到下一步
func (t *Thread) isBlock() bool {
START:
	// 缓冲区未满
	if len(*t.msgChan) < t.cfg.buffer {
		return false
	}

	// 缓冲区已满，需判断是否有关闭线程的指令，有则等待通道数据消费完成或超时，停止写入并关闭线程；没有则等待。
	for {
		select {
		case <-t.ctx.Done():
			i := 0
			for {
				// 缓冲区消费完成，退出
				if len(*t.msgChan) == 0 {
					return true
				}
				time.Sleep(1 * time.Second)
				i++
				if i >= 10 {
					// 超时
					return true
				}
			}

		case <-t.tk.C:
			goto START
		}
	}
}

func (t *Thread) Start() error {
	var err error
	scfg := sarama.NewConfig()
	scfg.Consumer.Return.Errors = true
	scfg.ChannelBufferSize = 20000
	scfg.Consumer.Group.Rebalance.Strategy = t.cfg.Ass()
	scfg.Consumer.Offsets.Initial = t.cfg.Offset()

	t.consumer, err = sarama.NewConsumerGroup(t.cfg.Addr(), t.cfg.group, scfg)
	if err != nil {
		logger.Errorf("%s kafka Thread.id=%d create consumer client error: %v", t.cfg.name, t.id, err)
		return err
	}
	go t.Handler(t.ctx)

	t.status = OK
	t.tk = time.NewTicker(1 * time.Second)
	//pub.Out.Info("%s kafka Thread.id=%d Start consumer ok", cfg.cfg.name)

	logger.Errorf("%s kafka Thread.id=%d Start consumer ok", t.cfg.name, t.id)
	return nil
}

func (t *Thread) Handler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Errorf("%s kafka Thread.id=%d consume exit", t.cfg.name, t.id)
			return
		default:
			if err := t.consumer.Consume(ctx, t.cfg.topic, t); err != nil {
				logger.Errorf("%s kafka Thread.id=%d consume error: %v", t.cfg.name, t.id, err)
			}
		}
	}
}

func (t *Thread) Close() {
	t.cancel()
	t.status = CLOSE
	if err := t.consumer.Close(); err != nil {
		logger.Errorf("%s kafka Thread.id=%d Close error: %v", t.cfg.name, t.id, err)
		return
	}

	t.tk.Stop()
	logger.Errorf("%s kafka Thread.id=%d Close successfully", t.cfg.name, t.id)
}
