package kafka_producer_sarama

import (
	"context"
	"io"
	"log"
	"sync"

	"git.intra.weibo.com/adx/logging"
	"github.com/Shopify/sarama"
)

var (
	mylog = log.New(io.Discard, "[Kafka Producer Sarama] ", log.LstdFlags)
)

func NewProducer(c *Config) (Producer, error) {
	err := c.check()
	if err != nil {
		return nil, err
	}

	if c.LogOut != nil {
		mylog.SetOutput(c.LogOut)
	}

	ap, err := sarama.NewAsyncProducer(c.Brokers, c.SaramaCfg)
	if err != nil {
		return nil, err
	}

	logCfg := &logging.LogConfig{
		File: &logging.LogFile{
			FileName:           c.FileName,
			LogPath:            c.LogPath,
			FileNameDateFormat: c.FileNameDateFormat,
			FileNameDateAlign:  !c.FileNameDateNotAlign,
			RotationDuration:   logging.Duration{Duration: c.RotationDuration},
			RotationCount:      c.RotationCount,
		},
	}

	mPer := &myProducer{
		cfg:           c,
		asyncProducer: ap,
		wg:            &sync.WaitGroup{},
		fileWriter:    logging.NewRawLoggerWithConfig(logCfg),
		msgChan:       make(chan *sarama.ProducerMessage, c.MsgChanCap),
	}
	mPer.ctx, mPer.cancel = context.WithCancel(context.Background())
	mPer.start()

	mylog.Println("topic", c.Topic, "producer started")

	return mPer, nil
}

func (p *myProducer) start() {
	for i := 0; i < p.cfg.WriteGoNum; i++ {
		p.wg.Add(1)
		go func(ip *myProducer, idx int) {
			defer ip.wg.Done()
			defer ip.cancel()
			for {
				select {
				case msg, ok := <-ip.msgChan:
					if !ok {
						mylog.Printf("all msg has been send, topic: %s, index: %d\n", ip.cfg.Topic, idx)
						return
					}
					ip.asyncProducer.Input() <- msg
				}
			}
		}(p, i)
	}

	if p.cfg.FailedMsgSaved {
		p.wg.Add(1)
		go func(ip *myProducer) {
			defer ip.wg.Done()
			for {
				select {
				case msg, ok := <-ip.asyncProducer.Errors():
					if !ok {
						return
					}
					if msg != nil && msg.Msg != nil || msg.Msg.Value != nil {
						v, _ := msg.Msg.Value.Encode()
						ip.fileWriter.Info(string(v))
						mylog.Printf("msg: %s, err: %s\n", string(v), msg.Error())
					}
				}
			}
		}(p)
	}

}

// Close 关闭消息管道，释放资源，该函数应该被调用，被调用后继续写入消息会 panic
func (p *myProducer) Close() {
	close(p.msgChan)             // 关闭消息管道，禁止继续写入消息
	<-p.ctx.Done()               // 等待消息管道中的消息已经全部写完，再去关闭生产者
	p.asyncProducer.AsyncClose() // 关闭生产者，此时会关闭错误消息管道
	p.wg.Wait()                  // 等待所有错误消息都被消费完毕
	mylog.Println("topic", p.cfg.Topic, "producer closed")
}

type Producer interface {
	Write(msg string)
	WriteWithKey(key, msg string) // 需要设置哈希方式，key 才会生效
	start()
	Close()
}

type myProducer struct {
	cfg           *Config
	ctx           context.Context
	cancel        context.CancelFunc
	asyncProducer sarama.AsyncProducer
	fileWriter    logging.Logger
	wg            *sync.WaitGroup
	msgChan       chan *sarama.ProducerMessage
}

func (p *myProducer) Write(msg string) {
	p.msgChan <- &sarama.ProducerMessage{Topic: p.cfg.Topic, Value: sarama.StringEncoder(msg)}
}
func (p *myProducer) WriteWithKey(key, msg string) {
	p.msgChan <- &sarama.ProducerMessage{Topic: p.cfg.Topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(msg)}
}
