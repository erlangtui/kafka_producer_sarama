package kafka_producer_sarama

import (
	"errors"
	"io"
	"time"

	"github.com/Shopify/sarama"
)

// Config 对于下列选填配置，要么严格按照给定的格式填写，要么不填，否则配置无效
type Config struct {
	SaramaCfg            *sarama.Config
	Brokers              []string      // 必填，kafka 节点
	Topic                string        // 必填，生产主题
	LogOut               io.Writer     // 选填，日志的输出位置
	WriteGoNum           int           // 选填，往kafka写消息的生产协程数，缺省时默认 10，0 值默认为缺省
	MsgChanCap           int           // 选填，消息管道的容量，缺省时默认 10000，0 值默认为缺省。如果调用方 panic 最多丢失 10000 条数据；如果不想丢失消息，建议设为 -1，创建阻塞管道
	FailedMsgSaved       bool          // 选填，失败消息是否落文件
	FileName             string        // 选填，失败消息保存文件名，缺省为 "{topic}"
	LogPath              string        // 选填，失败消息文件保存路径，缺省为 "{topic}"
	FileNameDateFormat   string        // 选填，失败消息文件名格式，缺省为 "20060102.150405"
	FileNameDateNotAlign bool          // 选填，失败消息文件名是否不按 RotationDuration 对齐，缺省为 false，即对齐
	RotationDuration     time.Duration // 选填，文件滚动周期，缺省时为 24h
	RotationCount        int           // 选填，文件滚动数量，缺省时为 3
}

func (c *Config) check() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if len(c.Brokers) == 0 {
		return errors.New("config brokers is empty")
	}
	if c.Topic == "" {
		return errors.New("config topic is empty")
	}
	if c.SaramaCfg == nil {
		c.SaramaCfg = sarama.NewConfig()
		c.SaramaCfg.Metadata.RefreshFrequency = time.Minute
	}
	if c.WriteGoNum <= 0 {
		c.WriteGoNum = 10
	}
	if c.MsgChanCap == 0 {
		c.MsgChanCap = 10000
	} else if c.MsgChanCap < 0 {
		c.MsgChanCap = 0
	}
	if c.FailedMsgSaved {
		if !c.SaramaCfg.Producer.Return.Errors {
			return errors.New("config conflict, save error msg to file, SaramaCfg.Producer.Return.Errors must be true")
		}
		if c.FileName == "" {
			c.FileName = c.Topic
		}
		if c.LogPath == "" {
			c.LogPath = c.Topic
		}
	}
	return nil
}
