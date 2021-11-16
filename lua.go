package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/rock-go/rock-kafka-go/consumer"
	"github.com/rock-go/rock-kafka-go/producer"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/xcall"
)

func LuaInjectApi(env xcall.Env) {
	kfk := lua.NewUserKV()

	kfk.Set("none", lua.LNumber(sarama.CompressionNone))
	kfk.Set("gzip", lua.LNumber(sarama.CompressionGZIP))
	kfk.Set("snappy", lua.LNumber(sarama.CompressionSnappy))
	kfk.Set("lz4", lua.LNumber(sarama.CompressionLZ4))
	kfk.Set("zstd", lua.LNumber(sarama.CompressionZSTD))

	kfk.Set("wait_for_all", lua.LNumber(sarama.WaitForAll))
	kfk.Set("wait_for_local", lua.LNumber(sarama.WaitForLocal))
	kfk.Set("no_response", lua.LNumber(sarama.NoResponse))

	producer.LuaInjectApi(kfk)
	consumer.LuaInjectApi(kfk)
	env.SetGlobal("kafka", kfk)
}
