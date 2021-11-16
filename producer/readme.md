# 说明
	作者： Suncle
	描述： 利用gopher-lua的userdata，解决与其他调用模块的耦合性。
	时间： 2020-10-11
---
# # install

```bash
go get github.com/edunx/kafka
```

# Usage

 kafka 主要是为了独立是实现 kafka的client的功能 。

### 配置选项

```lua
local userdata = rock.kafka.producer{
        name="error-kafka",
        addr="127.0.0.1:9092",
        timeout=60,
        topic="topic",
        num=1000,
        flush=10,
        buffer=4096,
        thread=10,
        limit=5000,
        compression="gzip",
        heartbeat=10,
    }
```

- name ： 是userdata的底层标识  可以提供日志输出 时候的标识

- addr：kafka的地址 多一个用逗号分割 如127.0.0.1:9092,127.0.01:9092

- timeout:  暂时 没用

- topic:  kafka 中topic字段

- num:  一次发送数据条数 默认 100

- flush:  刷新缓存时间 默认5s  单位 秒

- buffer: 缓冲区大小  默认 4096 byte

- thread: 发送线程数 默认5

- limit:   发送限速器 默认0  代表不限速  默认时间是秒

- compression: 开启压缩  gzip、lz4、snappy、zstd 、none ， 默认：none   <font style="color:red">注意：能减少网络IO但是会增加cpu使用率 ;  </font>

- hreatbeat: 默认心跳检测kafka存活时间

    ****

### 配置函数

```lua
local ud = rock.kafka{}

ud.reload() --重启kafka进程
ud.stop()   --关闭kafka进程
ud.start()  --启动kafka进程

-- kafka 里所有的配置都可以通过 userdata.field = "" 的方式修改
ud.name = "new-name" 
```

### go 内部调用

```golang
    import (
        kafka "github.com/ednux/rock-kafka-go"
    )

    //注入lua api
    kafka.LuaInjectApi(L , rock)

    //满足transport.Tunnel 接口
    obj.Push( v )
}

```

### 注意
```lua
  local ud = rock.kafka.producer{}
  --会自动启动kafka线程
```
