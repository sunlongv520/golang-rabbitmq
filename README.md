## go get github.com/ichunt2019/golang-rbmq-sl

**发送消息**

详见demo/send.go



**消费消息**

详见demo/recv.go





**说明：**


```
rabbitmq.Recv(rabbitmq.QueueExchange{
		"a_test_0001",
		"a_test_0001",
		"hello_go",
		"direct",
		"amqp://guest:guest@192.168.2.232:5672/",
	},t,3)
```



第一个参数 QueueExchange说明

```

	
// 定义队列交换机对象
type QueueExchange struct {
	QuName  string           // 队列名称
	RtKey   string           // key值
	ExName  string           // 交换机名称
	ExType  string           // 交换机类型
	Dns     string           //链接地址
}

```


第二个参数 type Receiver interface说明

| Consumer | FailAction |
| ------- | ------- |
|拿到消息后，用户可以处理任务，如果消费成功 返回nil即可，如果处理失败，返回一个自定义error即可         |      由于消息内部自带消息失败尝试3次机制，3次如果失败后就没必要一直存储在mq，所以此处扩展，可以用作消息补偿和告警     |





```
// 定义接收者接口
type Receiver interface {
	Consumer([]byte)    error
	FailAction(error ,[]byte)  error
}
```


第三个参数：runNusm


| runNusm |
| ------- | 
|   消息并发数，同时可以处理多少任务 普通任务 设置为1即可   需要并发的设置成3-5即可      |



# 支持rabbitmq断线重连
























