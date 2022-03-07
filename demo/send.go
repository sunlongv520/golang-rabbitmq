package main

import (
	"fmt"
	_ "fmt"
	"github.com/ichunt2019/golang-rbmq-sl/utils/rabbitmq"
)

func main() {


	for i := 0;i<2000;i++{
		body := fmt.Sprintf("{\"order_id\":%d}",i)
		fmt.Println(body)

		/**
			使用默认的交换机
			如果是默认交换机
			type QueueExchange struct {
			QuName  string           // 队列名称
			RtKey   string           // key值
			ExName  string           // 交换机名称
			ExType  string           // 交换机类型
			Dns     string			  //链接地址
			}
			如果你喜欢使用默认交换机
			RtKey  此处建议填写成 RtKey 和 QuName 一样的值
		 */

		queueExchange := rabbitmq.QueueExchange{
			"a_test_0001",
			"a_test_0001",
			"hello_go",
			"direct",
			"amqp://guest:guest@192.168.1.169:5672/",
		}

		/*
		 使用自定义的交换机 发送延时消息
		 */
		//queueExchange := rabbitmq.QueueExchange{
		//	"a_test_0001",
		//	"a_test_0001",
		//	"hello_go",
		//	"direct",
		//	"amqp://guest:guest@192.168.1.252:5672/",
		//}

		_ = rabbitmq.Send(queueExchange,body)


		//ttl 秒 发送延时消息
		//_ = rabbitmq.SendDelay(queueExchange,body,20)
		//fmt.Println(err)


	}


}