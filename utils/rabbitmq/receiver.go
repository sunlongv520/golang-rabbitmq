package rabbitmq

import (
	"errors"
	"strconv"
	"time"

	//"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)


// 定义全局变量,指针类型
var mqConn *amqp.Connection
var mqChan *amqp.Channel

// 定义生产者接口
type Producer interface {
	MsgContent() string
}

// 定义生产者接口
type RetryProducer interface {
	MsgContent() string
}

// 定义接收者接口
type Receiver interface {
	Consumer([]byte)    error
	FailAction(error , []byte)  error
}

// 定义RabbitMQ对象
type RabbitMQ struct {
	connection *amqp.Connection
	Channel *amqp.Channel
	dns string
	QueueName   string            // 队列名称
	RoutingKey  string            // key名称
	ExchangeName string           // 交换机名称
	ExchangeType string           // 交换机类型
	producerList []Producer
	retryProducerList []RetryProducer
	receiverList []Receiver
}

// 定义队列交换机对象
type QueueExchange struct {
	QuName  string           // 队列名称
	RtKey   string           // key值
	ExName  string           // 交换机名称
	ExType  string           // 交换机类型
	Dns     string			  //链接地址
}



// 链接rabbitMQ
func (r *RabbitMQ)MqConnect() (err error){

	mqConn, err = amqp.Dial(r.dns)
	r.connection = mqConn   // 赋值给RabbitMQ对象

	if err != nil {
		fmt.Printf("rbmq链接失败  :%s \n", err)
	}

	return
}

// 关闭mq链接
func (r *RabbitMQ)CloseMqConnect() (err error){

	err = r.connection.Close()
	if err != nil{
		fmt.Printf("关闭mq链接失败  :%s \n", err)
	}
	return
}

// 链接rabbitMQ
func (r *RabbitMQ)MqOpenChannel() (err error){
	mqConn := r.connection
	r.Channel, err = mqConn.Channel()
	//defer mqChan.Close()
	if err != nil {
		fmt.Printf("MQ打开管道失败:%s \n", err)
	}
	return err
}

// 链接rabbitMQ
func (r *RabbitMQ)CloseMqChannel() (err error){
	r.Channel.Close()
	if err != nil {
		fmt.Printf("关闭mq链接失败  :%s \n", err)
	}
	return err
}




// 创建一个新的操作对象
func NewMq(q QueueExchange) RabbitMQ {
	return RabbitMQ{
		QueueName:q.QuName,
		RoutingKey:q.RtKey,
		ExchangeName: q.ExName,
		ExchangeType: q.ExType,
		dns:q.Dns,
	}
}

func (mq *RabbitMQ) sendMsg (body string) (err error)  {
	err = mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}

	defer func() {
		_ = mq.Channel.Close()
	}()
	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}


	// 用于检查队列是否存在,已经存在不需要重复声明
	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}
	// 绑定任务
	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}

	if mq.ExchangeName != "" && mq.RoutingKey != ""{
		err = mq.Channel.Publish(
			mq.ExchangeName,     // exchange
			mq.RoutingKey, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
				ContentType: "text/plain",
				Body:        []byte(body),
				DeliveryMode: 2,
			})
	}else{
		err = mq.Channel.Publish(
			"",     // exchange
			mq.QueueName, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing {
				ContentType: "text/plain",
				Body:        []byte(body),
				DeliveryMode: 2,
			})
	}
	return

}


/*
发送延时消息
 */
func (mq *RabbitMQ)sendDelayMsg(body string,ttl int64) (err error){
	err =mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}
	defer mq.Channel.Close()

	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			return
		}
	}


	if ttl <= 0{
		return errors.New("发送延时消息，ttl参数是必须的")
	}

	table := make(map[string]interface{},3)
	table["x-dead-letter-routing-key"] = mq.RoutingKey
	table["x-dead-letter-exchange"] = mq.ExchangeName
	table["x-message-ttl"] = ttl*1000

	//fmt.Printf("%+v",table)
	//fmt.Printf("%+v",mq)
	// 用于检查队列是否存在,已经存在不需要重复声明
	ttlstring := strconv.FormatInt(ttl,10)
	queueName := fmt.Sprintf("%s_delay_%s",mq.QueueName ,ttlstring)
	routingKey := fmt.Sprintf("%s_delay_%s",mq.QueueName ,ttlstring)
	_, err = ch.QueueDeclare(queueName, true, false, false, false, table)
	if err != nil {
		return
	}
	// 绑定任务
	if routingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(queueName, routingKey, mq.ExchangeName, false, nil)
		if err != nil {
			return
		}
	}

	header := make(map[string]interface{},1)

	header["retry_nums"] = 0

	var ttl_exchange string
	var ttl_routkey string

	if(mq.ExchangeName != "" ){
		ttl_exchange = mq.ExchangeName
	}else{
		ttl_exchange = ""
	}


	if mq.RoutingKey != "" && mq.ExchangeName != ""{
		ttl_routkey = routingKey
	}else{
		ttl_routkey = queueName
	}

	err = mq.Channel.Publish(
		ttl_exchange,     // exchange
		ttl_routkey, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(body),
			Headers:header,
		})
	if err != nil {
		return

	}
	return
}


func (mq *RabbitMQ) sendRetryMsg (body string,retry_nums int32,args ...string)  {
	err :=mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}
	defer mq.Channel.Close()

	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}

	//原始路由key
	oldRoutingKey := args[0]
	//原始交换机名
	oldExchangeName := args[1]

	table := make(map[string]interface{},3)
	table["x-dead-letter-routing-key"] = oldRoutingKey
	if oldExchangeName != "" {
		table["x-dead-letter-exchange"] = oldExchangeName
	}else{
		mq.ExchangeName = ""
		table["x-dead-letter-exchange"] = ""
	}

	table["x-message-ttl"] = int64(20000)

	//fmt.Printf("%+v",table)
	//fmt.Printf("%+v",mq)
	// 用于检查队列是否存在,已经存在不需要重复声明
	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, table)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}
	// 绑定任务
	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}

	header := make(map[string]interface{},1)

	header["retry_nums"] = retry_nums + int32(1)

	var ttl_exchange string
	var ttl_routkey string

	if(mq.ExchangeName != "" ){
		ttl_exchange = mq.ExchangeName
	}else{
		ttl_exchange = ""
	}


	if mq.RoutingKey != "" && mq.ExchangeName != ""{
		ttl_routkey = mq.RoutingKey
	}else{
		ttl_routkey = mq.QueueName
	}

	//fmt.Printf("ttl_exchange:%s,ttl_routkey:%s \n",ttl_exchange,ttl_routkey)
	err = mq.Channel.Publish(
		ttl_exchange,     // exchange
		ttl_routkey, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(body),
			Headers:header,
		})
	if err != nil {
		fmt.Printf("MQ任务发送失败:%s \n", err)

	}

}


// 监听接收者接收任务 消费者
func (mq *RabbitMQ) ListenReceiver(receiver Receiver) {
	err :=mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil{
		log.Printf("Channel err  :%s \n", err)
	}
	defer mq.Channel.Close()
	if mq.ExchangeName != "" {
		if mq.ExchangeType == ""{
			mq.ExchangeType = "direct"
		}
		err =  ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}


	// 用于检查队列是否存在,已经存在不需要重复声明
	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}
	// 绑定任务
	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}
	// 获取消费通道,确保rabbitMQ一个一个发送消息
	err =  ch.Qos(1, 0, false)
	msgList, err :=  ch.Consume(mq.QueueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Consume err :%s \n", err)
	}
	for msg := range msgList {
		retry_nums,ok := msg.Headers["retry_nums"].(int32)
		if(!ok){
			retry_nums = int32(0)
		}
		// 处理数据
		err := receiver.Consumer(msg.Body)
		if err!=nil {
			//消息处理失败 进入延时尝试机制
			if retry_nums < 3{
				fmt.Println(string(msg.Body))
				fmt.Printf("消息处理失败 消息开始进入尝试  ttl延时队列 \n")
				retry_msg(msg.Body,retry_nums,QueueExchange{
						mq.QueueName,
						mq.RoutingKey,
						mq.ExchangeName,
						mq.ExchangeType,
						mq.dns,
					})
			}else{
				//消息失败 入库db
				fmt.Printf("消息处理3次后还是失败了 入库db 钉钉告警 \n")
				receiver.FailAction(err,msg.Body)
			}
			err = msg.Ack(true)
			if err != nil {
				fmt.Printf("确认消息未完成异常:%s \n", err)
			}
		}else {
			// 确认消息,必须为false
			err = msg.Ack(true)

			if err != nil {
				fmt.Printf("消息消费ack失败 err :%s \n", err)
			}
		}

	}
}

//消息处理失败之后 延时尝试
func retry_msg(msg []byte,retry_nums int32,queueExchange QueueExchange){
	//原始队列名称 交换机名称
	oldQName := queueExchange.QuName
	oldExchangeName := queueExchange.ExName
	oldRoutingKey := queueExchange.RtKey
	if oldRoutingKey == "" || oldExchangeName == ""{
		oldRoutingKey = oldQName
	}

	if queueExchange.QuName != "" {
		queueExchange.QuName = queueExchange.QuName + "_retry_3";
	}

	if queueExchange.RtKey != "" {
		queueExchange.RtKey = queueExchange.RtKey + "_retry_3";
	}else{
		queueExchange.RtKey = queueExchange.QuName + "_retry_3";
	}

//fmt.Printf("%+v",queueExchange)

	mq := NewMq(queueExchange)
	_ = mq.MqConnect()

	defer func(){
		_ = mq.CloseMqConnect()
	}()
	//fmt.Printf("%+v",queueExchange)
	mq.sendRetryMsg(string(msg),retry_nums,oldRoutingKey,oldExchangeName)


}


func Send(queueExchange QueueExchange,msg string) (err error){
	mq := NewMq(queueExchange)
	err = mq.MqConnect()
	if err != nil{
		return
	}

	defer func(){
		_ = mq.CloseMqConnect()
	}()

	err = mq.sendMsg(msg)

	return
}

//发送延时消息
func SendDelay(queueExchange QueueExchange,msg string,ttl int64)(err error){
	mq := NewMq(queueExchange)
	err = mq.MqConnect()
	if err != nil{
		return
	}
	defer func(){
		_ = mq.CloseMqConnect()
	}()
	err = mq.sendDelayMsg(msg,ttl)
	return
}


/*
runNums  开启并发执行任务数量
 */
func Recv(queueExchange QueueExchange,receiver Receiver,otherParams ...int) (err error){
	var (
		exitTask bool
		maxTryConnNums int  //rbmq链接失败后多久尝试一次
		runNums int
		maxTryConnTimeFromMinute int
	)

	if(len(otherParams) <= 0){
		runNums = 1
		maxTryConnTimeFromMinute = 0
	}else if(len(otherParams) == 1){
		runNums = otherParams[0]
		maxTryConnTimeFromMinute = 0
	}else if(len(otherParams) == 2){
		runNums = otherParams[0]
		maxTryConnTimeFromMinute = otherParams[1]
	}


	//maxTryConnNums := 360 //rbmq链接失败后最大尝试次数
	//maxTryConnTime := time.Duration(10) //rbmq链接失败后多久尝试一次
	maxTryConnNums = maxTryConnTimeFromMinute * 10 * maxTryConnTimeFromMinute//rbmq链接失败后最大尝试次数
	maxTryConnTime := time.Duration(6) //rbmq链接失败后多久尝试一次
	mq := NewMq(queueExchange)
	//链接rabbitMQ
	err = mq.MqConnect()
	if(err != nil){
		return
	}

	defer func() {
		if panicErr := recover(); panicErr != nil{
			fmt.Println(recover())
			err = errors.New(fmt.Sprintf("%s",panicErr))
		}
	}()

	//rbmq断开链接后 协程退出释放信号
	taskQuit:= make(chan struct{}, 1)
	//尝试链接rbmq
	tryToLinkC := make(chan struct{}, 1)

	//最大尝试次数
	tryToLinkMaxNums := make(chan struct{}, 1)

	maxTryNums := 0 //尝试重启次数

	//开始执行任务
	for i:=1;i<=runNums;i++{
		go Recv2(mq,receiver,taskQuit);
	}

	//如果rbmq断开连接后 尝试重新建立链接
	var tryToLink = func() {
		for {
			maxTryNums += 1
			err = mq.MqConnect()
			if(err == nil){
				tryToLinkC <- struct{}{}
				break
			}
			if(maxTryNums > maxTryConnNums){
				tryToLinkMaxNums <- struct{}{}
				break
			}
			//如果链接断开了 10秒重新尝试链接一次
			time.Sleep(time.Second * maxTryConnTime)
		}
		return
	}
	scheduleTimer := time.NewTimer(time.Millisecond*300)
	exitTask = true
	for{
		select {
		case <-tryToLinkC: //建立链接成功后 重新开启协程执行任务
			fmt.Println("重新开启新的协程执行任务")
			go Recv2(mq,receiver,taskQuit);
		case <-tryToLinkMaxNums://rbmq超出最大链接次数 退出任务
			fmt.Println("rbmq链接超过最大尝试次数!")
			exitTask = false
			err = errors.New("rbmq链接超过最大尝试次数!")
		case <- taskQuit ://rbmq断开连接后 开始尝试重新建立链接
			fmt.Println("rbmq断开连接后 开始尝试重新建立链接")
			 go tryToLink()
		case <- scheduleTimer.C:
			//fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~")
		}
		// 重置调度间隔
		scheduleTimer.Reset(time.Millisecond*300)
		if !exitTask{
			break
		}
	}
	fmt.Println("exit")
	return
}


func Recv2(mq RabbitMQ,receiver Receiver,taskQuit chan<- struct{}){
		defer func() {
			fmt.Println("rbmq链接失败,协程任务退出~~~~~~~~~~~~~~~~~~~~")
			taskQuit <- struct{}{}
			return
		}()
		// 验证链接是否正常
		err := mq.MqOpenChannel()
		if(err != nil){
			return
		}
		mq.ListenReceiver(receiver)
}


type retryPro struct {
	msgContent   string
}












