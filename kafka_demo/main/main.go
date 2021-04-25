package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)


var Address = []string{"127.0.0.1:9092"}

func main()  {
	syncProducer(Address)
	//asyncProducer1(Address)


	//topic := []string{"test"}
	//var wg = &sync.WaitGroup{}
	//wg.Add(2)
	////广播式消费：消费者1
	//go clusterConsumer(wg, Address, topic, "group-1")
	////广播式消费：消费者2
	//go clusterConsumer(wg, Address, topic, "group-2")
	//
	//wg.Wait()
}

//同步消息模式
func syncProducer(address []string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Timeout = 5 * time.Second
	p, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Printf("sarama.NewSyncProducer err, message=%s \n", err)
		return
	}
	defer p.Close()
	topic := "test"
	srcValue := "sync: this is a message. index=%d"
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf(srcValue, i)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}
		part, offset, err := p.SendMessage(msg)
		if err != nil {
			log.Printf("send message(%s) err=%s \n", value, err)
		} else {
			fmt.Fprintf(os.Stdout, value+"发送成功，partition=%d, offset=%d \n", part, offset)
		}
		time.Sleep(2 * time.Second)
	}
}

func SaramaProducer()  {

	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	//随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//设置使用的kafka版本,如果低于V0_10_0_0版本,消息中的timestrap没有作用.需要消费和生产同时配置
	//注意，版本设置不对的话，kafka会返回很奇怪的错误，并且无法成功发送消息
	config.Version = sarama.V0_10_0_1

	fmt.Println("start make producer")
	//使用配置,新建一个异步生产者
	producer, e := sarama.NewAsyncProducer([]string{"127.0.0.1:6667"}, config)
	if e != nil {
		fmt.Println(e)
		return
	}
	defer producer.AsyncClose()

	//循环判断哪个通道发送过来数据.
	fmt.Println("start goroutine")
	go func(p sarama.AsyncProducer) {
		for{
			select {
			case  <-p.Successes():
				//fmt.Println("offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
			case fail := <-p.Errors():
				fmt.Println("err: ", fail.Err)
			}
		}
	}(producer)

	var value string
	for i:=0;;i++ {
		time.Sleep(500*time.Millisecond)
		time11:=time.Now()
		value = "this is a message 0606 "+time11.Format("15:04:05")

		// 发送的消息,主题。
		// 注意：这里的msg必须得是新构建的变量，不然你会发现发送过去的消息内容都是一样的，因为批次发送消息的关系。
		msg := &sarama.ProducerMessage{
			Topic: "0606_test",
		}

		//将字符串转化为字节数组
		msg.Value = sarama.ByteEncoder(value)
		//fmt.Println(value)

		//使用通道发送
		producer.Input() <- msg
	}
}

// 支持brokers cluster的消费者
func clusterConsumer(wg *sync.WaitGroup,brokers, topics []string, groupId string)  {
	defer wg.Done()
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// init consumer
	consumer, err := cluster.NewConsumer(brokers, groupId, topics, config)
	if err != nil {
		log.Printf("%s: sarama.NewSyncProducer err, message=%s \n", groupId, err)
		return
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("%s:Error: %s\n", groupId, err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("%s:Rebalanced: %+v \n", groupId, ntf)
		}
	}()

	// consume messages, watch signals
	var successes int
Loop:
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s:%s/%d/%d\t%s\t%s\n", groupId, msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "")  // mark message as processed
				successes++
			}
		case <-signals:
			break Loop
		}
	}
	fmt.Fprintf(os.Stdout, "%s consume %d messages \n", groupId, successes)
}