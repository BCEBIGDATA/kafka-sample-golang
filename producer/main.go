package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
)

var (
	broker        = flag.String("broker", "kafka.bj.baidubce.com:9091", "The broker address can be found in https://cloud.baidu.com/doc/Kafka/QuickGuide.html#.E7.BB.BC.E8.BF.B0")
	topic         = flag.String("topic", "", "Required, the topic to consume from.  You can create a topic from console.")
	enableTLS     = flag.Bool("enableTLS", true, "TLS is required to access Baidu Kafka service.")
	clientPemPath = flag.String("client_pem", "client.pem", "File path to client.pem provided in kafka-key.zip from console.")
	clientKeyPath = flag.String("client_key", "client.key", "File path to client.key provided in kafka-key.zip from console.")
	caPemPath     = flag.String("ca_pem", "ca.pem", "File path to ca.pem provided in kafka-key.zip from console.")
)

func main() {
	if len(os.Args[1:]) == 0 {
		flag.Usage()
		os.Exit(1)		
	}

	flag.Parse()

	if *topic == "" {
		panic("Argument topic is required.")
	}

	sarama.Logger = log.New(os.Stderr, "[sarama]", log.LstdFlags)

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Net.TLS.Enable = *enableTLS
	if *enableTLS {
		config.Net.TLS.Config = configTLS()
	}

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer([]string{*broker}, config)
	if err != nil {
		log.Panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	numOfRecords := 10
	for i := 0; i < numOfRecords; i++ {
		message := &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.StringEncoder(fmt.Sprintf("%d-hello kafka", i)),
		}
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("message sent, topic:", *topic, "partition:", partition, "offset:", offset)
	}
}

func configTLS() (t *tls.Config) {
	checkFile(*clientPemPath)
	checkFile(*clientKeyPath)
	checkFile(*caPemPath)

	clientPem, err := tls.LoadX509KeyPair(*clientPemPath, *clientKeyPath)
	if err != nil {
		log.Panic(err)
	}

	caPem, err := ioutil.ReadFile(*caPemPath)
	if err != nil {
		log.Panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caPem)
	t = &tls.Config{
		Certificates:       []tls.Certificate{clientPem},
		RootCAs:            certPool,
		InsecureSkipVerify: true,
	}

	return t
}

func checkFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	if stat.Size() == 0 {
		panic("Please replace " + path + " with your own. ")
	}

}
