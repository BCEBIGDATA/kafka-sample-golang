package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
)

var (
	broker        = flag.String("broker", "kafka.bj.baidubce.com:9091", "The broker address can be found in https://cloud.baidu.com/doc/Kafka/QuickGuide.html")
	topic         = flag.String("topic", "", "Required, the topic to consume from.  You can create a topic from console.")
	enableTLS     = flag.Bool("enableTLS", true, "TLS is  to access Baidu Kafka service.")
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

	consumer, err := sarama.NewConsumer([]string{*broker}, config)
	if err != nil {
		log.Panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	partitions, err := consumer.Partitions(*topic)
	if err != nil {
		log.Panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for _, partition := range partitions {
		log.Println("consume partition:", partition)

		shutdown := make(chan os.Signal, 1)
		signal.Notify(shutdown, os.Interrupt)

		go func(partition int32) {
			partitionConsumer, err := consumer.ConsumePartition(*topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalln(err)
				return
			}

			defer func() {
				wg.Done()
				if err := partitionConsumer.Close(); err != nil {
					log.Fatalln(err)
				}
			}()

		ConsumerLoop:
			for {
				select {
				case err := <-partitionConsumer.Errors():
					if err != nil {
						log.Fatalln("error:", err)
					}
				case msg := <-partitionConsumer.Messages():
					if msg != nil {
						log.Println("topic:", msg.Topic, "partition:", msg.Partition, "offset:", msg.Offset, "message:", string(msg.Value))
					}
				case <-shutdown:
					log.Println("stop consuming partition:", partition)
					break ConsumerLoop
				}
			}
		}(partition)
	}

	wg.Wait()
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
