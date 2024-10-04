package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func main() {

	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Balancer: &kafka.LeastBytes{},
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "teste",
		GroupID:  "teste",
		MaxBytes: 10e6})

	// w.WriteMessages(context.Background(), kafka.Message{Value: []byte("HEYAAA"), Topic: "teste"})
	// w.WriteMessages(context.Background(), kafka.Message{Value: []byte("HEYAAA-retry"), Topic: "teste-retry-1"})

	fmt.Println("Message sent")

	spawnListeners[string](context.Background(), r, w, func(msg kafka.Message) error {
		fmt.Println(msg)
		return fmt.Errorf("")
	}, nil, nil, "teste", false, 10, 10, "retry", "dlt")
}

func RetryWithDLT(r *kafka.Reader, w *kafka.Writer) (kafka.Message, error) {
	return r.FetchMessage(context.Background())
}

type kfn func(msg kafka.Message) error

var errs map[error]bool

var mRetry int = 10

const retryNumKey = "retry-num"

type KafkaConfig struct {
	brokers           string
	groupId           string
	maxBytes          int
	isBlocking        bool
	numRetryListeners int
	retryPrefix       string
	dltPrefix         string
}

func NewKafkaConfig() *KafkaConfig {
	return &KafkaConfig{}
}

// var tpch chan string = make(chan string)
var sigs chan os.Signal = make(chan os.Signal, 1)
var errChan chan error = make(chan error)

func KafkaListener[T any](ctx context.Context, r *kafka.Reader, w *kafka.Writer, fn kfn, fndlt kfn, erros []error, defalutTopic string, topic string, isBlocking bool, nRetryListeners int, retry int, rtyPrefix string, dltPrefix string) {
	defer func() {
		errChan <- fmt.Errorf("")
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Its over")
			return
		default:
			msg, err := r.FetchMessage(ctx)
			fmt.Println(msg.Topic)
			if err != nil {
				fmt.Println(err)
				fmt.Println("ERROR HERE")
				errChan <- fmt.Errorf("")
				continue
			}

			var msgT T

			json.Unmarshal(msg.Value, &msgT)

			err = fn(msg)

			if isDlt(msg.Headers) {
				fndlt(msg)
				continue
			}

			_, existsErr := errs[err]

			if existsErr || (err != nil && isDlt(msg.Headers)) {
				r.CommitMessages(ctx, msg)
				continue
			}

			//Kafka non blocking topics
			if err != nil && !existsErr && !isBlocking {
				msg := configureRetry(msg, defalutTopic, rtyPrefix, dltPrefix)
				err := w.WriteMessages(ctx, msg)
				fmt.Println(err)
				r.CommitMessages(ctx, msg)
				continue
			}

			if !existsErr && isBlocking {
				for rty := 0; rty >= mRetry; rty++ {
					time.Sleep(5 * time.Second)
					if err := fn(msg); err == nil {
						r.CommitMessages(ctx, msg)
						break
					}
				}
				continue
			}

			if err != nil && existsErr {
				return
			}

			//If there is no error it will comit the message
			if err := r.CommitMessages(context.Background(), msg); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func configureRetry(msg kafka.Message, topic string, rtyPrefix string, dltPrefix string) kafka.Message {
	if len(msg.Headers) != 0 {
		for _, h := range msg.Headers {
			if h.Key == retryNumKey {
				v, err := strconv.Atoi(string(h.Value))
				if err != nil {
					panic(err)
				}

				if v <= mRetry {
					v++
					return kafka.Message{Value: msg.Value, Topic: fmt.Sprintf("%s-%s-%d", topic, rtyPrefix, v), Headers: []kafka.Header{{Key: "retry-num", Value: []byte(strconv.Itoa(v))}}}
				} else {
					msg.Topic = fmt.Sprintf("%s-%s", topic, dltPrefix)
					msg.Headers = append(msg.Headers, protocol.Header{Key: "DLT", Value: []byte("true")})
				}
			}
		}
	}

	msg.Headers = append(msg.Headers, protocol.Header{Key: retryNumKey, Value: []byte("1")})
	msg.Topic = fmt.Sprintf("%s-%s-%d", topic, rtyPrefix, 1)

	return kafka.Message{Value: msg.Value, Topic: fmt.Sprintf("%s-%s-%d", topic, rtyPrefix, 1), Headers: []kafka.Header{{Key: "retry-num", Value: []byte(strconv.Itoa(1))}}}
}

func isDlt(headers []kafka.Header) bool {
	for _, h := range headers {
		if h.Key == "DLT" {
			return true
		}
	}
	return false
}

func spawnListeners[T any](ctx context.Context, r *kafka.Reader, w *kafka.Writer, fn kfn, fndlt kfn, erros []error, topic string, isBlocking bool, nRetryListeners int, retry int, rtyPrefix string, dltPrefix string) {
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i <= nRetryListeners; i++ {
		tp := topic
		if i > 0 {
			tp = fmt.Sprintf("%s-%s-%d", tp, rtyPrefix, i)
		}

		if i == 0 {
			kafka.DialLeader(context.Background(), "tcp", "localhost", fmt.Sprintf("%s-%s", topic, dltPrefix), 0)
		}

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    tp,
			GroupID:  "teste",
			MaxBytes: 10e6})

		kafka.DialLeader(context.Background(), "tcp", "localhost:9092", tp, 0)

		go KafkaListener[T](ctx, r, w, fn, fndlt, erros, topic, tp, isBlocking, retry, nRetryListeners, rtyPrefix, dltPrefix)
	}

	select {
	case <-errChan:
		cancel()
	}

	time.Sleep(1 * time.Second)

}
