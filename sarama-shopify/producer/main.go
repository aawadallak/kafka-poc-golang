package main

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"github.com/xeipuuv/gojsonschema"
)

type ComplexType struct {
	ID string `json:"id"`
}

func main() {
	client, err := NewClient()
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	id, _ := uuid.NewUUID()
	record := NewRecord(ComplexType{ID: id.String()})

	p, o, err := producer.SendMessage(record)
	if err != nil {
		panic(err)
	}

	log.Printf(
		"Sended message to topic: %s on partition %d on offset: %d\n", topicProducer, p, o)
}

func AvroResolver(schema *srclient.Schema, payload []byte) ([]byte, error) {
	native, _, err := schema.Codec().NativeFromTextual(payload)
	if err != nil {
		return nil, err
	}

	binary, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	return binary, nil
}

func JsonResolver(schema *srclient.Schema, payload []byte) ([]byte, error) {
	loader := gojsonschema.NewBytesLoader(payload)
	doc := gojsonschema.NewStringLoader(schema.Schema())
	result, err := gojsonschema.Validate(doc, loader)
	if err != nil || len(result.Errors()) > 0 {
		return nil, err
	}

	if !result.Valid() {
		return nil, errors.New("schema is not valid")
	}

	return payload, nil
}

func HandleSchema(schema *srclient.Schema, payload []byte) ([]byte, error) {
	switch schema.SchemaType().String() {
	case srclient.Avro.String():
		return AvroResolver(schema, payload)
	case srclient.Json.String():
		return JsonResolver(schema, payload)
	default:
		return nil, errors.New("invalid schema type")
	}
}

func NewRecord(payload any) *sarama.ProducerMessage {
	log.Println(payload)
	value, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	var record []byte
	record = append(record, byte(0))
	record = append(record, make([]byte, 4)...)
	record = append(record, value...)

	key, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topicProducer,
		Key:   sarama.StringEncoder(key.String()),
		Value: sarama.ByteEncoder(record),
	}

	return msg
}

func RetrieveSchema(subject string) (*srclient.Schema, error) {
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)
	schemaRegistryClient.SetCredentials(schemaBasicUser, schemaBasicPass)
	schema, err := schemaRegistryClient.GetLatestSchema(subject)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func NewClient() (sarama.Client, error) {
	cfg := sarama.NewConfig()
	cfg.Net.SASL.Enable = true
	cfg.Net.TLS.Enable = true
	cfg.Net.SASL.Mechanism = saslMechanisms
	cfg.Net.SASL.User = saslUsername
	cfg.Net.SASL.Password = saslPassword
	cfg.Producer.Return.Successes = true

	client, err := sarama.NewClient([]string{bootstrapServers}, cfg)
	if err != nil {
		return nil, err
	}

	return client, err
}
