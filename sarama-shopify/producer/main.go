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

const (
	bootstrapServers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
	securityProtocol = "SASL_SSL"
	saslMechanisms   = "PLAIN"
	saslUsername     = "2CXYJZKQ6HSN3SY7"
	saslPassword     = "OADcN5+1AILtj/cmAvjoarbg8Vi+CFxxQu44NPtmJWWxm/3Jpltic0CP9pbZPmNu"
	topicProducer    = "poc-kafka-schema-registry"

	schemaRegistryURL = "https://psrc-q8qx7.us-central1.gcp.confluent.cloud"
	schemaBasicUser   = "QOVTQR5SWF5WZKNF"
	schemaBasicPass   = "UxzuZ9rY/tSiKeTgz/TEZ7tUQ1hha+ZovzCYSwU8lg83WqvTLD2GbtlPyBQS1F9+"
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
	msg := ComplexType{ID: id.String()}

	schema, err := RetrieveSchema(topicProducer + "-value")
	if err != nil {
		panic(err)
	}

	payload, err := HandleSchema(schema, msg)
	if err != nil {
		panic(err)
	}

	record := NewRecord(payload)

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

func HandleSchema(schema *srclient.Schema, value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	switch schema.SchemaType().String() {
	case srclient.Avro.String():
		return AvroResolver(schema, payload)
	case srclient.Json.String():
		return JsonResolver(schema, payload)
	default:
		return nil, errors.New("invalid schema type")
	}
}

func NewRecord(payload []byte) *sarama.ProducerMessage {
	var record []byte
	record = append(record, byte(0))
	record = append(record, make([]byte, 4)...)
	record = append(record, payload...)

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
