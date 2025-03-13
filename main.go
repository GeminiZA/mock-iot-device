package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
)

type config struct {
	MQTTBrokerIp     string
	MQTTBrokerPort   string
	MQTTUsername     string
	MQTTPassword     string
	MQTTUpdatesTopic string
	NumDevices       int
	RandomSeed       int64
	Rnd              *rand.Rand
	Mux              sync.Mutex
	Wg               sync.WaitGroup
}

func loadConfig() (*config, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, err
	}
	mqttBrokerHost := os.Getenv("MQTT_BROKER_HOST")
	if mqttBrokerHost == "" {
		return nil, fmt.Errorf("missing MQTT_BROKER_HOST in env")
	}
	ip := net.ParseIP(mqttBrokerHost)
	mqttBrokerIp := ""
	if ip == nil {
		ips, err := net.LookupIP(mqttBrokerHost)
		if err != nil {
			return nil, fmt.Errorf("unable to resolve MQTT_BROKER_HOST ip, %v, %s", err, mqttBrokerHost)
		}
		if len(ips) > 0 {
			mqttBrokerIp = ips[0].String()
		}
	} else {
		mqttBrokerIp = ip.String()
	}
	mqttBrokerPort := os.Getenv("MQTT_BROKER_PORT")
	if mqttBrokerPort == "" {
		return nil, fmt.Errorf("missing MQTT_BROKER_PORT in env")
	}
	mqttUsername := os.Getenv("MQTT_USERNAME")
	if mqttUsername == "" {
		return nil, fmt.Errorf("missing MQTT_USERNAME in env")
	}
	mqttPassword := os.Getenv("MQTT_PASSWORD")
	if mqttPassword == "" {
		return nil, fmt.Errorf("missing MQTT_PASSWORD in env")
	}
	mqttUpdatesTopic := os.Getenv("MQTT_UPDATES_TOPIC")
	if mqttUpdatesTopic == "" {
		return nil, fmt.Errorf("missing MQTT_UPDATES_TOPIC in env")
	}
	numDevicesStr := os.Getenv("NUM_DEVICES")
	if numDevicesStr == "" {
		return nil, fmt.Errorf("missing NUM_DEVICES in env")
	}
	numDevices, err := strconv.Atoi(numDevicesStr)
	if err != nil {
		return nil, err
	}
	randomSeedStr := os.Getenv("RANDOM_SEED")
	if randomSeedStr == "" {
		return nil, fmt.Errorf("missing RANDOM_SEED in env")
	}
	randomSeed, err := strconv.ParseInt(randomSeedStr, 10, 64)
	if err != nil {
		return nil, err
	}
	rndSrc := rand.NewSource(randomSeed)
	rnd := rand.New(rndSrc)
	return &config{
		MQTTBrokerIp:     mqttBrokerIp,
		MQTTBrokerPort:   mqttBrokerPort,
		MQTTUsername:     mqttUsername,
		MQTTPassword:     mqttPassword,
		MQTTUpdatesTopic: mqttUpdatesTopic,
		NumDevices:       int(numDevices),
		RandomSeed:       randomSeed,
		Rnd:              rnd,
	}, nil

}

type Device struct {
	running bool
	id      uint
}

func (cfg *config) GetRandUInt(a, b uint) uint {
	cfg.Mux.Lock()
	defer cfg.Mux.Unlock()
	return (uint(cfg.Rnd.Uint32()) % (b - a)) + a
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	logMsg(fmt.Sprintf("Starting %d devices with seed: %d...\n", cfg.NumDevices, cfg.RandomSeed), 0)
	for i := 1; i < cfg.NumDevices+1; i++ {
		cfg.Wg.Add(1)
		time.Sleep(time.Millisecond * time.Duration(cfg.GetRandUInt(100, 500)))
		go cfg.runDevice(uint(i), time.Second*30)
	}
	cfg.Wg.Wait()
	logMsg("All devices done", 0)
}

func (cfg *config) runDevice(id uint, duration time.Duration) {
	logMsg(fmt.Sprintf("%s - Starting device %d...\n", time.Now(), id), id)
	startTime := time.Now()
	clientId := fmt.Sprintf("device/%d", id)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("%s:%s", cfg.MQTTBrokerIp, cfg.MQTTBrokerPort))
	opts.SetClientID(clientId)
	opts.SetUsername(cfg.MQTTUsername)
	opts.SetPassword(cfg.MQTTPassword)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		fmt.Printf("MQTT Broker connection lost (%v) on device(%d)\n", err, id)
	})
	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		logMsg(fmt.Sprintf("Connecting to MQTT broker(%s)...", broker.String()), id)
		return tlsCfg
	})
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		logMsg(fmt.Sprintf("Connected to MQTT broker(%s:%s)", cfg.MQTTBrokerIp, cfg.MQTTBrokerPort), id)
	})
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(time.Second * 3) {
	}
	if err := token.Error(); err != nil {
		logMsg(fmt.Sprintf("Error connecting to broker: %v; stopping device", err), id)
		return
	}
	device := Device{
		running: true,
	}
	cfg.subscribe(id, client, &device)
	for startTime.Add(duration).After(time.Now()) {
		time.Sleep(time.Millisecond * time.Duration(cfg.GetRandUInt(1000, 2000)))
		msgStruct := struct {
			Status    string                 `json:"status"`
			Telemetry map[string]interface{} `json:"telemetry"`
		}{
			Status: "online",
			Telemetry: map[string]interface{}{
				"humidity":    cfg.GetRandUInt(20, 100),
				"temperature": cfg.GetRandUInt(0, 40) - 10,
				"time":        time.Now().String(),
			},
		}
		// sink values instead of waiting
		if device.running {
			msg, err := json.Marshal(msgStruct)
			if err != nil {
				logMsg(fmt.Sprintf("Error marshalling json: %v", err), id)
				continue
			}
			err = cfg.publishData(id, msg, client)
			if err != nil {
				logMsg(fmt.Sprintf("Error publishing data: %v", err), id)

			}
			logMsg(fmt.Sprintf("Published message: %s", string(msg)), id)
		}
	}
	logMsg("Device done, stopping", id)
	cfg.Wg.Done()
}

func (cfg *config) publishData(deviceId uint, msg []byte, c mqtt.Client) error {
	token := c.Publish(path.Join(cfg.MQTTUpdatesTopic, fmt.Sprintf("%d", deviceId)), 1, false, msg)
	for !token.WaitTimeout(500 * time.Millisecond) {
	}
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

func (cfg *config) subscribe(deviceId uint, c mqtt.Client, d *Device) error {
	token := c.Subscribe(path.Join(cfg.MQTTUpdatesTopic, fmt.Sprintf("%d", deviceId)), 1, d.msgCallback)
	for !token.WaitTimeout(500 * time.Millisecond) {
	}
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

func (d *Device) msgCallback(c mqtt.Client, msg mqtt.Message) {
	topicComponents := strings.Split(msg.Topic(), "/")
	if len(topicComponents) < 2 {
		logMsg(fmt.Sprintf("topic length incorrect expected: 2; got: %d", len(topicComponents)), d.id)
		return
	}
	deviceId, err := strconv.Atoi(topicComponents[len(topicComponents)-1])
	if err != nil || deviceId != int(d.id) {
		return
	}
	var msgStruct struct {
		Command string `json:"command"`
	}
	err = json.Unmarshal(msg.Payload(), &msgStruct)
	if err != nil {
		logMsg(fmt.Sprintf("Error unmarshalling json for messsage: %s", string(msg.Payload())), d.id)
		return
	}
	switch msgStruct.Command {
	case "stop":
		d.running = false
		logMsg("Got command, stopping", d.id)
	case "start":
		d.running = true
		logMsg("Got command, starting", d.id)
	default:
		return
	}
}

func logMsg(msg string, id uint) {
	fmt.Printf("%s - Device %d - %s\n", time.Now().String(), id, msg)
}
