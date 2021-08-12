package ttn

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"encoding/base64"
	"encoding/json"
	"github.com/TheThingsNetwork/go-cayenne-lib/cayennelpp"
	"time"
)

// UplinkMessage json struct that represents TTN Uplink message
// Autogenerated using https://mholt.github.io/json-to-go/
type UplinkMessage struct {
	EndDeviceIds struct {
		DeviceID       string `json:"device_id"`
		ApplicationIds struct {
			ApplicationID string `json:"application_id"`
		} `json:"application_ids"`
		DevEui  string `json:"dev_eui"`
		JoinEui string `json:"join_eui"`
		DevAddr string `json:"dev_addr"`
	} `json:"end_device_ids"`
	CorrelationIds []string  `json:"correlation_ids"`
	ReceivedAt     time.Time `json:"received_at"`
	UplinkMessage  struct {
		SessionKeyID   string `json:"session_key_id"`
		FPort          int    `json:"f_port"`
		FCnt           int    `json:"f_cnt"`
		FrmPayload     string `json:"frm_payload"`
		DecodedPayload struct {
			Message struct {
				DegreesC float64 `json:"degreesC"`
				Humidity float64 `json:"humidity"`
			} `json:"message"`
		} `json:"decoded_payload"`
		DecodedPayloadWarnings []interface{} `json:"decoded_payload_warnings"`
		RxMetadata             []struct {
			GatewayIds struct {
				GatewayID string `json:"gateway_id"`
				Eui       string `json:"eui"`
			} `json:"gateway_ids"`
			Timestamp    int64   `json:"timestamp"`
			Rssi         int     `json:"rssi"`
			ChannelRssi  int     `json:"channel_rssi"`
			Snr          float64 `json:"snr"`
			UplinkToken  string  `json:"uplink_token"`
			ChannelIndex int     `json:"channel_index"`
		} `json:"rx_metadata"`
		Settings struct {
			DataRate struct {
				Lora struct {
					Bandwidth       int `json:"bandwidth"`
					SpreadingFactor int `json:"spreading_factor"`
				} `json:"lora"`
			} `json:"data_rate"`
			CodingRate string `json:"coding_rate"`
			Frequency  string `json:"frequency"`
			Timestamp  int64  `json:"timestamp"`
		} `json:"settings"`
		ReceivedAt      time.Time `json:"received_at"`
		ConsumedAirtime string    `json:"consumed_airtime"`
		NetworkIds      struct {
			NetID     string `json:"net_id"`
			TenantID  string `json:"tenant_id"`
			ClusterID string `json:"cluster_id"`
		} `json:"network_ids"`
	} `json:"uplink_message"`
}

func parseDeviceData(payload string) map[string]interface{} {
	rawPayload, err := base64.StdEncoding.DecodeString(payload)

	if err != nil {
		return map[string]interface{}{
			"raw":   payload,
			"error": err.Error(),
		}
	}

	decoder := cayennelpp.NewDecoder(bytes.NewBuffer(rawPayload))
	target := NewUplinkTarget()

	err = decoder.DecodeUplink(target)

	if err != nil {
		return map[string]interface{}{
			"raw":   payload,
			"error": err.Error(),
		}
	}

	return target.GetValues()
}

// DeviceData represents a row item.
type DeviceData struct {
	DeviceID  string
	Data      map[string]interface{}
	Timestamp time.Time
}

// Save implements the ValueSaver interface.
func (dd *DeviceData) Save() (map[string]bigquery.Value, string, error) {
	data, err := json.Marshal(dd.Data)
	if err != nil {
		return nil, "", err
	}
	return map[string]bigquery.Value{
		"deviceId": dd.DeviceID,
		"data":     string(data),
		"time":     dd.Timestamp,
	}, "", nil
}

// GetDeviceUpdate convert uplink message into a firebase update map
func GetDeviceUpdate(msg UplinkMessage) map[string]interface{} {

	gateways := map[string]interface{}{}
	for _, gateway := range msg.UplinkMessage.RxMetadata.GatewayIds {
		gateways[gateway.GatewayID] = map[string]interface{}{
			"id":        gateway.GatewayID,
			"rssi":      msg.UplinkMessage.RxMetadata.Rssi,
			"snr":       msg.UplinkMessage.RxMetadata.Snr,
			"channel":   msg.UplinkMessage.RxMetadata.ChannelRssi,
			"time":      msg.UplinkMessage.RxMetadata.Timestamp,
			"latitude": 0,
			"longitude": 0,
			"altitude": 0,
		}
	}

	base := map[string]interface{}{
		"deviceId": msg.EndDeviceIds.DeviceID,
		"serial":   "",
		"data":     msg.UplinkMessage.DecodedPayload,
		"meta": map[string]interface{}{
			"updated":   msg.Settings.Timestamp,
			"frequency": msg.Settings.Frequency,
			"latitude":  0,
			"longitude": 0,
			"altitude":  0,
			"gateways":  gateways,
		},
	}

	return base
}
