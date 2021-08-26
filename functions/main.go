package ttn

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"
	"firebase.google.com/go"
	"firebase.google.com/go/db"
)

var app *firebase.App
var database *db.Client
var bqClient *bigquery.Client
var projectID = os.Getenv("GCP_PROJECT")
var datasetID = "ttn_dataset"
var tableID = "raw_data"

func init() {
	ctx := context.Background()

	var err error

	bqClient, err = bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("error initializing bigquery client: %v\n", err)
	}
}

// HandleTTNUplink process uplink msg sent by TTN
func HandleTTNUplink(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("error reading body: %v\n", err.Error())
		sendErrorResponse(w, err.Error())
		return
	}
	log.Printf("received body %v\n", string(bodyBytes))

	// Restore the io.ReadCloser to its original state
	r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))

	var msg UplinkMessage
	err = json.NewDecoder(r.Body).Decode(&msg)

	if err != nil {
		log.Printf("error decoding body: %v\n", err.Error())
		sendErrorResponse(w, err.Error())
		return
	}

	log.Printf("Function received valid json %v\n", msg)

	deviceData := GetDeviceUpdate(msg)

	ctx := context.Background()

	u := bqClient.Dataset(datasetID).Table(tableID).Uploader()

	var temp float64 = msg.UplinkMessage.DecodedPayload.Message.DegreesC
	var humidity float64 = msg.UplinkMessage.DecodedPayload.Message.Humidity

	if (temp == 0 || humidity == 0) {
		temp = msg.UplinkMessage.DecodedPayload.Message.TempC_DS
		humidity = msg.UplinkMessage.DecodedPayload.Message.HumSHT

		log.Printf("Replacing temp and humidity %v\n", msg.UplinkMessage.DecodedPayload.Message)
	}

	log.Printf("Debugging time stamp %v\n", msg.UplinkMessage.Settings.Timestamp * 1000)
	log.Printf("Debugging time stamp %v\n", msg.UplinkMessage.Settings.Timestamp)

	rows := []*DeviceData{
		{DeviceID: msg.EndDeviceIds.DeviceID, Data: deviceData, Timestamp: msg.UplinkMessage.Settings.Timestamp, Temp: temp, Humidity: humidity},
	}

	err = u.Put(ctx, rows)
	if err != nil {
		log.Printf("error inserting on bigquery: %v", err.Error())
		sendErrorResponse(w, err.Error())
		return
	}

	log.Printf("Data Inserted on BigQuery\n")

	sendSuccessResponse(w)
	return
}
