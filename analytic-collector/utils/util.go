package utils

import (
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/inconshreveable/log15"
)

var (
	// EnvNodeName is an environment variable for Node Name
	EnvNodeName = "ANALYTIC_COLLECTOR_NODE_NAME"
)

// String convert string to pointer
func String(data string) *string {
	return &data
}

// Float64 convert float64 to pointer
func Float64(data float64) *float64 {
	return &data
}

// Int convert int to pointer
func Int(data int) *int {
	return &data
}

// JakartaTime return Jakarta Time
func JakartaTime() time.Time {
	loc, _ := time.LoadLocation("Asia/Jakarta")
	return time.Now().In(loc)
}

// GetDetikID ..
func GetDetikID(dts string) string {
	log15.Debug("before: ", dts)
	decodedValue, err := url.QueryUnescape(dts)
	if err != nil {
		log15.Error("Err GetDetikID ", "decoding", err)
		return ""
	}
	re := regexp.MustCompile("&ui=[0-9]*")
	value := re.FindString(decodedValue)

	ui := strings.Split(value, "=")
	if len(ui) > 1 {
		log15.Debug("after: ", ui)
		if ui[1] != "" {
			return ui[1]
		}
	}
	return ""
}

// GetGAID ..
func GetGAID(ga string) string {
	log15.Debug("before: ", ga)

	ui := strings.Split(ga, ".")
	if len(ui) >= 4 {
		log15.Debug("after: ", ui)
		if ui[2] != "" {
			return ui[2] + "." + ui[3]
		}
	}
	return ""
}
