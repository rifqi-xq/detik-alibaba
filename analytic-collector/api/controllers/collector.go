package controllers

import (
	"compress/gzip"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/inconshreveable/log15"

	"analytic-collector/services"

	"github.com/goadesign/goa"
	"analytic-collector/api/app"
)

var trackerImage = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x21, 0xF9, 0x04,
	0x01, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02,
}

// CollectorController implements the collector resource.
type CollectorController struct {
	*goa.Controller

	appService services.ServiceInterface
}

// NewCollectorController creates a collector controller.
func NewCollectorController(service *goa.Service, appService services.ServiceInterface) *CollectorController {
	return &CollectorController{Controller: service.NewController("CollectorController"), appService: appService}
}

// CollectDtm runs the collectDtm action.
func (c *CollectorController) CollectDtm(ctx *app.CollectDtmCollectorContext) error {
	// CollectorController_CollectDtm: start_implement
	log15.Info("entering collect dtm")

	var query map[string][]string
	query = ctx.Params

	if len(query) == 0 {
		return ctx.OK(trackerImage)
	}

	// get detikId from cookies
	dTs, err := ctx.Cookie("dts-")
	dTssec, errdTssec := ctx.Cookie("dts-sec")
	if err == nil && dTs.Value != "" {
		query["D_TS"] = []string{dTs.Value}
	} else if errdTssec == nil {
		query["D_TS"] = []string{dTssec.Value}
	}

	// get dtma from cookies for handle AMP Page
	dtma, errDtma := ctx.Cookie("__dtma")
	if errDtma == nil && dtma.Value != "" {
		query["dtma"] = []string{dtma.Value}
	}

	// get session-notif from cookies
	sessionNotif, err := ctx.Cookie("session-notif")
	if err == nil {
		query["session-notif"] = []string{sessionNotif.Value}
	}

	ga, err := ctx.Cookie("_ga")
	if err == nil {
		query["ga"] = []string{ga.Value}
	}

	query["entry-time"] = []string{strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)}

	// get UserAgent
	query["useragent"] = []string{ctx.UserAgent()}

	// Loop through headers
	for name, headers := range ctx.RequestData.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			query["header-"+strings.ToLower(name)] = []string{h}
		}
	}
	// register to collector's job queue
	c.appService.AddCollectorGETJob(query)

	return ctx.OK(trackerImage)
	// CollectorController_CollectDtm: end_implement
}

// CollectPost runs the collectPost action.
func (c *CollectorController) CollectPost(ctx *app.CollectPostCollectorContext) error {
	// CollectorController_CollectPost: start_implement

	var objmap map[string]*json.RawMessage
	var decoder *json.Decoder
	switch ctx.RequestData.Header.Get("Content-Encoding") {
	case "gzip":
		gz, err := gzip.NewReader(ctx.Body)
		if err != nil {
			log15.Error("failed to parse gzip file", "err", err)
			return ctx.BadRequest(&app.DetikError{Msg: "failed to parse gzip file"})
		}
		defer ctx.Body.Close()
		defer gz.Close()
		decoder = json.NewDecoder(gz)
	default:
		decoder = json.NewDecoder(ctx.Body)
		defer ctx.Body.Close()
	}

	err := decoder.Decode(&objmap)
	if err != nil {
		log15.Error(err.Error())
		return ctx.BadRequest(&app.DetikError{Msg: "failed to parse json"})
	}

	// get detikId from cookies
	dTs, err := ctx.Cookie("dts-")
	if err == nil {
		data := []byte(`"` + dTs.Value + `"`)
		js := json.RawMessage(data)
		objmap["D_TS"] = &js

	}

	// get UserAgent
	uaData := []byte(`"` + ctx.UserAgent() + `"`)
	js := json.RawMessage(uaData)
	objmap["useragent"] = &js

	// header
	castHeaderName := map[string]string{}
	for key, val := range ctx.RequestData.Header {
		if len(val) > 0 {
			castHeaderName[strings.Replace(key, "-", "_", -1)] = val[0]
		}
	}
	castHeaderName["Logged_Time"] = strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	castHeaderName["Entry_Time"] = strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	headerMap, err := json.Marshal(castHeaderName)
	if err == nil {
		headerJS := json.RawMessage(headerMap)
		objmap["header"] = &headerJS

	}

	jsResult, err := json.Marshal(objmap)
	if err != nil {
		log15.Error(err.Error())
		return ctx.BadRequest(&app.DetikError{Msg: "failed to parse to json"})
	}

	c.appService.AddCollectorPOSTJob(string(jsResult))

	return ctx.NoContent()
	// CollectorController_CollectPost: end_implement
}
