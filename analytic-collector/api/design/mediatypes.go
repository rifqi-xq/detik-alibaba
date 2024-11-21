package design

import (
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var DetikErrorMedia = MediaType("application/vnd.detik.error+json", func(){
	ContentType("application/json")
	Attribute("code", String, "Message ID", func(){
		Example("UNAUTHORIZED")
	})
	Attribute("msg", String, "Localized message", func(){
		Example("Unauthorized access")
	})

	View("default", func(){
		Attribute("code")
		Attribute("msg")
	})

	Required("code", "msg")
})

var VersionMedia = MediaType("application/vnd.detik.collector.version+json", func(){
	ContentType("application/json")
	Attribute("version", String, "Application version", func() {
		Example("1.0")
	})
	Attribute("git", String, "Git commit hash", func() {
		Example("000000")
	})
	View("default", func() {
		Attribute("version")
		Attribute("git")
	})
	Required("version")
})