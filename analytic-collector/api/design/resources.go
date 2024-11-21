package design

import (
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

// swagger
var _ = Resource("swagger", func() {
	NoSecurity()
	Origin("*", func() {
		Methods("GET")
	})
	Files("/detikanalytic/swagger.json", "api/swagger/swagger.json")
})

// version
var VersionType = Type("version", func() {
	Attribute("version", String, "Application version", func() {
		Example("1.0")
	})
	Attribute("git", String, "Git commit hash", func() {
		Example("000000")
	})

	Required("version")
})

var _ = Resource("version", func() {
	Action("version", func() {
		Routing(GET("version"))
		Response(OK, VersionMedia)
		Metadata("swagger:summary", "Return application's version and commit hash")
	})
})

var ArticleCustomParamType = Type("articleCustomParamType", func() {
	Attribute("key", String, "Custom param key", func() {
		Example("pagetype")
	})
	Attribute("value", String, "Custom param value", func() {
		Example("multiplepaging")
	})
})

var ArticleType = Type("articleMedia", func() {
	Attribute("articleId", String, "Article ID", func() {
		Example("123456")
	})
	Attribute("accountType", String, "Account type name", func() {
		Example("acc-detikcom")
	})
	Attribute("subAccountType", String, "Sub Account type name", func() {
		Enum("desktop", "mobile", "apps")
		Example("desktop")
	})
	Attribute("channelId", String, "Article's channel ID", func() {
		Example("123")
	})
	Attribute("createdDate", Number, "Article's created date (in unix seconds format)", func() {
		Example(1523342750)
	})
	Attribute("publishedDate", Number, "Article's published date (in unix seconds format)", func() {
		Example(1523342750)
	})
	Attribute("customParams", ArrayOf(ArticleCustomParamType))

	Required("accountType", "subAccountType")
})

var _ = Resource("collector", func() {
	Action("collectDtm", func() {
		Routing(GET("/__dtm.gif"))
		Response(OK, "image/gif")
		Response(Unauthorized)
		Response(Forbidden)
		Response(BadRequest, DetikErrorMedia)
		Response(InternalServerError, DetikErrorMedia)
		Metadata("swagger:summary", "Saves analytic data")
	})

	Action("collectPost", func() {
		Routing(POST("/__dtm.gif"))
		Response(NoContent)
		Response(Unauthorized)
		Response(Forbidden)
		Response(BadRequest, DetikErrorMedia)
		Response(InternalServerError, DetikErrorMedia)
		Metadata("swagger:summary", "Saves analytic data")
	})
})
