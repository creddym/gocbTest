package appApi

import (
	"github.com/gorilla/mux"
	"net/http"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

func NewRouter() *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {

		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return router
}

var routes = Routes{
	{
		"InsertData",
		"POST",
		"/provision/{key}",
		InsertData,
	},
	{
		"UpsertData",
		"PUT",
		"/provision/{key}",
		UpsertData,
	},
	{
		"PatchData",
		"PATCH",
		"/provision/{key}",
		PatchData,
	},
	{
		"GetData",
		"GET",
		"/provision/{key}",
		GetData,
	},
	{
		"DeleteData",
		"DELETE",
		"/provision/{key}",
		DeleteData,
	},
}

func GetRoutes() Routes {
	return routes
}
