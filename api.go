package main

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"
)

// Handler
func base(c echo.Context) error {
	return c.String(http.StatusOK, "Hello, welcome to the video segementing API Docs")
}

type invalid struct {
	Invalid bool
}

type jsonError struct {
	Error string
}

// getJson extracts the json type froma push request
func getJson[T any](c echo.Context, target *T) error {
	var bodyBytes []byte
	if c.Request().Body != nil {
		bodyBytes, _ = io.ReadAll(c.Request().Body)

	} else {
		return c.JSON(http.StatusOK, invalid{true})
	}

	err := json.Unmarshal(bodyBytes, &target)
	// company ticker

	if err != nil {

		return c.JSON(http.StatusUnprocessableEntity, jsonError{Error: err.Error()})
	}

	return nil
}
