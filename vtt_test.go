package main

import (
	"os"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestSubstitles(t *testing.T) {
	groupSegments("./servercontents/1545682495177101653/segments/")
	vt := newvtt(os.Stdout)
	vt.AddSub("00:00.000", "00:05.111", key{ChapterTag: "test"})

	injector(echo.New().AcquireContext())

}
