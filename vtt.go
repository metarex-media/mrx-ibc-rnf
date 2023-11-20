package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/peterbourgon/mergemap"
)

type webVtt struct {
	w io.Writer
}

//newVtt generates a new webvtt Writer
func newvtt(w io.Writer) webVtt {

	w.Write([]byte("WEBVTT\n\n"))

	return webVtt{w: w}
}

// have the sub in a specific format
func (w webVtt) AddSub(startTime, endTime string, sub key) {
	w.w.Write([]byte(fmt.Sprintf("%s --> %s\n", startTime, endTime)))

	results := make(map[string]bool)
	// add each to the map to be converted to json format
	tagToMap(sub.ChapterTag, results)
	tagToMap(sub.SegmentTag, results)
	tagToMap(sub.TracksTag, results)

	bod, _ := json.Marshal(results)

	w.w.Write(bod)
	w.w.Write([]byte("\n\n"))
}

// tagToMap adds all tags to the base map of tags
func tagToMap(tag string, base map[string]bool) {
	if tag == "" {
		return
	}

	tags := strings.Split(tag, ",")
	for _, t := range tags {
		base[t] = true
	}
}

func vttUpdate(update map[string]any, vttFile string) {

	file, _ := os.Open(vttFile)

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	c := 0
	// scanner
	var newvtt bytes.Buffer
	for scanner.Scan() {

		// the subtitle track is one line at line 3
		// so when line 3 is got do all the json magic
		if c == 3 {
			var m map[string]any
			json.Unmarshal(scanner.Bytes(), &m)
			// update the json with the updates
			m = mergemap.Merge(m, update)

			mvb, _ := json.Marshal(m)
			newvtt.Write(mvb)
			newvtt.Write([]byte("\n"))
		} else if c < 3 {
			newvtt.Write(scanner.Bytes())
			newvtt.Write([]byte("\n"))
		}
		c++
		//	fmt.Println(scanner.Text())
	}

	// just remake the file here
	newVttFile, _ := os.Create(vttFile)
	newVttFile.Write(newvtt.Bytes())
}

// inject all and inject select
// update only specific segments,
// in a database with more 
// metadata
func injectSelect(c echo.Context) error {
	jobID := c.Param("database")
	var individual map[int]map[string]any
	parent := "./servercontents/" + jobID + "/segments/"
	files, _ := os.ReadDir(parent)

	vttex, _ := regexp.Compile(`(\.vtt)$`)

	dataType := c.QueryParam("datatype")
	if dataType == "csv" {
		bodyBytes, _ := io.ReadAll(c.Request().Body)
		individual = getCsv(bodyBytes)
	} else {
		err := getJson(c, &individual)
		if err != nil {
			return err
		}
	}

	processedSegs := []int{}
	// @TODO log file names for updating the manifest
	for _, f := range files {

		if vttex.MatchString(f.Name()) {
			sects := strings.Split(f.Name(), "_")
			var num int
			fmt.Sscanf(sects[1], "%04d", &num)

			// only update if a vtt file is required
			if update, ok := individual[num]; ok {
				processedSegs = append(processedSegs, num)
				//	then update the segment
				vttUpdate(update, parent+f.Name())
			}
		}
	}

	// rezip the video and subtitle file
	rezip(parent, processedSegs)

	return c.JSONPretty(http.StatusAccepted, "Updates applied", "    ")
}




// getcsv extracts the csv data has segment number and
// metadata
func getCsv(data []byte) map[int]map[string]any {

	var mockCsv bytes.Buffer
	mockCsv.Write(data)

	mdcsv := csv.NewReader(&mockCsv)

	rows, err := mdcsv.Read()
	header := rows
	segmentData := make(map[int]map[string]any)

	row := 0

	for err == nil {
		segmentData[row] = make(map[string]any)

		rows, err = mdcsv.Read()

		for i, data := range rows {
			segmentData[row][header[i]] = data
		}

		row++
	}

	return segmentData
}

// inhector updates all segments within a database
func injector(c echo.Context) error {
	jobID := c.Param("database")
	parent := "./servercontents/" + jobID + "/segments/"
	files, _ := os.ReadDir(parent)

	var update map[string]any
	err := getJson(c, &update)
	if err != nil {
		return err
	}

	vttex, _ := regexp.Compile(`(\.vtt)$`)
	processedSegs := []int{}
	for i, f := range files {
		processedSegs = append(processedSegs, i)
		if vttex.MatchString(f.Name()) {
			// if its a vtt file update regardless
			vttUpdate(update, parent+f.Name())

		}
	}

	rezip(parent, processedSegs)

	return nil
}

var format, _ = regexp.Compile(`[\w\-]{1,}\_[\w\-,]{1,}`)

// rezip rezips all the video and vtt files with the same naming format
func rezip(folderLocation string, segments []int) {

	entries, _ := os.ReadDir(folderLocation)
	var lastRun clipPropertiesAPI
	lastRunb, _ := os.ReadFile(folderLocation + "/lastrun.json")
	json.Unmarshal(lastRunb, &lastRun)

	processed := make(map[int]string)
	for _, ent := range entries {
		if format.MatchString(ent.Name()) {
			// then process by getting the segment value
			sects := strings.Split(ent.Name(), "_")
			var num int
			fmt.Sscanf(sects[1], "%04d", &num)

			// stop duplicate for the vtt and mp4
			if _, ok := processed[num]; !ok {
				// rezip the file and upload it to the output

				md5 := zipVideoVtt(lastRun, num, key{TracksTag: sects[2][0 : len(sects[2])-4]})
				processed[num] = md5
			}

		} // else ignore
	}

	manb, _ := os.ReadFile(lastRun.OutputFolder + "/manifest.json")
	var man manifest
	json.Unmarshal(manb, &man)

	for seg, md := range processed {
		mid := man.Segments[seg]
		mid.Md5 = md
		man.Segments[seg] = mid
	}

	f, _ := os.Create(lastRun.OutputFolder + "/manifest.json")
	newman, _ := json.Marshal(man)
	f.Write(newman)
}

// group segments groups the segments together based on their number.
func groupSegments(folderLocation string) {

	entries, _ := os.ReadDir(folderLocation)
	segments := make(map[int][]string)

	for _, ent := range entries {
		if format.MatchString(ent.Name()) {
			// then process by getting the segment value
			sects := strings.Split(ent.Name(), "_")
			var num int
			fmt.Sscanf(sects[1], "%04d", &num)
			segments[num] = append(segments[num], folderLocation+"/"+ent.Name())

		} // else ignore
	}

	for segNum, seg := range segments {
		fmt.Println(segNum, seg, len(seg))
		zipFiles(seg, segNum)
	}
}


// zip files zips multiple files as tar.gz
func zipFiles(names []string, segNum int) string {

	// @TODO make the output customisable
	// or rely on feedback for the moment
	f, _ := os.Create(fmt.Sprintf("generated/lostpasttest"+"/segment%04d.tar.gz", segNum))

	defer f.Close()
	gzw := gzip.NewWriter(f) //wrap the file with a zip function
	tw := tar.NewWriter(gzw) // @TODO possobly swap the zip tar order

	// match all segments
	for _, name := range names {
		tarball(tw, name)
	}

	// close the tar then the zip after writing
	tw.Close()
	gzw.Close()

	// read the completed file bytes
	// to generate the hash
	f.Seek(0, 0)
	finf, _ := f.Stat()
	body := make([]byte, finf.Size())
	f.Read(body)
	return fmt.Sprintf("%032x", md5.Sum(body))
}
