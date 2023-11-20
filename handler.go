package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

/*
generate a  databse with the first call under the job ID
*/
func databaseGen(c echo.Context) error {

	// set up the input
	mappingPaths := paths{}
	mappingPaths.Groups = c.QueryParam("groups")
	mappingPaths.GroupsTag = c.QueryParam("groupsTag")

	mappingPaths.TracksTag = c.QueryParam("tracksTag") // marker tracks
	mappingPaths.Tracks = c.QueryParam("tracks")

	mappingPaths.Start = c.QueryParam("start")
	mappingPaths.End = c.QueryParam("end") // the individual bits of data
	mappingPaths.MetadataTag = c.QueryParam("metadataTag")

	mappingPaths.DataType = c.QueryParam("dataType")

	// fmt.Println(converter)

	var bodyBytes []byte
	if c.Request().Body != nil {
		bodyBytes, _ = io.ReadAll(c.Request().Body)

	} else {
		return c.String(http.StatusBadRequest, "no data")
	}

	random := rand.NewSource(time.Now().Unix())
	jobID := random.Int63()
	os.MkdirAll(fmt.Sprintf("./servercontents/%08d/", jobID), 0777)
	if err := extractAndSegment(mappingPaths, bodyBytes, jobID); err != nil {

		return c.String(http.StatusBadRequest, err.Error())
	}
	type reg struct{ Registered int64 }
	return c.JSON(http.StatusOK, reg{Registered: jobID})

}

func extractAndSegment(mappingPaths paths, entry []byte, jobID int64) error {
	if mappingPaths.DataType == "" {
		return fmt.Errorf("no dataType is chosen, please delcare the input type of json/csv/yaml")
	}

	// have a randomly outputted database designed to be discarded
	// dest :=
	// set up the database
	sqliteDatabase, err := generateSQL(fmt.Sprintf("./servercontents/%08d/tagging.db", jobID), false)
	if err != nil {
		return fmt.Errorf("error setting up sql database %v", err)
	}

	// close the database after processing
	defer sqliteDatabase.Close()

	fields, err := extractCleanMetadata(entry, mappingPaths.DataType, mappingPaths)
	if err != nil {
		return err
	}

	// insert the metadata into the sql instance
	err = sqlInserter(sqliteDatabase, fields, "api.call", "noTag")
	if err != nil {
		return fmt.Errorf("error inserting into database: %v", err)
	}

	return nil

}

type segmentInputs struct {

	// two distinct inputs - the tagging

	// these are allparts of the splitting process
	SegmentSize int    `json:"size" yaml:"size"`
	Framerate   int    `json:"framerate" yaml:"framerate"`
	Title       string `json:"title" yaml:"title"`
	InputFrames string `json:"InputFrames" yaml:"InputFrames"`

	// can these be split to be the bash script splitter e.g. bash framework = organised input
}

// list lists all databases on the server
func list(c echo.Context) error {
	folderlist, _ := os.ReadDir("./servercontents/")

	type folderDetails struct {
		ID           string
		CreationDate time.Time
	}

	type jobIDs struct {
		JobIds []folderDetails
	}

	j := jobIDs{JobIds: []folderDetails{}}
	for _, fol := range folderlist {
		if fol.IsDir() {
			db, _ := os.Open(fmt.Sprintf("./servercontents/%s/tagging.db", fol.Name()))
			inf, _ := db.Stat()
			j.JobIds = append(j.JobIds, folderDetails{ID: fol.Name(), CreationDate: inf.ModTime()})
			// implement a method to log the data about each database
		}
	}

	return c.JSONPretty(http.StatusOK, j, "    ")
}

type clipPropertiesAPI struct {
	Size          int    `json:"size" yaml:"size"`
	Framerate     int    `json:"framerate" yaml:"framerate"`
	Title         string `json:"title" yaml:"title"`
	OutputFolder  string `json:"OutputFolder" yaml:"OutputFolder"` //output folder is rnf/bbb or springwatch1 etc
	SegmentFolder string
	SegmentsToGen []int `json:"SegmentsToGen"`
}

// videoSegmenter gets the tags from the database and generates the video segments
func videoSegmenter(c echo.Context) error {
	jobID := c.Param("database")

	// @TODO include an override call so this behaviour becomes optional
	if _, err := os.Open(fmt.Sprintf("./servercontents/%s/segments/manifest.json", jobID)); err == nil {
		//return here to stop repeat behaviour

		// return the results processed find them here
		// add a force function
		return c.String(http.StatusOK, "video files already generated")
	}

	// do a check here to see if it has already run, so can return a 200

	var input clipPropertiesAPI
	err := getJson(c, &input)
	if err != nil {
		return err
	}

	// update to just be generated
	if strings.Contains(input.OutputFolder, "secure") {
		input.OutputFolder = filepath.Clean("./generated/" + input.OutputFolder)
	} else {
		input.OutputFolder = filepath.Clean("./generated/" + input.OutputFolder)
	}

	sqliteDatabase, err := sql.Open("sqlite3", fmt.Sprintf("./servercontents/%s/tagging.db", jobID)) // Open the created SQLite File
	if err != nil {
		return c.String(http.StatusConflict, fmt.Sprintf("error generating database %v", err))
	}
	defer sqliteDatabase.Close()

	results := fmt.Sprintf("./servercontents/%s/segments/", jobID)
	os.Mkdir(results, 0777)

	input.SegmentFolder = results

	// get the body first
	err = segmentMeta(sqliteDatabase, input, results)

	if err != nil {
		return c.String(http.StatusExpectationFailed, err.Error())
	}

	// cache the segmentation for use with the vtt stuff
	lastSaveBytes, _ := json.Marshal(input)
	f, _ := os.Create(results + "lastrun.json")
	f.Write(lastSaveBytes)
	f.Close()

	return c.String(http.StatusOK, "all good")
}

// segmentMetaData runs through the metadata in frame order and groups together the segments.
// it runs the ffmpeg script as part of this process.
func segmentMeta(db *sql.DB, clipInfo clipPropertiesAPI, baseFol string) error {
	rows, err := db.Query("SELECT * FROM metadata ORDER BY frameId") //("SELECT * FROM metadata WHERE key='Excellent' ORDER BY frameId")

	if err != nil {
		return err
	}
	defer rows.Close()

	// totalData := make(map[key][]metadata)
	manifest := manifest{Source: clipInfo.Title, FrameRate: clipInfo.Framerate, Segments: make(map[int]segmentLog), AllTags: []string{}}
	// get the segments in order
	//	count := 0
	var prev = &metadata{}
	var prevSeg int
	allocated := make(map[string]bool)
	//	startPos := 0

	segmentCounter := &segmentTrackerAPI{script: io.Discard, startPos: 1}

	for rows.Next() { // Iterate and fetch the records from result cursor

		var id int // can implement a struct that self scans and loop through but is that really needed
		var md metadata
		var seg int
		err := rows.Scan(&id, &md.frameId, &seg, &md.tags.ChapterTag, &md.tags.TracksTag, &md.tags.SegmentTag, &md.source)

		if segmentCounter.segmentCount == 0 && segmentCounter.frameCount == 0 {
			prev = &md
		}

		if err != nil {

			return err
		}

		//if the previous bit does not match the frame then write it
		if md.tags != prev.tags || seg != prevSeg {

			tags := strings.Split(prev.tags.String(), ",")

			for _, t := range tags {

				allocated[t] = true
			}

			// write the csv and video as part of the increment
			err = segmentCounter.increment(*prev, clipInfo, manifest)

			// update the new csv with the record
			//	segmentCounter.frameMetadata.Write([]string{fmt.Sprintf("%v", md.frameId), md.tags.groupTag, md.tags.tracksTag, md.tags.metadataTag, md.source})
		} else {

			segmentCounter.frameCount++
			// update the csv with the record
			//	segmentCounter.frameMetadata.Write([]string{fmt.Sprintf("%v", md.frameId), md.tags.groupTag, md.tags.tracksTag, md.tags.metadataTag, md.source})
			if segmentCounter.frameCount == clipInfo.Size {
				tags := strings.Split(md.tags.String(), ",")
				for _, t := range tags {
					allocated[t] = true
				}
				err = segmentCounter.increment(md, clipInfo, manifest)

			}
		}
		noPoint := md //point to the previous
		prev = &noPoint
		prevSeg = seg

		if err != nil {
			return err
		}

	}

	// if there are leftover frames
	// make sure to make them a segment
	if segmentCounter.frameCount != 0 {
		tags := strings.Split(prev.tags.String(), ",")

		for _, t := range tags {

			allocated[t] = true
		}
		segmentCounter.increment(*prev, clipInfo, manifest)
	}

	//add the tags
	for a := range allocated {
		manifest.AllTags = append(manifest.AllTags, a)
	}

	// save the manifest
	f, _ := os.Create(clipInfo.OutputFolder + "/manifest.json") //, clipInfo.OutputFolder))
	defer f.Close()
	b, _ := json.MarshalIndent(manifest, "", "    ")
	f.Write(b)

	fseg, _ := os.Create(clipInfo.SegmentFolder + "/manifest.json") //, clipInfo.OutputFolder))
	defer f.Close()
	fseg.Write(b)

	return nil

}

// API Handling for video segmentation
////////////////////////////////////////////////

type segmentTrackerAPI struct {
	frameCount   int
	segmentCount int
	startPos     int
	script       io.Writer
}


//increment generates the segments and webvtt, as well as keeping count of the segment and the segmentation process.
func (s *segmentTrackerAPI) increment(metaDataCap metadata, clipInfo clipPropertiesAPI, manifest manifest) error {

	if len(clipInfo.SegmentsToGen) == 0 || Contains(clipInfo.SegmentsToGen, s.segmentCount) {

		f, _ := os.Create(fmt.Sprintf(outputVTT, clipInfo.SegmentFolder, clipInfo.Title, s.segmentCount, metaDataCap.tags))

		v := newvtt(f)
		count := metaDataCap.frameId - s.startPos + 1

		v.AddSub("00:00:00.000", framesToDur(count+1, clipInfo.Framerate), metaDataCap.tags)
		f.Close()

		// reset the frame count
		s.frameCount = 0
		// s.frameMetadata.Flush()

		// generate the video then zip it all up
		scr, err := ffmpegSegmentScript(metaDataCap.tags, clipInfo, s.startPos, metaDataCap.frameId-s.startPos+1, s.segmentCount)
		if err != nil {
			return fmt.Errorf("error generating video segment %v", err)
		}

		s.script.Write([]byte(scr))
		md5 := zipVideoVtt(clipInfo, s.segmentCount, metaDataCap.tags)
		manifest.Segments[s.segmentCount] = segmentLog{In: s.startPos, Out: metaDataCap.frameId, Md5: md5, Tags: metaDataCap.tags.String()}

		//set up the counts for the next segment

		// generate a new csv
		// s.frameMetadata, _ = newCsv(fmt.Sprintf(outputCSV, clipInfo.OutputFolder, s.segmentCount))
	}
	s.startPos = metaDataCap.frameId + 1
	s.segmentCount++
	return nil
}


// generate and run the segmentation ffmpeg script
func ffmpegSegmentScript(key key, clipInfo clipPropertiesAPI, start, end, segment int) (string, error) {

	inputs := get(clipInfo.Title)

	starter := framesToDur(start, clipInfo.Framerate)
	durere := framesToDur(end+1, clipInfo.Framerate)
	if _, err := os.Open(fmt.Sprintf(outputAudio, clipInfo.SegmentFolder, segment)); err != nil {
		// fmt.Println(starter, durere, start, end, segment)
		ff2 := exec.Command("ffmpeg", "-y", "-i", inputs.InputAudio, "-ss", starter, "-t", durere, "-acodec", "pcm_s16le", "-ac", "1", "-ar", "16000",
			fmt.Sprintf(outputAudio, clipInfo.SegmentFolder, segment))

		err := ff2.Run()

		if err != nil {
			return "", fmt.Errorf("got error: %v with the commands %v", err, ff2.Args)
		}
	}
	// ffmpeg -i output_audio.aiff -ss 00:00:00 -t 00:00:00.080 -acodec pcm_s16le -ac 1 -ar 16000 sample.wav
	ffCommand := fmt.Sprintf("ffmpeg -start_number %v -i %s -frames:v %v -vcodec mpeg4 -r %v "+outputVid+"\n",
		start, inputs.InputFrames, end, clipInfo.Framerate, clipInfo.SegmentFolder, segment)

	/*cmd := exec.Command("ffmpeg", "-y", "-start_number", fmt.Sprintf("%v", start), "-i", clipInfo.InputFrames,
	"-i", fmt.Sprintf(outputAudio, clipInfo.OutputFolder, segment),
	"-frames:v",
	fmt.Sprintf("%v", end),
	"-vcodec", "mpeg4",
	"-r",
	fmt.Sprintf("%v", clipInfo.Framerate),
	fmt.Sprintf(outputVid, clipInfo.OutputFolder, segment))*/
	if _, err := os.Open(fmt.Sprintf(outputVideoTag, clipInfo.SegmentFolder, clipInfo.Title, segment, key)); err != nil {

		cmd := exec.Command("ffmpeg", "-y", "-start_number", fmt.Sprintf("%v", start),
			"-framerate", fmt.Sprintf("%v", clipInfo.Framerate),
			"-i", inputs.InputFrames,
			"-i", fmt.Sprintf(outputAudio, clipInfo.SegmentFolder, segment),
			"-frames:v",
			fmt.Sprintf("%v", end),
			"-vcodec", "mpeg4",
			"-r", fmt.Sprintf("%v", clipInfo.Framerate),
			"-q:v", "0", // keep the quality for the inputs
			fmt.Sprintf(outputVideoTag, clipInfo.SegmentFolder, clipInfo.Title, segment, key)) //fmt.Sprintf(outputVid, clipInfo.OutputFolder, segment))
		//fmt.Println(cmd.Args)
		err = cmd.Run()
		if err != nil {
			return ffCommand, fmt.Errorf("got error: %v with the commands %v", err, cmd.Args)
		}
	}
	return ffCommand, nil
}

func zipVideoVtt(clipInfo clipPropertiesAPI, segment int, key key) string {

	// @TODO make the output customisable
	// or rely on feedback for the moment
	f, _ := os.Create(fmt.Sprintf(clipInfo.OutputFolder+"/segment%04d.tar.gz", segment))
	// ../mount/bot-tlh/main/media/rnf/bbb_title
	//f, _ := os.Create(fmt.Sprintf("mount/bot-tlh/main/media/rnf/bbb/segment%04d.tar.gz", segment))
	defer f.Close()
	gzw := gzip.NewWriter(f) //wrap the file with a zip function
	tw := tar.NewWriter(gzw)

	/* skip the frames for now
	for i := start; i <= end; i++ {
		name := fmt.Sprintf(clipInfo.InputFrames, i)
		tarball(tw, name) //zip each files

	}*/

	// add the generated video
	tarball(tw, fmt.Sprintf(outputVideoTag, clipInfo.SegmentFolder, clipInfo.Title, segment, key)) //fmt.Sprintf(outputVid, clipInfo.OutputFolder, segment))
	// add the frame by frame metadata information
	tarball(tw, fmt.Sprintf(outputVTT, clipInfo.SegmentFolder, clipInfo.Title, segment, key))

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

// These are direct copies of go 1.21 slices
// for use when with earlier version of go, up until 1.18

// Contains reports whether v is present in s.
func Contains[S ~[]E, E comparable](s S, v E) bool {
	return Index(s, v) >= 0
}

// Index returns the index of the first occurrence of v in s,
// or -1 if not present.
func Index[S ~[]E, E comparable](s S, v E) int {
	for i := range s {
		if v == s[i] {
			return i
		}
	}
	return -1
}
