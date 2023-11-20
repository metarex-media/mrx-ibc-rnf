package main

// code copied from https://www.codeproject.com/Articles/5261771/Golang-SQLite-Simple-Example

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"encoding/csv"

	"github.com/antchfx/jsonquery"
	"github.com/labstack/echo/v4/middleware"

	"github.com/labstack/echo/v4"

	_ "github.com/mattn/go-sqlite3" // Import go-sqlite3 library
	"gopkg.in/yaml.v3"
)

var config map[string]inputs

type inputs struct {
	InputFrames string `json:"InputFrames" yaml:"InputFrames"`
	InputAudio  string `json:"InputAudio" yaml:"InputAudio"`
}

func init() {
	// read inputs to set up locations of files to be segmented
	configInputs, _ := os.ReadFile("config/config.json")
	json.Unmarshal(configInputs, &config)
	// fmt.Println(config)
}

func get(title string) inputs {
	return config[title]
}

// the instructions of how to decode and where to save
type command struct {
	//how to extract it
	DataInput    string `json:"dataInput" yaml:"dataInput"`
	Input        string
	MapCommands  paths  `json:"mapCommands" yaml:"mapCommands"` // this is the mapping bit
	Output       string `json:"outputDb" yaml:"outputDb"`
	OutputScript string `json:"outputCmd" yaml:"outputCmd"`
	/*
		tarball or segment outputs
		any mainfest parameters

	*/
	// the commands to call
	Clinputs   clipProperties `json:"clipProperties" yaml:"clipProperties"`
	DefaultTag string         `json:"defaultTag" yaml:"defaultTag"`
	Query      string
}

type clipProperties struct {
	Size         int    `json:"size" yaml:"size"`
	Framerate    int    `json:"framerate" yaml:"framerate"`
	Title        string `json:"title" yaml:"title"`
	InputFrames  string `json:"InputFrames" yaml:"InputFrames"`
	InputAudio   string `json:"InputAudio" yaml:"InputAudio"`
	OutputFolder string `json:"OutputFolder" yaml:"OutputFolder"`
}

func main() {

	//server set up
	e := echo.New()

	e.Use(middleware.Logger())  // Logger
	e.Use(middleware.Recover()) // Recover

	// API handling sections
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.HEAD, echo.PUT, echo.PATCH, echo.POST, echo.DELETE},
	}))

	// Routes
	e.GET("/", base)

	// latest iteration
	// data base abd video generation
	e.POST("/newdb", databaseGen)

	e.GET("/list", list)
	// VTT Handlers
	// add methods to update the vtts after the updates
	e.POST("/:database/segment", videoSegmenter)
	e.POST("/:database/update", injectSelect)
	e.POST("/:database/updateall", injector)
	// Start server
	e.Logger.Fatal(e.Start(":1323"))

}

// for use as a cli tool instead of a server
type inputFlags struct {
	command   *string
	overwrite *bool
}

// clean just runs the filepath.abs on all the locations
// to make errors more easy to understand and to save having to repeat them.
// the paths are based on where the file was called from
func (c *command) cleanCommand(commandLocation string) {
	folder := filepath.Dir(commandLocation)

	c.DataInput, _ = filepath.Abs(filepath.Join(folder, c.DataInput))
	c.Output, _ = filepath.Abs(filepath.Join(folder, c.Output))
	c.OutputScript, _ = filepath.Abs(filepath.Join(folder, c.OutputScript))
	c.Input, _ = filepath.Abs(filepath.Join(folder, c.Input))

	// clean the clipProperties
	c.Clinputs.OutputFolder, _ = filepath.Abs(filepath.Join(folder, c.Clinputs.OutputFolder))
	c.Clinputs.InputFrames, _ = filepath.Abs(filepath.Join(folder, c.Clinputs.InputFrames))

}

// paths lets the user gives the paths to the
// individual data points
type paths struct {
	Groups    string `yaml:"groups"` // marker groups
	GroupsTag string `yaml:"groupsTag"`

	TracksTag string `yaml:"tracksTag"` // marker tracks
	Tracks    string `yaml:"tracks"`

	Start       string `yaml:"start"`
	End         string `yaml:"end"` // the individual bits of data
	MetadataTag string `yaml:"metadataTag"`

	////
	DataType string `yaml:"dataType"`
}

type sqlFormat struct {
	groupTag    string
	start, end  int
	tracksTag   string
	metadataTag string
}

// change the foramtting slightly, where arrays of single items are changed to object,
// and its converted to the sql generic format.
func extractCleanMetadata(cleanBytes []byte, dataType string, format paths) ([]sqlFormat, error) {

	var err error

	baseMap := make(map[string]any)

	// find the type of the file
	switch strings.ToLower(dataType) {
	case "json":
		err = json.Unmarshal(cleanBytes, &baseMap)
	case "yaml":
		err = yaml.Unmarshal(cleanBytes, &baseMap)
	case "csv":
		var mock bytes.Buffer
		mock.Write(cleanBytes)
		return csvDecoder(&mock, format)
	default:

		return []sqlFormat{}, fmt.Errorf("invalid data type %v", dataType)

	}

	if err != nil {
		return []sqlFormat{}, err
	}

	// clean the single json arrays into json objects
	cleanBytes, err = clean(baseMap)
	if err != nil {
		return []sqlFormat{}, err
	}

	// parse the json object into jquery
	doc, err := jsonquery.Parse(strings.NewReader(string(cleanBytes)))
	if err != nil {
		return []sqlFormat{}, err
	}

	// break the json down into individual tracks
	tracks, err := getGroups(doc, format)
	if err != nil {
		return []sqlFormat{}, err
	}
	// then get the seperate metadata tracks
	return trackSeparator(tracks, format)

}

// csvDecoderFile is used with the command line method
func csvDecoderFile(csvFile string, format paths) ([]sqlFormat, error) {
	f, err := os.Open(csvFile)
	if err != nil {
		return []sqlFormat{}, err
	}
	return csvDecoder(f, format)
}

func csvDecoder(csvFile io.Reader, format paths) ([]sqlFormat, error) {

	fullCsv := csv.NewReader(csvFile)
	row, err := fullCsv.Read()
	if err != nil {
		return []sqlFormat{}, err
	}

	// first row is the header
	header := row
	results := make([]sqlFormat, 0)
	//get the actual row for reading
	row, err = fullCsv.Read()
	for err == nil {
		rowResult := sqlFormat{}

		for i, h := range header {
			// switch the header to see if it matches an input
			switch h {
			case format.GroupsTag:
				rowResult.groupTag = row[i]
			case format.TracksTag:
				rowResult.tracksTag = row[i]
			case format.MetadataTag:
				rowResult.metadataTag = row[i]
			case format.Start:
				fmt.Sscanf(row[i], "%v", &rowResult.start)
			case format.End:
				fmt.Sscanf(row[i], "%v", &rowResult.end)

			}
		}

		results = append(results, rowResult)
		row, err = fullCsv.Read()
	}

	if err != io.EOF {
		return []sqlFormat{}, err
	}

	return results, nil
}

type mdChannels struct {
	tag    string
	tracks []*jsonquery.Node
}

// getGroups breaks down the json object into the groups layer, if it is applicable,
// then the tracks, then the individual metadata values.
func getGroups(parent *jsonquery.Node, format paths) ([]mdChannels, error) {
	var Groups []*jsonquery.Node
	var err error

	// if there are groups to be separated by extract them
	if format.Groups != "" {
		// get these as a base
		Groups, err = jsonquery.QueryAll(parent, "//"+format.Groups)
		if len(Groups) == 0 && err == nil { //check something was found
			err = fmt.Errorf("no key was found with the name \"%s\"", format.Groups)
		}
	}
	if err != nil {
		return []mdChannels{}, err
	}

	// if there are tracks to be separated by do that.
	var tracks []mdChannels
	if format.Tracks != "" {

		if len(Groups) != 0 {
			// loop through the groups assigning tags and children
			for _, gro := range Groups {
				for _, g := range gro.ChildNodes() {
					var track []*jsonquery.Node
					track, err = jsonquery.QueryAll(g, "//"+format.Tracks)
					if len(track) == 0 {

						return []mdChannels{}, fmt.Errorf("no key was found with the name \"%s\"", format.Groups)
					} else if err != nil {
						return []mdChannels{}, err
					}

					var tag string
					if format.GroupsTag != "" {
						title, _ := jsonquery.Query(g, "//"+format.GroupsTag+"[not(ancestor::"+format.Tracks+")]")
						if title != nil {
							tag = title.Value().(string)
						} else {
							return []mdChannels{}, fmt.Errorf("no key was found with the name \"%s\"", format.GroupsTag)
						}
					}
					tracks = append(tracks, mdChannels{tracks: track, tag: tag})

				}
			}
		}
		/*
			extract each track per event if there is one
			assign any metadata for the track

		*/
	} else {
		tracks = []mdChannels{{tag: "", tracks: []*jsonquery.Node{parent}}}
	}

	return tracks, nil
}

// trackSeparator gets the metadata for each base metadata track
func trackSeparator(mdSources []mdChannels, format paths) ([]sqlFormat, error) {

	results := []sqlFormat{}

	for _, source := range mdSources {
		for _, tracks := range source.tracks {
			for _, track := range tracks.ChildNodes() {

				// get the title for the set of tracks
				trackJsonTag, _ := jsonquery.Query(track, "//"+format.TracksTag)
				// get the individual data points for each track
				starts, _ := jsonquery.QueryAll(track, "//"+format.Start)
				ends, _ := jsonquery.QueryAll(track, "//"+format.End)
				titles, _ := jsonquery.QueryAll(track, "//"+format.MetadataTag)

				if len(starts) != len(ends) && len(ends) != len(titles) {
					return []sqlFormat{}, fmt.Errorf("invalid lengths, found %v start points, %v end points and %v metadata tags",
						len(starts), len(ends), len(titles))
				}

				for i, s := range starts {
					trackTag := ""
					if trackJsonTag != nil {
						trackTag = trackJsonTag.Value().(string)
					}

					results = append(results, sqlFormat{start: int(s.Value().(float64)),
						end: int(ends[i].Value().(float64)), groupTag: source.tag,
						metadataTag: titles[i].Value().(string), tracksTag: trackTag,
					})
				}
			}
		}
	}

	return results, nil
}

type metadata struct {
	frameId int
	tags    key
	source  string
}

type key struct {
	ChapterTag, SegmentTag, TracksTag string
}

// Automatically build the tags when printing the key
// utilises the go String()  format.
func (k key) String() string {
	var place bool
	var sep = "-"
	var tags string
	if k.ChapterTag != "" {
		tags += k.ChapterTag
		place = true
	}

	if k.TracksTag != "" {
		if place {
			tags += sep
		}

		tags += k.TracksTag
		place = true
	}

	if k.SegmentTag != "" {
		if place {
			tags += sep
		}

		tags += k.SegmentTag
	}

	return tags
}

type manifest struct {
	Source    string // e.g. big buck bunny
	FrameRate int
	AllTags   []string `json:"All Metadata Tags"`
	Segments  map[int]segmentLog
}

type segmentLog struct {
	In, Out int
	Src     string // e.g. rnf/segment0004.tar
	Md5     string //
	Tags    string
}

func tarball(tw *tar.Writer, name string) error {
	// get the file
	f, err := os.Open(name)
	if err != nil {
		return err
	}

	//generate the tar header
	finf, _ := f.Stat()
	hdr, _ := tar.FileInfoHeader(finf, f.Name())

	if err := tw.WriteHeader(hdr); err != nil {
		return err

	}

	//save the file
	b := make([]byte, finf.Size())
	f.Read(b)
	if _, err := tw.Write(b); err != nil {
		return err

	}

	return nil
}

// file names that will be used by some different functions
const (
	outputVideoTag = "%s/%s_%04v_%s.mp4"
	// output2     = "%s/%s_01_01_%04v.mp4"
	outputVid   = "%s/segment%04v.mp4"
	outputAudio = "%s/segment%04v.wav"
	outputCSV   = "%s/segment%04v.csv"
	outputVTT   = "%s/%s_%04v_%s.vtt"
)

// framesToDur changes the time in frames to
// time in hours:miuntes:seconds:milliseconds
func framesToDur(frame, fps int) string {
	// mod number, divisor
	hourSize := fps * 60 * 60
	minuteSize := fps * 60

	hour := frame / hourSize
	if hour >= 1 {
		frame = frame - (hour * hourSize)
	}

	minute := frame / minuteSize
	if minute >= 1 {
		frame = frame - (minute * minuteSize)
	}

	second := frame / fps
	if second >= 1 {
		frame = frame - (second * fps)
	}

	subSecond := int((float64(frame) - 1) * float64(1000.0/float64(fps)))

	if frame == 0 {
		return fmt.Sprintf("%02d:%02d:%02d.000", hour, minute, second)
	} else {

		return fmt.Sprintf("%02d:%02d:%02d.%03d", hour, minute, second, subSecond)
	}

}
