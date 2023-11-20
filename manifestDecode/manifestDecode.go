package manifestdecode

import (
	"fmt"
	"sort"
	"strings"
)

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


func getSegments(layout manifest, wanted map[string]bool) {
	segments := layout.Segments
	matches := []int{}
	for segNum, log := range segments {
		tags := strings.Split(log.Tags, ",")

		for _, t := range tags {
			if wanted[t] {
				matches = append(matches, segNum)
				break
			}
		}
	}

	// order the segments
	sort.Ints(matches)

	// show the expected segements and build their titles
	fmt.Printf("Expecting %v segments:\n", len(matches))
	for _, segment := range matches {
		fmt.Printf("    %s_%04d_%s.mp4\n", layout.Source, segment, segments[segment].Tags)
	}
}
