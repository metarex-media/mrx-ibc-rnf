package manifestdecode

import (
	"encoding/json"
	"os"
	"testing"
)

func TestXxx(t *testing.T) {

	manifestBytes, _ := os.ReadFile("../mount/bot-tlh/main/media/rnf/bbb/manifest.json")

	testMan := manifest{}
	json.Unmarshal(manifestBytes, &testMan)
	//	fmt.Println(testMan, len(manifestBytes))
	getSegments(testMan, map[string]bool{"bunny": true, "introduction": true})
}
