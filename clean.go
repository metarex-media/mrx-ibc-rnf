package main

import (
	"encoding/json"
)

// clean converts 1 length arrays into a single object of that type
// it works better with json query that way
func clean(input map[string]any) ([]byte, error) {

	recurse(input)

	return json.MarshalIndent(input, "", "    ")
}

// recurse loops through maps and arrays to ensure each data item is checked
func recurse(input map[string]any) error {

	for k, v := range input {
		//	fmt.Println(k, reflect.TypeOf(v), dataKey)
		//	if k == dataKey {
		//		// don't clean the target data
		//		// just skip it
		//		continue
		//	}

		// check for types that need cleaning
		switch v := v.(type) {
		case []any:

			if len(v) > 1 {

				for _, v1 := range v {

					switch x := v1.(type) {
					case map[string]any:
						// handle the datakey if it is a map to remove the single arrays
						recurse(x)
					}
				}
			} else if len(v) == 1 {
				// if it is a further map recurse down into it
				switch x := v[0].(type) {
				case map[string]any:
					input[k] = x
					recurse(input[k].(map[string]any))
				}
			}
		case map[string]any:

			recurse(v)
		}

	}

	return nil
}
