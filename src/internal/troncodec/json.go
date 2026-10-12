package troncodec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Reject duplicate keys, including escaped spellings, before any parser can
// select a different occurrence. Keep numbers exact; values are not decoded
// into float64 or rebuilt before the commitment check.
func validateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := readJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("invalid trailing TRON JSON")
	}
	return nil
}

func readJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 128 {
		return fmt.Errorf("TRON JSON nesting too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("invalid TRON JSON")
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			key, isString := token.(string)
			if err != nil || !isString || seen[key] {
				return fmt.Errorf("invalid or duplicate TRON JSON key")
			}
			seen[key] = true
			if err := readJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := readJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid TRON JSON delimiter")
	}
	end, err := decoder.Token()
	if err != nil || (delimiter == '{' && end != json.Delim('}')) || (delimiter == '[' && end != json.Delim(']')) {
		return fmt.Errorf("invalid TRON JSON closing delimiter")
	}
	return nil
}
