package serialize

import (
	"encoding/json"
)

func MarshalJSON(data any) ([]byte, error) {
	return json.Marshal(data)
}

func UnMarshalJSON(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
}
