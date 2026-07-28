package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

func encode(gl GameLog) ([]byte, error) {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)

	err := enc.Encode(gl)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %v", err)
	}

	data := buff.Bytes()

	return data, nil
}

func decode(data []byte) (GameLog, error) {
	buff := bytes.NewReader(data)

	dec := gob.NewDecoder(buff)

	var gl GameLog

	err := dec.Decode(&gl)
	if err != nil {
		return gl, fmt.Errorf("error decoding: %v", err)
	}

	return gl, nil
}

// don't touch below this line

type GameLog struct {
	CurrentTime time.Time
	Message     string
	Username    string
}
