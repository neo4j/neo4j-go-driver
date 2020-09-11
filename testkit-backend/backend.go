package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// Handles a testkit backend session.
// Tracks all objects (and errors) that is created by testkit frontend.
type backend struct {
	rd             *bufio.Reader // Socket to read requests from
	wr             io.Writer     // Socket to write responses (and logs) on, don't buffer (WriteString on bufio was weird...)
	drivers        map[string]neo4j.Driver
	sessionStates  map[string]*sessionState
	results        map[string]neo4j.Result
	transactions   map[string]neo4j.Transaction
	recordedErrors map[string]error
	id             int // Id to use for next object created by frontend
	wrLock         sync.Mutex
}

// To implement transactional functions a bit of extra state is needed on the
// driver session.
type sessionState struct {
	session          neo4j.Session
	retryableState   int
	retryableErrorId string
}

const (
	retryable_nothing  = 0
	retryable_positive = 1
	retryable_negative = -1
)

func newBackend(rd *bufio.Reader, wr io.Writer) *backend {
	return &backend{
		rd:             rd,
		wr:             wr,
		drivers:        make(map[string]neo4j.Driver),
		sessionStates:  make(map[string]*sessionState),
		results:        make(map[string]neo4j.Result),
		transactions:   make(map[string]neo4j.Transaction),
		recordedErrors: make(map[string]error),
		id:             0,
	}
}

type frontendError struct {
	msg string
}

func (e *frontendError) Error() string {
	return e.msg
}

func (b *backend) writeLine(s string) error {
	bs := []byte(s + "\n")
	_, err := b.wr.Write(bs)
	return err
}

func (b *backend) writeLineLocked(s string) error {
	b.wrLock.Lock()
	defer b.wrLock.Unlock()
	return b.writeLine(s)
}

// Reads and writes to the socket until it is closed
func (b *backend) serve() {
	for b.process() {
	}
}

func (b *backend) setError(err error) string {
	id := b.nextId()
	b.recordedErrors[id] = err
	return id
}

func (b *backend) writeError(err error) {
	// Convert error if it is a known type of error.
	// This is very simple right now, no extra information is sent at all just keep
	// track of this error so that we can reuse the real thing within a retryable tx
	isDriverError := neo4j.IsNeo4jError(err) ||
		neo4j.IsUsageError(err) ||
		neo4j.IsConnectivityError(err)

	if isDriverError {
		id := b.setError(err)
		b.writeResponse("DriverError", map[string]interface{}{"id": id})
		return
	}

	// This is an error that originated in frontend
	frontendErr, isFrontendErr := err.(*frontendError)
	if isFrontendErr {
		b.writeResponse("FrontendError", map[string]interface{}{"msg": frontendErr.msg})
		return
	}

	// TODO: Return the other kinds of errors as well...

	// Unknown error, interpret this as a backend error
	// Report this to frontend and close the connection
	// This simplifies debugging errors from the frontend perspective, it will also make sure
	// that the frontend doesn't hang when backend suddenly disappears.
	b.writeResponse("BackendError", map[string]interface{}{"msg": err.Error()})
}

func (b *backend) nextId() string {
	b.id++
	return fmt.Sprintf("%d", b.id)
}

func (b *backend) process() bool {
	request := ""
	in_request := false

	for {
		line, err := b.rd.ReadString('\n')
		if err != nil {
			return false
		}

		switch line {
		case "#request begin\n":
			if in_request {
				panic("Already in request")
			}
			in_request = true
		case "#request end\n":
			if !in_request {
				panic("End while not in request")
			}
			b.handleRequest(b.toRequest(request))
			request = ""
			in_request = false
			return true
		default:
			if !in_request {
				panic("Line while not in request")
			}

			request = request + line
		}
	}
}

func (b *backend) writeResponse(name string, data interface{}) {
	response := map[string]interface{}{"name": name, "data": data}
	responseJson, err := json.Marshal(response)
	fmt.Printf("RES: %s\n", string(responseJson))
	if err != nil {
		panic(err.Error())
	}
	// Make sure that logging framework doesn't write anything inbetween here...
	b.wrLock.Lock()
	defer b.wrLock.Unlock()
	err = b.writeLine("#response begin")
	if err != nil {
		panic(err.Error())
	}
	err = b.writeLine(string(responseJson))
	if err != nil {
		panic(err.Error())
	}
	err = b.writeLine("#response end")
	if err != nil {
		panic(err.Error())
	}
}

func (b *backend) toRequest(s string) map[string]interface{} {
	fmt.Printf("REQ: %s\n", s)
	req := map[string]interface{}{}
	err := json.Unmarshal([]byte(s), &req)
	if err != nil {
		panic(err)
	}
	return req
}

func (b *backend) handleRequest(req map[string]interface{}) {
	name := req["name"].(string)
	data := req["data"].(map[string]interface{})
	switch name {
	case "NewDriver":
		authTokenMap := data["authorizationToken"].(map[string]interface{})["data"].(map[string]interface{})
		var authToken neo4j.AuthToken
		switch authTokenMap["scheme"] {
		case "basic":
			authToken = neo4j.BasicAuth(
				authTokenMap["principal"].(string),
				authTokenMap["credentials"].(string),
				authTokenMap["realm"].(string))
		default:
			b.writeError(errors.New("Unsupported scheme"))
			return
		}
		driver, err := neo4j.NewDriver(data["uri"].(string), authToken, func(c *neo4j.Config) {
			c.Log = &streamLog{writeLine: b.writeLineLocked}
		})
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.drivers[idKey] = driver
		b.writeResponse("Driver", map[string]interface{}{"id": idKey})
	case "DriverClose":
		driverId := data["driverId"].(string)
		driver := b.drivers[driverId]
		err := driver.Close()
		if err != nil {
			b.writeError(err)
			return
		}
		delete(b.drivers, driverId)
		b.writeResponse("Driver", map[string]interface{}{"id": driverId})
	case "NewSession":
		driver := b.drivers[data["driverId"].(string)]
		var accessMode neo4j.AccessMode
		switch data["accessMode"].(string) {
		case "r":
			accessMode = neo4j.AccessModeRead
		case "w":
			accessMode = neo4j.AccessModeWrite
		default:
			b.writeError(errors.New("Unknown accessmode: " + data["accessMode"].(string)))
			return
		}
		session, err := driver.Session(accessMode, "")
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.sessionStates[idKey] = &sessionState{session: session}
		b.writeResponse("Session", map[string]interface{}{"id": idKey})
	case "SessionClose":
		sessionId := data["sessionId"].(string)
		sessionState := b.sessionStates[sessionId]
		err := sessionState.session.Close()
		if err != nil {
			b.writeError(err)
			return
		}
		delete(b.sessionStates, sessionId)
		b.writeResponse("Session", map[string]interface{}{"id": sessionId})
	case "SessionRun":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		cypher := data["cypher"].(string)
		params, _ := data["params"].(map[string]interface{})
		for i, p := range params {
			params[i] = cypherToNative(p)
		}
		result, err := sessionState.session.Run(cypher, params)
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.results[idKey] = result
		b.writeResponse("Result", map[string]interface{}{"id": idKey})
	case "TransactionRun":
		tx := b.transactions[data["txId"].(string)]
		cypher := data["cypher"].(string)
		params, _ := data["params"].(map[string]interface{})
		for i, p := range params {
			params[i] = cypherToNative(p)
		}
		result, err := tx.Run(cypher, params)
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.results[idKey] = result
		b.writeResponse("Result", map[string]interface{}{"id": idKey})
	case "SessionReadTransaction":
		sid := data["sessionId"].(string)
		sessionState := b.sessionStates[sid]
		_, err := sessionState.session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			sessionState.retryableState = retryable_nothing
			// Instruct client to start doing it's work
			txid := b.nextId()
			b.transactions[txid] = tx
			b.writeResponse("RetryableTry", map[string]interface{}{"id": txid})
			defer delete(b.transactions, txid)
			// Process all things that the client might do within the transaction
			for {
				b.process()
				switch sessionState.retryableState {
				case retryable_positive:
					// Client succeeded and wants to commit
					return nil, nil
				case retryable_negative:
					// Client failed in some way
					if sessionState.retryableErrorId != "" {
						return nil, b.recordedErrors[sessionState.retryableErrorId]
					} else {
						return nil, &frontendError{msg: "Error from client"}
					}
				case retryable_nothing:
					// Client did something not related to the retryable state
				}
			}
		})

		if err != nil {
			b.writeError(err)
		} else {
			b.writeResponse("RetryableDone", map[string]interface{}{})
		}

	case "RetryablePositive":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		sessionState.retryableState = retryable_positive

	case "RetryableNegative":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		sessionState.retryableState = retryable_negative
		sessionState.retryableErrorId = data["errorId"].(string)

	case "ResultNext":
		result := b.results[data["resultId"].(string)]
		more := result.Next()
		if more {
			values := result.Record().Values
			cypherValues := make([]interface{}, len(values))
			for i, v := range values {
				cypherValues[i] = nativeToCypher(v)
			}
			b.writeResponse("Record", map[string]interface{}{"values": cypherValues})
		} else {
			err := result.Err()
			if err != nil {
				b.writeError(err)
				return
			}
			b.writeResponse("NullRecord", nil)
		}
	default:
		b.writeError(errors.New("Unknown request: " + name))
	}
}
