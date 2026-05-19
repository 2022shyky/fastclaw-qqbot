package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// ── JSON-RPC Types ──

type RPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id,omitempty"`
}

type RPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type RPCNotification struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type InboundMessage struct {
	Channel    string `json:"channel"`
	ChatID     string `json:"chatId"`
	UserID     string `json:"userId"`
	Text       string `json:"text"`
	PeerKind   string `json:"peerKind"`
	SenderName string `json:"senderName"`
	Account    string `json:"account,omitempty"`
}

// ── JSON-RPC I/O ──

var stdoutMu sync.Mutex

func rpcSend(obj interface{}) {
	stdoutMu.Lock()
	defer stdoutMu.Unlock()
	b, _ := json.Marshal(obj)
	os.Stdout.Write(append(b, '\n'))
}

func respond(id interface{}, result interface{}) {
	rpcSend(RPCResponse{JSONRPC: "2.0", Result: result, ID: id})
}

func respondError(id interface{}, code int, message string) {
	rpcSend(RPCResponse{JSONRPC: "2.0", Error: &RPCError{Code: code, Message: message}, ID: id})
}

func notify(method string, params interface{}) {
	rpcSend(RPCNotification{JSONRPC: "2.0", Method: method, Params: params})
}

func notifyInbound(msg InboundMessage) {
	fileLog("RPC-OUT", "message.inbound", msg)
	notify("message.inbound", msg)
}

// ── Request Handler ──

type InitParams struct {
	Config PluginConfig `json:"config"`
}

type SendParams struct {
	ChatID string `json:"chatId"`
	Text   string `json:"text"`
}

func handleRequest(req RPCRequest) {
	switch req.Method {
	case "initialize":
		var params InitParams
		json.Unmarshal(req.Params, &params)
		config = params.Config

		if config.AppID == "" || config.ClientSecret == "" {
			respondError(req.ID, -32000, "appId and clientSecret are required")
			return
		}
		if config.Sandbox == "true" {
			apiBase = SandboxAPIBase
			log("Using sandbox API")
		}

		logf("Initializing with appId=%s", config.AppID)
		// Respond immediately, connect in background
		respond(req.ID, map[string]string{"status": "ok"})
		go connectGateway()

	case "channel.send":
		var params SendParams
		json.Unmarshal(req.Params, &params)
		result, err := handleChannelSend(params)
		if err != nil {
			respondError(req.ID, -32000, err.Error())
			return
		}
		respond(req.ID, result)

	case "shutdown":
		log("Shutting down...")
		shutdownMu.Lock()
		isShuttingDown = true
		shutdownMu.Unlock()
		cleanup()
		respond(req.ID, map[string]string{"status": "ok"})
		time.AfterFunc(100*time.Millisecond, func() { os.Exit(0) })

	default:
		respondError(req.ID, -32601, fmt.Sprintf("Unknown method: %s", req.Method))
	}
}
