package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ── Gateway Types ──

type GatewayPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type HelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type EventAuthor struct {
	ID           string `json:"id"`
	UserOpenID   string `json:"user_openid"`
	MemberOpenID string `json:"member_openid"`
	Username     string `json:"username"`
	Bot          bool   `json:"bot"`
}

type EventMember struct {
	Nick string `json:"nick"`
}

type EventData struct {
	ID          string       `json:"id"`
	Content     string       `json:"content"`
	Author      EventAuthor  `json:"author"`
	Member      EventMember  `json:"member"`
	ChannelID   string       `json:"channel_id"`
	GuildID     string       `json:"guild_id"`
	GroupOpenID string       `json:"group_openid"`
	GroupID     string       `json:"group_id"`
	SessionID   string       `json:"session_id"`
	Shard       []int        `json:"shard"`
	User        *EventAuthor `json:"user,omitempty"`
	Attachments []Attachment `json:"attachments"`
}

// ── WebSocket Connection ──

var conn *websocket.Conn

func cleanup() {
	if heartbeatStop != nil {
		close(heartbeatStop)
		heartbeatStop = nil
	}
	connMu.Lock()
	if conn != nil {
		conn.Close()
		conn = nil
	}
	connMu.Unlock()
}

func wsSend(data interface{}) {
	connMu.Lock()
	defer connMu.Unlock()
	if conn == nil {
		return
	}
	b, _ := json.Marshal(data)
	fileLog("WS-SEND", "payload", string(b))
	conn.WriteMessage(websocket.TextMessage, b)
}

func wsSendQuiet(data interface{}) {
	connMu.Lock()
	defer connMu.Unlock()
	if conn == nil {
		return
	}
	b, _ := json.Marshal(data)
	conn.WriteMessage(websocket.TextMessage, b)
}

func connectGateway() {
	if isShutdown() {
		return
	}
	cleanup()

	if _, err := getAccessToken(); err != nil {
		logf("Failed to get token: %v", err)
		scheduleReconnect()
		return
	}

	gatewayURL, err := getGatewayURL()
	if err != nil {
		logf("Failed to get gateway URL: %v", err)
		scheduleReconnect()
		return
	}

	logf("Connecting to gateway: %s", gatewayURL)
	c, _, err := websocket.DefaultDialer.Dial(gatewayURL, nil)
	if err != nil {
		logf("WebSocket dial failed: %v", err)
		scheduleReconnect()
		return
	}

	connMu.Lock()
	conn = c
	connMu.Unlock()

	log("WebSocket connected")
	fileLog("WS-EVENT", "OPEN", "connection established")

	go func() {
		defer func() {
			cleanup()
			if !isShutdown() {
				scheduleReconnect()
			}
		}()
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				logf("WebSocket read error: %v", err)
				fileLog("WS-EVENT", "CLOSE", err.Error())
				return
			}
			fileLog("WS-RECV", "raw", string(message))
			var payload GatewayPayload
			if err := json.Unmarshal(message, &payload); err != nil {
				fileLog("WS-RECV", "PARSE-ERROR", string(message))
				continue
			}
			handleGatewayPayload(payload)
		}
	}()
}

func scheduleReconnect() {
	if isShutdown() || reconnectAttempts >= maxReconnectAttempts {
		log("Max reconnect attempts reached or shutting down")
		return
	}
	idx := reconnectAttempts
	if idx >= len(reconnectDelays) {
		idx = len(reconnectDelays) - 1
	}
	delay := reconnectDelays[idx]
	reconnectAttempts++
	logf("Reconnecting in %v (attempt %d)", delay, reconnectAttempts)
	time.AfterFunc(delay, connectGateway)
}

// ── Payload Handling ──

func handleGatewayPayload(p GatewayPayload) {
	if p.Op != 11 {
		logf("[ws-recv] op=%d t=%s s=%v", p.Op, p.T, p.S)
	}
	if p.S != nil {
		lastSeq = p.S
	}

	switch p.Op {
	case 10:
		var hello HelloData
		json.Unmarshal(p.D, &hello)
		handleHello(hello)
	case 11: // Heartbeat ACK
	case 0:
		handleDispatch(p.T, p.D)
	case 7:
		log("Server requested reconnect")
		cleanup()
		go connectGateway()
	case 9:
		log("Invalid session, resetting")
		sessionID = ""
		lastSeq = nil
		cleanup()
		scheduleReconnect()
	default:
		logf("Unknown op: %d", p.Op)
	}
}

func handleHello(hello HelloData) {
	interval := hello.HeartbeatInterval
	if interval == 0 {
		interval = 41250
	}
	logf("Hello received, heartbeat interval: %dms", interval)

	heartbeatStop = make(chan struct{})
	stop := heartbeatStop
	go func() {
		ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				wsSendQuiet(map[string]interface{}{"op": 1, "d": lastSeq})
			}
		}
	}()

	tokenMu.Lock()
	token := ""
	if tokenCache != nil {
		token = tokenCache.Token
	}
	tokenMu.Unlock()

	if sessionID != "" && lastSeq != nil {
		logf("Resuming session: %s, seq: %d", sessionID, *lastSeq)
		wsSend(map[string]interface{}{
			"op": 6,
			"d": map[string]interface{}{
				"token": "QQBot " + token, "session_id": sessionID, "seq": *lastSeq,
			},
		})
	} else {
		log("Identifying...")
		wsSend(map[string]interface{}{
			"op": 2,
			"d": map[string]interface{}{
				"token": "QQBot " + token, "intents": FullIntents, "shard": []int{0, 1},
				"properties": map[string]string{"$os": "linux", "$browser": "fastclaw", "$device": "fastclaw"},
			},
		})
	}
}

// ── Dispatch ──

func handleDispatch(eventType string, raw json.RawMessage) {
	var data EventData
	json.Unmarshal(raw, &data)
	logf("[dispatch] eventType=%s", eventType)

	switch eventType {
	case "READY":
		sessionID = data.SessionID
		reconnectAttempts = 0
		username := "unknown"
		if data.User != nil {
			username = data.User.Username
		}
		logf("Ready! session=%s, user=%s", sessionID, username)

	case "RESUMED":
		reconnectAttempts = 0
		log("Session resumed")

	case "C2C_MESSAGE_CREATE":
		userID := data.Author.UserOpenID
		if userID == "" {
			userID = data.Author.ID
		}
		rawContent := strings.TrimSpace(data.Content)
		attachmentText := formatAttachments(data.Attachments)
		content := rawContent + attachmentText
		if (rawContent == "" && attachmentText == "") || userID == "" {
			break
		}
		chatKey := "c2c:" + userID
		setLastMsgID(chatKey, data.ID)
		logf("C2C ← %s: %s", userID, truncate(content, 200))
		notifyInbound(InboundMessage{
			Channel: "qqbot", ChatID: chatKey, UserID: userID,
			Text: content, PeerKind: "dm", SenderName: truncate(userID, 8),
			Account: config.AppID,
		})
		// 直接通过 Chat API 回复（绕过 Gateway owner 解析）
		go replyViaChatAPI(userID, content)

	case "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE":
		groupID := data.GroupOpenID
		if groupID == "" {
			groupID = data.GroupID
		}
		senderID := data.Author.MemberOpenID
		if senderID == "" {
			senderID = data.Author.ID
		}
		rawContent := strings.TrimSpace(data.Content)
		attachmentText := formatAttachments(data.Attachments)
		if (rawContent == "" && attachmentText == "") || groupID == "" {
			break
		}
		content := stripMentions(rawContent) + attachmentText
		chatKey := "group:" + groupID
		setLastMsgID(chatKey, data.ID)
		senderName := data.Author.Username
		if senderName == "" {
			senderName = truncate(senderID, 8)
		}
		logf("Group ← %s/%s: %s", groupID, senderID, truncate(content, 200))
		notifyInbound(InboundMessage{
			Channel: "qqbot", ChatID: chatKey, UserID: senderID,
			Text: content, PeerKind: "group", SenderName: senderName,
			Account: config.AppID,
		})

	case "AT_MESSAGE_CREATE", "MESSAGE_CREATE":
		channelID := data.ChannelID
		authorID := data.Author.ID
		rawContent := strings.TrimSpace(data.Content)
		if rawContent == "" || channelID == "" || data.Author.Bot {
			break
		}
		content := stripMentions(rawContent)
		chatKey := "channel:" + channelID
		setLastMsgID(chatKey, data.ID)
		senderName := data.Author.Username
		if senderName == "" {
			senderName = data.Member.Nick
		}
		if senderName == "" {
			senderName = truncate(authorID, 8)
		}
		logf("Guild ← %s/%s: %s", channelID, authorID, truncate(content, 200))
		notifyInbound(InboundMessage{
			Channel: "qqbot", ChatID: chatKey, UserID: authorID,
			Text: content, PeerKind: "group", SenderName: senderName,
			Account: config.AppID,
		})

	case "DIRECT_MESSAGE_CREATE":
		guildID := data.GuildID
		authorID := data.Author.ID
		content := strings.TrimSpace(data.Content)
		if content == "" || guildID == "" || data.Author.Bot {
			break
		}
		chatKey := "dm:" + guildID
		setLastMsgID(chatKey, data.ID)
		senderName := data.Author.Username
		if senderName == "" {
			senderName = truncate(authorID, 8)
		}
		logf("DM ← %s/%s: %s", guildID, authorID, truncate(content, 200))
		notifyInbound(InboundMessage{
			Channel: "qqbot", ChatID: chatKey, UserID: authorID,
			Text: content, PeerKind: "dm", SenderName: senderName,
			Account: config.AppID,
		})

	default:
		logf("[dispatch] UNHANDLED eventType=%s", eventType)
	}
}


// ── Chat API 直接回复（绕过 Gateway owner 解析） ──

func replyViaChatAPI(userOpenID, text string) {
	apiKey := os.Getenv("FASTCLAW_API_KEY")
	if apiKey == "" {
		logf("[chat] FASTCLAW_API_KEY 未设置，跳过 Chat API")
		return
	}
	agentID := os.Getenv("FASTCLAW_AGENT_ID")
	if agentID == "" {
		agentID = "agt_c255b73a23f7d9643ae6"
	}

	payload := map[string]interface{}{
		"agentId":   agentID,
		"sessionId": "qq_" + truncate(userOpenID, 12),
		"message":   text,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", "http://localhost:18953/api/chat/stream",
		bytes.NewReader(body))
	if err != nil {
		logf("[chat] 创建请求失败: %v", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logf("[chat] Chat API 请求失败: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		logf("[chat] Chat API HTTP %d", resp.StatusCode)
		return
	}

	// 读取流式回复
	var reply strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			reply.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
	}

	// 简单解析 SSE 格式，提取 content
	replyText := strings.Builder{}
	for _, line := range strings.Split(reply.String(), "\n") {
		if strings.HasPrefix(line, "data: ") {
			var data struct {
				Type string `json:"type"`
				Data struct {
					Content string `json:"content"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(line[6:]), &data); err == nil {
				if data.Type == "content" {
					replyText.WriteString(data.Data.Content)
				}
			}
		}
	}

	replyStr := strings.TrimSpace(replyText.String())
	if replyStr == "" {
		logf("[chat] AI 回复为空")
		return
	}

	logf("[chat] AI 回复成功: %s", truncate(replyStr, 100))
	sendC2CMessage(userOpenID, replyStr, "")
}

