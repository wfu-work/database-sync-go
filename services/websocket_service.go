package services

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/wfu-work/nav-common-go-lib/global"
	"go.uber.org/zap"
	"golang.org/x/net/websocket"
)

const (
	WebSocketEventBackupUpdated       = "backup.updated"
	WebSocketEventDataSourceUpdated   = "datasource.updated"
	WebSocketEventNotificationCreated = "notification.created"
)

type WebSocketMessage struct {
	Type string `json:"type"`
	Data any    `json:"data,omitempty"`
	Time int64  `json:"time"`
}

type WebSocketService struct {
	mu      sync.RWMutex
	sendMu  sync.Mutex
	clients map[*websocket.Conn]struct{}
}

func (s *WebSocketService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	server := websocket.Server{
		Handshake: func(*websocket.Config, *http.Request) error {
			return nil
		},
		Handler: websocket.Handler(s.handleConnection),
	}
	server.ServeHTTP(w, r)
}

func (s *WebSocketService) Broadcast(eventType string, data any) {
	if eventType == "" {
		return
	}
	body, err := json.Marshal(WebSocketMessage{
		Type: eventType,
		Data: data,
		Time: time.Now().UnixMilli(),
	})
	if err != nil {
		global.NAV_LOG.Warn("marshal websocket message failed", zap.String("type", eventType), zap.Error(err))
		return
	}

	s.mu.RLock()
	clients := make([]*websocket.Conn, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	for _, client := range clients {
		if err := websocket.Message.Send(client, string(body)); err != nil {
			s.removeClient(client)
		}
	}
}

func (s *WebSocketService) handleConnection(conn *websocket.Conn) {
	s.addClient(conn)
	defer s.removeClient(conn)

	for {
		var message string
		if err := websocket.Message.Receive(conn, &message); err != nil {
			return
		}
	}
}

func (s *WebSocketService) addClient(conn *websocket.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients == nil {
		s.clients = make(map[*websocket.Conn]struct{})
	}
	s.clients[conn] = struct{}{}
}

func (s *WebSocketService) removeClient(conn *websocket.Conn) {
	s.mu.Lock()
	if s.clients != nil {
		delete(s.clients, conn)
	}
	s.mu.Unlock()
	_ = conn.Close()
}
