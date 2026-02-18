package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "strings"
)

/**
 * @author kkandeka
 */

type ChatSampleApp struct {
    cfg *PeerServerCfg
} 

func NewChatSampleApp(cfg *PeerServerCfg) *ChatSampleApp {
    return &ChatSampleApp {
        cfg: cfg,
    }
}

// Define callback
func (a *ChatSampleApp) onMsg(msg *Message) {
    fmt.Printf("\n%s: says '%v'\nsay:", msg.Source, msg.Data)
}

func (a *ChatSampleApp) Run() {
    // Create server
	s, err := NewPeerServer(a.cfg, a.onMsg)
	if err != nil {
		log.Fatal(err)
	}

	// Start the server
	go func() {
		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

    bio := bufio.NewReader(os.Stdin)
    for {
        fmt.Printf("\nsay: ")
        linebytes, _, _ := bio.ReadLine()
        line := string(linebytes)
        if strings.TrimSpace(line) == "" {
            continue
        }

        s.Multicast(line)
    }
}

