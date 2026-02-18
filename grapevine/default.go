package main

import (
    "fmt"
    "log"
)
/**
 * @author kkandeka
 */

type DefaultSampleApp struct {
    cfg *PeerServerCfg
} 

func NewDefaultSampleApp(cfg *PeerServerCfg) *DefaultSampleApp {
    return &DefaultSampleApp {
        cfg: cfg,
    }
}

func (a *DefaultSampleApp) Run() {
    // Define callback method
    onMsg := func(msg *Message) {
		  snippet := msg.Data
		  trimlen := 24
		  if len(snippet) > trimlen {
		      snippet = fmt.Sprintf("%s... (%d b total)", snippet[:trimlen], len(snippet))
		  }
		  fmt.Printf("%s: #%05d %s sends '%v'\n", a.cfg.name, msg.MsgId, msg.Source, snippet)
		}

	s, err := NewPeerServer(a.cfg, onMsg)
	if err != nil {
		log.Fatal(err)
	}

	// Start the server
	go func() {
		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}
