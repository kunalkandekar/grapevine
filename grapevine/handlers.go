package main

import (
	"bytes"
	"log"
	"net/http"
	"strconv"
    "encoding/json"
)

func (s *PeerServer) configureFailoverHandler(w http.ResponseWriter, req *http.Request) {
    req.Close = true
    values := req.URL.Query()
    dat, inmap := values["newfailover"]
    ret := false
    if inmap && (len(dat) > 0) {
        ret = s.SetFailover(dat[0])
    }
    if ret {
        w.WriteHeader(http.StatusOK)
    } else {
    	w.WriteHeader(http.StatusBadRequest)
    }
}

func (s *PeerServer) topologyHandler(w http.ResponseWriter, req *http.Request) {
    topoResp := s.Topology()
    req.Close = true
    var b bytes.Buffer
    json.NewEncoder(&b).Encode(topoResp)
    w.Write(b.Bytes())
}

func (s *PeerServer) dieHandler(w http.ResponseWriter, req *http.Request) {
    resp := []byte("TIME TO DIE\n")
    w.Write(resp)
    s.Die()
}

func (s *PeerServer) updateHandler(w http.ResponseWriter, req *http.Request) {
    req.Close = true
    values := req.URL.Query()
    dat, inmap := values["dat"]
    if inmap && (len(dat) > 0) {
        s.Multicast(dat[0])
    }
}

func (s *PeerServer) lastMsgHandler(w http.ResponseWriter, req *http.Request) {
    req.Close = true
    var b bytes.Buffer
    json.NewEncoder(&b).Encode(s.GetLastMsg())
    w.Write(b.Bytes())
}

func (s *PeerServer) msgStatsHandler(w http.ResponseWriter, req *http.Request) {
    req.Close = true
    var b bytes.Buffer
    json.NewEncoder(&b).Encode(s.GetMsgStats())
    w.Write(b.Bytes())
}

// this is for testing purposes, so we can ask a peer to initiate a multicast
func (s *PeerServer) commandHandler(w http.ResponseWriter, req *http.Request) {
    values := req.URL.Query()
    cmd, inmapc := values["cmd"]
    arg, inmapa := values["args"]
    if inmapc && (len(cmd) > 0) {
        if inmapa && (len(arg) > 0) {
            s.Command(cmd[0], arg[0])
        } else {
            s.Command(cmd[0], "")
        }
    }
}

func (s *PeerServer) debugHandler(w http.ResponseWriter, req *http.Request) {
    values := req.URL.Query()
    dat, inmap := values["flag"]
    if inmap && (len(dat) > 0) {
        if flag, err := strconv.Atoi(dat[0]); err == nil {
            s.SetDebugFlag(flag)
        } else {
            log.Printf("%s: Bad flag value %s: %v", s.name, dat[0], err)
        }
    } else {
        log.Printf("%s: No flag value provided", s.name)
    }
}
