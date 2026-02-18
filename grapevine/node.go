package main

import (
	"fmt"
	"bytes"
	"io"
	"log"
	"os"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
    "encoding/json"
)

/**
 * @author kkandeka
 */

type Message struct {
    MsgId       int         `json:"msgId"`
    MsgType     string      `json:"type"`
    TTL         int         `json:"ttl"`
    Source      string      `json:"source"`
    Forwarder   string      `json:"forwarder"`
    Data        string      `json:"data"`
}

type StatusRequest struct {
    Name        string      `json:"name"`
    Address     string      `json:"address"`
    ClusterId   string      `json:"clusterId"`
}

type ReflectStatusRequest struct {
    Name        string      `json:"name"`
    Address     string      `json:"address"`
    ClusterId   string      `json:"clusterId"`
    Target      *Peer       `json:"target"`
}

type StatusResponse struct {
    Alive       bool        `json:"alive"`
    Children    []*Peer     `json:"children"`
    LastMsgId   int         `json:"lastMsgId"`
    RTT         int         `json:"rtt"`
}

type TopologyResponse struct {
    Parent      *Peer       `json:"parent"`
    NumChildren int         `json:"num"`
    Children    []*Peer     `json:"children"`
}

type MessageStats struct {
    LastMsgId   int         `json:"lastMsgId"`
    NumMsgs     int         `json:"numMsgs"`
    CRCTally    uint32      `json:"crcTally"`
}

const (
    MSG_TYPE_CMD = "cmd"
    MSG_TYPE_DAT = "dat"
)

const (
    CMD_NEW_ROOT        = "NEWROOT"
    CMD_NEW_FAILOVER    = "NEWFAILOVER"
    CMD_PROMPT_LEAVES   = "PROMPTLEAVES"
    CMD_DIE             = "DIE"
)

const (
    DEBUG_FLAG_NONE     = 0
    DEBUG_FLAG_FWD      = 1
    DEBUG_FLAG_STATUS   = 2
    DEBUG_FLAG_JOIN     = 4
    DEBUG_FLAG_TALLYCRC = 8
)

// overlay settings
const (
    MAX_JOIN_ATTEMPTS  = 20 
    MAX_BCAST_ATTEMPTS = 10
    MAX_LAST_JOINED    = 10
    DEFAULT_TTL        = 16 
    DELAY_PER_CHILD_MS    = 5  
	HEARTBEAT_INTERVAL_MS = 1000
)

type callback func(update *Message)

type PeerServer struct {
    name        string
    clusterId   string
    root        string
    failover    string 
    address     string
    router      *http.ServeMux
    httpServer  *http.Server
    debugFlag   int
    forceClose  bool

    capacity    int
    parent      *Peer
    oldParent   *Peer
    self        *Peer
    treelock    sync.RWMutex
    children    map[string]*Peer    // Peers who are direct child nodes
    peersName   map[string]*Peer    // Peers we are aware of, keyed by name
    peersAddr   map[string]*Peer    // Peers we are aware of, keyed by address
    mayBeLeaves []*Peer             // Peers most recently joined, probably leaves hence failover candidates
    depth       int

    lastMsgId   int
    msglock     sync.Mutex
    onMessage   callback
    lastMsg     *Message
    msgStats    *MessageStats
    trackMsgCrc bool

    doLoopback  bool
    heartbeat   <- chan time.Time
    isJoining   bool
    findingFailover bool
    isFailover  bool
    joinlock    sync.Mutex
    joinChan    chan []*Peer
    pruneChan   chan *Peer
    confirmChan chan string
    failoverChan chan string
    srvStopChan chan string
    mgrStopChan chan string
    leafMonChan chan *Peer

    maybeFailover   bool
    failoverStrat   int
}

type PeerServerCfg struct {
    name        string
    host        string
    addr        string
    port        int
    cluster     string
    root        string
    failover    string 
    maxChildren int
    failoverStrat int
}

// Creates a new PeerServer.
func NewPeerServer(cfg *PeerServerCfg, onMessage callback) (*PeerServer, error) {
    //resolve hostname to IP
    if cfg.host == "localhost" && cfg.addr == "" {
        addrs, err := net.LookupHost(cfg.host)
        if err != nil {
            log.Printf("Unable to look up host %s, error: %v", cfg.host, err)
            cfg.addr = "127.0.0.1"
        } else if len(addrs) > 0 {
            // choose the first IP by default
            cfg.addr = addrs[0]
        } else {
            // choose the first IP by default
            cfg.addr = "127.0.0.1"
        }
    }
    address := fmt.Sprintf("%s:%d", cfg.host, cfg.port)
    s := &PeerServer{
        name: cfg.name,
		address:  address,
        clusterId: cfg.cluster,
		failover: cfg.failover,
		self: nil,
		parent: nil,
		depth: -1,
		msgStats: &MessageStats{},
		router: http.NewServeMux(),
		capacity: cfg.maxChildren,
        children: make(map[string]*Peer),
        peersName: make(map[string]*Peer),
        peersAddr: make(map[string]*Peer),
        mayBeLeaves: make([]*Peer, 0),
        onMessage: onMessage,
        doLoopback: false,
        heartbeat: time.NewTicker(time.Duration(HEARTBEAT_INTERVAL_MS) * time.Millisecond).C,
        joinChan: make(chan []*Peer),
        pruneChan: make(chan *Peer),
        confirmChan: make(chan string),
        failoverChan: make(chan string), 
        srvStopChan: make(chan string),
        mgrStopChan: make(chan string),
        leafMonChan: make(chan *Peer),
        failoverStrat: FAILOVER_STRAT_NONE,
    }
    s.self = s.getPeer(cfg.name, address)
    if s.name == "" {
        // none provided, use listen address
        s.name = s.address
    }
    if cfg.root == "" {
        s.root = s.address
    } else {
        s.root = cfg.root
    }
    if s.failover != "" {
        s.failoverStrat = FAILOVER_STRAT_MASTER_CFG
    }
    s.lastMsg = &Message{
        MsgType:    "NONE",
    }
    return s, nil
}

// Starts the PeerServer.
func (s *PeerServer) ListenAndServe() error {
    s.httpServer = &http.Server{
        Addr:    s.address,
        Handler: s.router,
        ReadTimeout: (HEARTBEAT_INTERVAL_MS * 2)*time.Millisecond,
    }

    // Protocol handlers
    s.router.HandleFunc("/multicast", s.multicastHandler)
    s.router.HandleFunc("/fwd", s.fwdHandler)
    s.router.HandleFunc("/join", s.joinHandler)
    s.router.HandleFunc("/leave", s.leaveHandler)
    s.router.HandleFunc("/confirm", s.confirmHandler)
	s.router.HandleFunc("/status", s.statusHandler)
	s.router.HandleFunc("/reflect", s.reflectStatusHandler)
	s.router.HandleFunc("/leaf", s.leafCheckinHandler)
	s.router.HandleFunc("/nominate", s.failoverNominateHandler)
	s.router.HandleFunc("/accept", s.failoverAcceptHandler)
	
    //Test / debug / configuration handlers
	s.router.HandleFunc("/configureFailover", s.configureFailoverHandler)
	s.router.HandleFunc("/topo", s.topologyHandler)
	s.router.HandleFunc("/update", s.updateHandler)
	s.router.HandleFunc("/command", s.commandHandler)
	s.router.HandleFunc("/debug", s.debugHandler)
	s.router.HandleFunc("/lastmsg", s.lastMsgHandler)
	s.router.HandleFunc("/msgstats", s.msgStatsHandler)
	s.router.HandleFunc("/die", s.dieHandler)


    go s.serve()

    go s.managePeers()

    if !s.isRoot() {
        log.Printf("%s: Starting as slave, listening at %v:", s.name, s.address)
        log.Printf("%s: Attempting to join root @ %v:", s.name, s.getRoot())
        if err := s.join(s.getRoot()); err != nil {
            log.Fatal(err)
        }
    } else {
        log.Printf("%s: Starting as root, listening at %v:", s.name, s.address)

        s.setParent(s.self, 0)
    }

    return nil
}

func (s *PeerServer) SetFailover(failover string) bool {
    return s.assignNewFailover(failover)
}

func (s *PeerServer) ForceCloseConnections() {
    s.forceClose = true
}

func (s *PeerServer) SetDebugFlag(flag int) {
    s.debugFlag |= flag
}

func (s *PeerServer) ResetDebugFlag() {
    s.debugFlag |= 0
}

// Do we want onMessage to be called for our own messages?
func (s *PeerServer) SetProcessLoopback(loopback bool) {
    s.doLoopback = loopback
}

func (s *PeerServer) GetPeerString() string {
    return s.self.String()
}

func (s *PeerServer) GetLastMsg() *Message {
    s.msglock.Lock()
    defer s.msglock.Unlock()
    return s.lastMsg
}

func (s *PeerServer) GetMsgStats() *MessageStats {
    s.msglock.Lock()
    defer s.msglock.Unlock()
    return &MessageStats {
        s.msgStats.LastMsgId,
        s.msgStats.NumMsgs,
        s.msgStats.CRCTally,
    }
}

func (s *PeerServer) Topology() *TopologyResponse {
    s.treelock.RLock()
    defer s.treelock.RUnlock()
    return &TopologyResponse {
        s.parent,
        len(s.children),
        s.getChildren(), 
    }
}

func (s *PeerServer) Die() {
    go func() {
        time.Sleep(1000 * time.Millisecond)
        log.Printf("%s: DYING!", s.name)
        os.Exit(0)
    }()
}

func (s *PeerServer) Stop() {
    s.srvStopChan <- "STOP"
    s.mgrStopChan <- "STOP"
    close(s.confirmChan)
}

func (s *PeerServer) Command(cmd string, args string) bool {
    return s.sendCommand(cmd, args, false)
}

func (s *PeerServer) Multicast(data string) bool {
    msg := &Message{
        MsgType:    MSG_TYPE_DAT,
        Source:     s.address,
        Forwarder:  s.address,
        Data:       data,
    }
    return s.multicastMsg(msg)
}


func (s *PeerServer) chkDebugFlag(flag int) bool {
    return (s.debugFlag & flag) != 0
}

func (s *PeerServer) sendCommand(cmd string, args string, subtreeOnly bool) bool {
    msg := &Message{
        MsgId:      0,  // Keep default of 0 for commands?
        MsgType:    MSG_TYPE_CMD,
        TTL:        DEFAULT_TTL,
        Source:     s.address,
        Forwarder:  s.address,
        Data:       fmt.Sprintf("%s:%s", cmd, args),
    }
    var b bytes.Buffer
    json.NewEncoder(&b).Encode(msg)

    if subtreeOnly || s.isRoot() {
        s.sendToChildren("/fwd", b.Bytes(), true)
        return true
    }
    return s.multicastMsg(msg)
}

// Hack to force close all connections. This is only really used for testing
// because in real usage a peer going down will cut all connections anyway
type ConnTrackingListener struct {
    net.Listener
    sync.Mutex
    conns    []net.Conn
}

func (ctl *ConnTrackingListener) Accept() (c net.Conn, err error) {
	c, err = ctl.Listener.Accept()
	if err == nil {
		ctl.Lock()
		ctl.conns = append(ctl.conns, c)
		ctl.Unlock()
	}
	return
}

func isConnClosed(c net.Conn) bool {
    c.SetReadDeadline(time.Now())
    one := make([]byte, 1)
    if _, err := c.Read(one); err == io.EOF {
        return true
    }
    return false
}

func (ctl ConnTrackingListener)  closeAllConnections() (int, int, int) {
    nconns := 0
    nclean := 0
    nclosed := 0
    ctl.Lock()
    for i := 0; i < len(ctl.conns); i++ {
        conn := ctl.conns[i]
        nconns++
        if isConnClosed(conn) {
            nclosed++
        }
        err := conn.Close()
        if err != nil {
            nclean++
        }
    }
    ctl.conns = make([]net.Conn, 0)
    ctl.Unlock()
    return nconns, nclosed, nclean
}
    
func (s *PeerServer) serve() {
    addr := s.httpServer.Addr
    if addr == "" {
        addr = ":http"
    }
    l, e := net.Listen("tcp", addr)
    if e != nil {
        msg := fmt.Sprintf("Listen error: %v", e)
        log.Fatal(msg)
    }
    var ctl *ConnTrackingListener
    if s.forceClose {
        ctl = &ConnTrackingListener{Listener: l}
        go s.httpServer.Serve(ctl)
    } else {
        go s.httpServer.Serve(l)
    }
    
    select {
        case <- s.srvStopChan:
            if s.forceClose {
                ctl.Close()
                nconns, nclosed, nclean := ctl.closeAllConnections()
                log.Printf("Server %v thread exit, closed %d connections, %d pre-closed, %d cleanly", s.GetPeerString(), nconns, nclosed, nclean)
            } else {
                l.Close()
                log.Printf("Server %v thread exit, closed ? connections, ? cleanly", s.GetPeerString())
            }
            return
    }
}

func makeUrl(host, path string) string {
    return "http://"+host+path
}

func (s *PeerServer) status(peer *Peer) *StatusResponse {
    t := time.Now()
    
    statusReq := &StatusRequest{
        Name:       s.name,
        Address:    s.address,
        ClusterId:  s.clusterId,
    }

	statusResp := &StatusResponse{}

    onStatusOK := func(p *Peer, body io.Reader) error {
    	if err := json.NewDecoder(body).Decode(&statusResp); err != nil {
            log.Printf("%s: STATUS RESP for %v, parsing failed - %v : %v!", s.name, p, body, err)
            statusResp = &StatusResponse {
               false, 
               make([]*Peer, 0), 
               -1,
               -1,
           }
           return err
    	}
        statusResp.Alive = true
        statusResp.RTT = int(time.Since(t).Seconds()/1000.0)
        return nil 
    }

    var b bytes.Buffer
    json.NewEncoder(&b).Encode(statusReq)
    err := s.postAndProcess(peer, "/status", &b, "status", false, onStatusOK, nil)

    if err != nil {
        log.Printf("%s: STATUS failed %v : %v!", s.name, peer, err)
        return &StatusResponse {
           false, 
           make([]*Peer, 0), 
           -1,
           -1,
       }
    }
    return statusResp
}

func (s *PeerServer) statusHandler(w http.ResponseWriter, req *http.Request) {

    statusReq := &StatusRequest{}
	if err := json.NewDecoder(req.Body).Decode(&statusReq); err != nil {
	   log.Printf("%s: Unable to parse STATUS REQ - %v : %v!", s.name, req.Body, err)
	} else {
        go func() {
            s.treelock.RLock()
        	defer s.treelock.RUnlock()
        	var peer *Peer
        	if (s.parent != nil) && (statusReq.Address == s.parent.Address) {
        	   peer = s.parent
        	} else {
        	   peer = s.children[statusReq.Name]
        	}
            if peer != nil {
                peer.lastSeen = time.Now()
            }
        }()
	}

    statusResp := &StatusResponse {
        true, 
        s.getChildren(),
        s.getLastMsgId(),
        0,
    }

    var b bytes.Buffer
    json.NewEncoder(&b).Encode(statusResp)
    w.Write(b.Bytes())
}

//check status for somebody else
func (s *PeerServer) reflectStatusHandler(w http.ResponseWriter, req *http.Request) {
    statusReq := &ReflectStatusRequest{}
	if err := json.NewDecoder(req.Body).Decode(&statusReq); err != nil {
	   log.Printf("%s: Unable to parse REFLECT STATUS REQ - %v : %v!", s.name, req.Body, err)
	   http.Error(w, "Could not decode ReflectStatusRequestMsg", http.StatusBadRequest)
	   return
	}
    // ping target 
    statusResp := s.status(statusReq.Target)
    var b bytes.Buffer
    json.NewEncoder(&b).Encode(statusResp)
    w.Write(b.Bytes())
}

func (s *PeerServer) onCommand(source string, cmdargs string) {
    tokens := strings.SplitN(cmdargs, ":", 2)
    cmd := tokens[0]
    args := ""
    if len(tokens) > 1 {
        args = tokens[1]
    }
    
    switch cmd {
    case CMD_NEW_ROOT:
        // verify this was the right failover
        failover := s.getFailover()
        newroot := args
        if failover != "" && failover != newroot {
            log.Printf("%s: New failover %s does not match configured failover %s", s.name, newroot, failover)
        } else {
            s.setRoot(newroot)
            log.Printf("%s: New root %s", s.name, newroot)
        }

    case CMD_NEW_FAILOVER:
        // verify this is from the root
        failover := args
        root := s.getRoot()
        if root != source {
            log.Printf("%s: Source %s does not match root %s. Only root can set new failover, rejecting %s.", s.name, source, root, failover)
        } else {
            s.setFailover(failover)
        }

    case CMD_PROMPT_LEAVES: 
        //log.Printf("%s: Got prompt from %s, have kids %d", s.name, source, s.numChildren())
        if s.numChildren() < 1 {
            s.leafCheckin(source)
        }

    case CMD_DIE:
        s.Die()

    default:
        log.Printf("%s: Got unknown command '%s', ignoring.", s.name, cmd)
    }
}

func (s *PeerServer) fwdHandler(w http.ResponseWriter, req *http.Request) {
	msg := &Message{}
	if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
	      //gossip failed
        log.Printf("%s: FORWARD decode failed %v : %v!", s.name, msg, err)
        http.Error(w, "Could not decode msg to forward", http.StatusInternalServerError)
		return
	}

	fwdErr := s.fwdMsg(msg)
    if fwdErr != nil {
        http.Error(w, fwdErr.Error(), http.StatusInternalServerError)
        return
    }
}

// This is only actually performed by the root
func (s *PeerServer) multicastHandler(w http.ResponseWriter, req *http.Request) {
    req.Close = true
    root := s.getRoot()
    if s.address != root {
        //redirect request
        log.Printf("%s: MULTICAST [%v] redirect request %v to %v", s.name, s.address, req, root)
        http.Redirect(w, req, makeUrl(root, "/multicast"), http.StatusMovedPermanently)
        return
    }
    
    msg := &Message{}
    if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
        log.Printf("%s: MULTICAST request from %v FAILED! BAD JSON", s.name, msg.Source)
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    msg.MsgId = s.getNextMsgId()
    msg.TTL = DEFAULT_TTL
    msg.Forwarder = s.address

    fwdErr := s.fwdMsg(msg)
    if fwdErr != nil {
        http.Error(w, fwdErr.Error(), http.StatusInternalServerError)
        return
    }
}