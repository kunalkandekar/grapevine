package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

/**
 * @author kkandeka
 */

const (
	JOIN_RESP_CODE_NO_VACANCY    = 0
	JOIN_RESP_CODE_ACCEPT        = 1
	JOIN_RESP_CODE_WRONG_CLUSTER = 2
)

const (
	TIMEOUT_JOIN_COMPLETE               = 5000 * time.Millisecond
	TIMEOUT_FIRST_KIDS_WITHOUT_FAILOVER = 3000 * time.Millisecond
	TIMEOUT_RESET_MAYBE_FAILOVER        = 5000 * time.Millisecond
	TIMEOUT_FAILOVER                    = 3000 * time.Millisecond
	TIMEOUT_NO_FAILOVER_PROMPT_LEAVES   = 3000 * time.Millisecond
	TIMEOUT_FAILOVER_CONNECT_CHECK      = 2000 * time.Millisecond
	TIMEOUT_SINCE_LAST_CHECK            = 3000 * time.Millisecond
)

type JoinRequest struct {
	Name       string `json:"name"`
	Address    string `json:"address"`
	ClusterId  string `json:"clusterId"`
	IsFailover bool   `json:"isFailover"`
}

type JoinResponse struct {
	Code     int     `json:"code"`
	Name     string  `json:"name"`
	Root     string  `json:"root"`
	Failover string  `json:"failover"`
	Children []*Peer `json:"children"`
	Depth    int     `json:"depth"`
}

type LeaveRequest struct {
	Name      string `json:"name"`
	Address   string `json:"address"`
	ClusterId string `json:"clusterId"`
}

type Peer struct {
	Name        string `json:"name"`
	Address     string `json:"address"`
	ClusterId   string `json:"clusterId"`
	children    []*Peer
	numChildren int
	rttMsec     int
	depth       int
	wlock       sync.Mutex // to ensure one concurrent write/peer to avoid creating too many connections
	lastSeen    time.Time
}

func (p *Peer) String() string {
	return fmt.Sprintf("[%s @ %s]", p.Name, p.Address)
}

func NewPeer(pname string, paddr string) *Peer {
	p := &Peer{
		Name:    pname,
		Address: paddr,
	}
	return p
}

func (s *PeerServer) _getPeer(name string, addr string) *Peer {
	peer, inall := s.peersName[name]
	if !inall {
		peer, inall = s.peersAddr[addr]
		if !inall {
			// track all peers we see, just in case
			peer = NewPeer(name, addr)
			s.peersName[name] = peer
			s.peersAddr[addr] = peer
		}
	}
	peer.lastSeen = time.Now()
	return peer
}

func (s *PeerServer) getPeer(name string, addr string) *Peer {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	return s._getPeer(name, addr)
}

func (s *PeerServer) runConfirmLoop(joinReq *JoinRequest) {
	for {
		confirmHost, ok := <-s.confirmChan
		if !ok {
			log.Printf("Exiting confirm routine")
			return
		}
		oldParent := s.getParent()
		if oldParent != nil {
			if s.chkDebugFlag(DEBUG_FLAG_JOIN) {
				log.Printf("%s: Already joined %v, skipping CONFIRM to %v", s.name, oldParent, confirmHost)
			}
			continue
		}
		//don't spawn a new subroutine because this MUST be sequential,
		//as we don't want to confirm multiple parents simultaneously
		s.sendConfMsg(confirmHost, joinReq)
	}
}

func (s *PeerServer) handleDeadParent() {
	parent := s.getParent()
	if parent == s.self {
		log.Printf("%s: Parent %v is us and seems to be gone. We can't be dead!", s.name, parent)
		return
	}

	s.setParent(nil, -1)

	// something is wrong, rejoin tree from root
	root := s.getRoot()

	if parent != nil && parent.Address == root {
		//this is the top-level cluster - check for failover
		s.failoverChan <- root
	} else {
		log.Printf("%s: Parent %v seems to be gone. Re-join will be triggered at next heartbeat", s.name, parent)
	}
}

func (s *PeerServer) managePeers() {
	joinReq := &JoinRequest{
		Name:      s.name,
		Address:   s.address,
		ClusterId: s.clusterId,
	}
	//start confirm loop
	go s.runConfirmLoop(joinReq)

	checkStatus := func(peer *Peer, t time.Time, onPeerDead func(peer *Peer)) {
		// Peer is assumed dead if it hasn't pinged us in too long, OR if we can't ping it now
		timeSinceLastSeen := t.Sub(peer.lastSeen)
		if timeSinceLastSeen > TIMEOUT_SINCE_LAST_CHECK {
			log.Printf("%s: peer %v not seen in %v, (now = %v, last seen = %v) DEAD!", s.name, peer, timeSinceLastSeen, t, peer.lastSeen)
			onPeerDead(peer)
			return
		}
		status := s.status(peer)
		if !status.Alive {
			onPeerDead(peer)
		} else {
			peer.numChildren = len(status.Children)
			peer.children = status.Children
			peer.rttMsec = status.RTT
			peer.lastSeen = time.Now()
		}
	}

	failoverMap := make(map[string]bool)

	for {
		select {
		case t := <-s.heartbeat:
			parent := s.getParent()
			if parent != nil {
				//log.Printf("%s: Monitoring %v %#v", s.name, s.getParent(), s.children)
				if !s.isRoot() {
					go checkStatus(parent, t, func(peer *Peer) {
						s.handleDeadParent()
					})
				}

				s.treelock.RLock()
				if s._isRoot() && len(s.children) > 0 && s.failover == "" {
					// if we are root AND we have children AND we don't have failover, begin quest for new failover
					failoverMap = make(map[string]bool)
					go s.findNewFailoverPeer()
				}

				jitterRange := len(s.children) * 10
				for _, peer := range s.children {
					go func(p *Peer) {
						//random jitter just in case
						time.Sleep(time.Duration(rand.Intn(jitterRange)) * time.Millisecond)
						checkStatus(peer, t, func(p *Peer) {
							s.pruneChan <- p
						})
					}(peer)
				}
				s.treelock.RUnlock()
			} else {
				if !s.isJoinInProgress() {
					log.Printf("%s: Parent is missing, joining at root: %s", s.name, s.getRoot())
					// Sleep a bit to avoid thundering herd of rejoining siblings and
					// to let upstream peers detect dead peer and make vacancy accordingly
					delayMsec := rand.Intn(100)
					time.Sleep(time.Duration(delayMsec) * time.Millisecond)
					go s.join(s.getRoot())
				}
			}

		case joinHosts := <-s.joinChan:
			oldParent := s.getParent()
			if oldParent != nil {
				//log.Printf("%s: Already joined %v, skipping JOIN to %#v", s.name, oldParent, joinHosts)
				continue
			}
			//log.Printf("%s: sending JOINs to %d", s.name, len(joinHosts))
			// ugh
			joinReq.IsFailover = s.isFailingOver()
			for _, peer := range joinHosts {
				//log.Printf("%s: sending JOIN to %v", s.name, peer.Address)
				go s.sendJoinMsg(peer.Address, joinReq)
			}

		case pruneHost := <-s.pruneChan:
			s.removeChild(pruneHost.Name, "timing out")
			// is this a failover child though?
			if s.isRoot() && pruneHost.Address == s.getFailover() {
				s.setFailover("")
				log.Printf("%s: Failover %v left, find new one at next heartbeat", s.name, pruneHost.Address)
			}

		case leaf := <-s.leafMonChan:
			// do we need failover?
			//log.Printf("%s: Failover candidate %v", s.name, leaf)
			s.treelock.RLock()
			needFailover := (s._isRoot() && s.failover == "")
			//log.Printf("%s: Need?  %v", s.name, needFailover)
			if needFailover && !failoverMap[leaf.Address] {
				failoverMap[leaf.Address] = true
				//log.Printf("%s: Need a failover! Asking %v", s.name, leaf)
				go s._nominateFailover(leaf)
			}
			s.treelock.RUnlock()

		case _, ok := <-s.failoverChan:
			if !ok {
				log.Printf("Exiting failover routine")
				return
			}
			root := s.getRoot()
			if root != "" && root == s.address {
				log.Printf("%s: We are root %v, already failed over.", s.name, root)
				continue
			}
			// TODO try to get consensus on whether parent is really dead?
			failover := s.getFailover()
			if failover == "" {
				//uh oh root is gone and we have no failover! Hope it comes back soon!
				log.Printf("%s: Parent %v is root, and it's gone! Checking again later", s.name, root)
				// TODO elect a new one
			} else if failover == s.address {
				// We are failover! Verify parent is dead
				parent := s.getOldParent()
				if parent == nil {
					//uh oh root is gone and we have no failover! Hope it comes back soon
					log.Printf("%s: Parent %v is root, and old parent is nil too! Checking again later", s.name, root)
					continue
				}

				/*reflectStatusReq := &ReflectStatusRequest {
				      Name:       s.name,
				      Address:    s.address,
				      ClusterId:  s.clusterId,
				      Target:     parent,
				  }
				  var b bytes.Buffer
				  json.NewEncoder(&b).Encode(reflectStatusReq)

				  var mut sync.Mutex
				  numSuccess := 0

				  onStatusOK := func(peer *Peer, body io.Reader) error {
				      statusResp := &StatusResponse{}
				  	if err := json.NewDecoder(body).Decode(&statusResp); err != nil {
				         return nil
				  	}
				      if statusResp.Alive {
				          mut.Lock()
				          numSuccess++
				          mut.Unlock()
				      }
				      return nil
				  }

				  s.sendToPeers(parent.children, "/reflect", b.Bytes(), true, "RELECT STATUS", true, onStatusOK, nil)*/

				// Take over as root
				s.setRoot(s.address)
				s.setParent(s.self, 0)
				s.setFailover("")
				log.Printf("%s: Parent %v is root, and it's gone! Taking over as root: %v / %v", s.name, root, parent, s.getRoot())
			} else {
				log.Printf("%s: Parent %v is root, and it's gone! Failing over to %v", s.name, root, failover)
				s.startFailover(failover)
			}

		case <-s.mgrStopChan:
			log.Printf("Exiting manager routine")
			return
		}
	}
}

// we only want one join in progress
func (s *PeerServer) startJoin() bool {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	if s.isJoining {
		return false
	}
	s.isJoining = true
	return true
}

func (s *PeerServer) isJoinInProgress() bool {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	return s.isJoining
}

func (s *PeerServer) doneJoin() {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	s.isJoining = false
	if s.isFailover {
		s.sendCommand(CMD_NEW_ROOT, s.root, true)
	}
	s.isFailover = false
}

func (s *PeerServer) _isRoot() bool {
	return s.root == s.address
}

func (s *PeerServer) isRoot() bool {
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	return s._isRoot()
}

func (s *PeerServer) getParent() *Peer {
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	return s.parent
}

func (s *PeerServer) getOldParent() *Peer {
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	return s.oldParent
}

func (s *PeerServer) setParent(peer *Peer, depth int) {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	s.oldParent = s.parent
	s.parent = peer
	s.depth = depth
}

func (s *PeerServer) getRoot() string {
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	return s.root
}

func (s *PeerServer) setRoot(newroot string) {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	s.root = newroot
	if s.failover == s.root {
		// no longer have a failover
		s.failover = ""
	}
}

func (s *PeerServer) _appendLeaf(peer *Peer) {
	extraLastJoined := len(s.mayBeLeaves) - MAX_LAST_JOINED
	if extraLastJoined < 0 {
		s.mayBeLeaves = append(s.mayBeLeaves, peer)
	} else {
		s.mayBeLeaves = append(s.mayBeLeaves[extraLastJoined+1:], peer)
	}
	//log.Printf("%s: MayBeLeaves (trimmed %d) %v", s.name, extraLastJoined, s.mayBeLeaves)
}

func (s *PeerServer) _addChild(peer *Peer) {
	s.children[peer.Name] = peer
	peer.lastSeen = time.Now()
	log.Printf("%s: Added peer: %v", s.name, peer)
}

func (s *PeerServer) removeChild(name string, reason string) {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	peer := s.children[name]
	delete(s.children, name)
	log.Printf("%s: Removed peer for %s: (%s) %v", s.name, reason, name, peer)
}

func (s *PeerServer) getChildren() []*Peer {
	peers := make([]*Peer, len(s.children))
	count := 0
	for _, peer := range s.children {
		peers[count] = peer
		count++
	}
	return peers
}

// We don't want to send JOINs to failover. We want it unloaded in case switchover needs to happen
func (s *PeerServer) getChildrenExceptFailover() []*Peer {
	if s.root != s.address || s.failover == "" {
		return s.getChildren()
	}

	peers := make([]*Peer, 0)
	for _, peer := range s.children {
		if peer.Address != s.failover {
			peers = append(peers, peer)
		}
	}
	return peers
}

func (s *PeerServer) _numChildren() int {
	return len(s.children)
}

func (s *PeerServer) numChildren() int {
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	return s._numChildren()
}

// Starts at the root and follows redirects until it can join the tree
func (s *PeerServer) join(root string) error {
	if !s.startJoin() {
		// join already in progress
		return nil
	}
	attempts := 0
	for {
		if s.getParent() != nil {
			return nil
		}

		if (attempts > 0) || s.chkDebugFlag(DEBUG_FLAG_JOIN) {
			log.Printf("%s: Trying to join root %v, attempt # %d", s.name, root, attempts)
		}

		s.joinChan <- []*Peer{&Peer{Name: "root", Address: root}}
		//wait some time
		time.Sleep(TIMEOUT_JOIN_COMPLETE)

		attempts++
		if attempts > MAX_JOIN_ATTEMPTS {
			break
		}
	}
	logmsg := fmt.Sprintf("%v: JOIN timed out", s.name)
	log.Fatal(logmsg)
	return nil
}

func (s *PeerServer) sendJoinMsg(host string, joinReq *JoinRequest) error {
	onJoin := func(joinResp *JoinResponse) error {
		s.confirmChan <- host
		return nil
	}
	return s.sendJoinOrConfirmMsg(host, "/join", joinReq, "JOIN", onJoin)
}

func (s *PeerServer) sendConfMsg(host string, joinReq *JoinRequest) error {
	onConfirm := func(joinResp *JoinResponse) error {
		parent := s.getPeer(joinResp.Name, host)
		s.setParent(parent, joinResp.Depth+1)
		if joinResp.Root != "" {
			s.setRoot(joinResp.Root)
		}
		if joinResp.Failover != "" {
			s.setFailover(joinResp.Failover)
		}
		log.Printf("%s: Joined at : %v", s.name, parent)
		s.doneJoin()
		return nil
	}
	return s.sendJoinOrConfirmMsg(host, "/confirm", joinReq, "CONFIRM", onConfirm)
}

func (s *PeerServer) sendJoinOrConfirmMsg(host string, path string, joinReq *JoinRequest, msgType string, onCanJoin func(joinResp *JoinResponse) error) error {
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(joinReq)

	onStatusOK := func(p *Peer, body io.Reader) error {
		joinResp := &JoinResponse{}
		if err2 := json.NewDecoder(body).Decode(&joinResp); err2 != nil {
			log.Printf("%s: Decode %s failed %v : %v!", s.name, msgType, joinResp, err2)
		} else {
			switch joinResp.Code {
			case JOIN_RESP_CODE_ACCEPT:
				return onCanJoin(joinResp)

			case JOIN_RESP_CODE_WRONG_CLUSTER:
				// We are barking up the wrong tree!!
				return errors.New(fmt.Sprintf("Wrong tree: This is not %s", s.clusterId))
				s.doneJoin()

			case JOIN_RESP_CODE_NO_VACANCY:
				log.Printf("%s: %s redirected us to %v", s.name, host, joinResp.Children)
				s.joinChan <- joinResp.Children
			}
		}
		return nil
	}
	return s.postAndProcess(NewPeer("root", host), path, &b, msgType, true, onStatusOK, nil)
}

func (s *PeerServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	s.joinOrConfirmHandler("JOIN", false, w, req)
}

func (s *PeerServer) confirmHandler(w http.ResponseWriter, req *http.Request) {
	s.joinOrConfirmHandler("CONFIRM", true, w, req)
}

func (s *PeerServer) joinOrConfirmHandler(method string, isConfirm bool, w http.ResponseWriter, req *http.Request) {
	req.Close = true
	//log.Printf("%s: %s request from %v", s.name, method, req)
	joinReq := &JoinRequest{}
	if err := json.NewDecoder(req.Body).Decode(&joinReq); err != nil {
		log.Printf("%s: %s request from %v FAILED! BAD JSON: %v", s.name, method, joinReq, req)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.treelock.Lock()
	defer s.treelock.Unlock()

	children := s.getChildrenExceptFailover()

	if joinReq.ClusterId != s.clusterId {
		errMsg := fmt.Sprintf("%s: %s request from %v FAILED! Requested cluster %v != actual cluster %v", s.name, method, joinReq.Address, joinReq.ClusterId, s.clusterId)
		log.Print(errMsg)
		joinResp := &JoinResponse{
			JOIN_RESP_CODE_WRONG_CLUSTER,
			"",
			"",
			"",
			make([]*Peer, 0),
			-1,
		}

		var b bytes.Buffer
		json.NewEncoder(&b).Encode(joinResp)
		w.Write(b.Bytes())
		return
	}

	peer := s._getPeer(joinReq.Name, joinReq.Address)

	existing, inmap := s.children[joinReq.Name]

	var respCode int
	if len(children) >= s.capacity || ((s.root != s.address) && s.maybeFailover && !joinReq.IsFailover) {
		respCode = JOIN_RESP_CODE_NO_VACANCY
	} else {
		respCode = JOIN_RESP_CODE_ACCEPT
	}
	if s.failover == s.address {
		// We are the failover, we don't accept any JOINs
		respCode = JOIN_RESP_CODE_NO_VACANCY
	} else if s._isRoot() && joinReq.Address == s.failover {
		// We are the root and this is the failover, accept by default
		respCode = JOIN_RESP_CODE_ACCEPT
		if isConfirm {
			go s.announceNewFailover(joinReq.Address)
		}
	}

	joinResp := &JoinResponse{
		respCode,
		s.name,
		s.root,
		s.failover,
		children,
		s.depth,
	}

	if !inmap {
		if joinResp.Code == JOIN_RESP_CODE_ACCEPT {
			if isConfirm {
				//log.Printf("%s: %s successful, added %v @ %v! resp=%v", s.name, method, joinReq.Name, joinReq.Address, joinResp)
				s._addChild(peer)
			} else {
				//log.Printf("%s: %s have vacancy, canjoin %v @ %v! resp=%v", s.name, method, joinReq.Name, joinReq.Address, joinResp)
				// sleep an amount proportional to our load. This should probabilistically
				// distribute load more or less evenly, because the less loaded peers will respond more quickly
				time.Sleep(time.Duration(DELAY_PER_CHILD_MS*len(s.children)) * time.Millisecond)
			}
		} else {
			s._appendLeaf(peer)
			if s.chkDebugFlag(DEBUG_FLAG_JOIN) {
				log.Printf("%s: %s no vacancy for %v @ %v! tryAt=%v", s.name, method, joinReq.Name, joinReq.Address, joinResp)
			}
		}
	} else if existing.Address != joinReq.Address {
		//namespace collision!
		log.Printf("%s: Duplicate peer names!! [%s @ %s] and [%s @ %s], dropping request", s.name, joinReq.Name, joinReq.Address, existing.Name, existing.Address)
		http.Error(w, "Duplicate peer name!", http.StatusBadRequest)
		return
	} else {
		//else we already have this child in our map
		log.Printf("%s: %s already in children %v @ %v! resp=%v", s.name, method, joinReq.Name, joinReq.Address, joinResp)
		joinResp.Code = JOIN_RESP_CODE_ACCEPT
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(joinResp)
	w.Write(b.Bytes())
}

func (s *PeerServer) leave() bool {
	leaveReq := &LeaveRequest{
		Name:      s.name,
		Address:   s.address,
		ClusterId: s.clusterId,
	}
	log.Printf("%s: LEAVE", s.name)
	parent := s.getParent()
	s.setParent(nil, -1)
	if parent != nil {
		url := makeUrl(parent.Address, "/leave")
		var b bytes.Buffer
		json.NewEncoder(&b).Encode(leaveReq)

		resp, err := http.Post(url, "application/json", &b)
		if err != nil {
			log.Printf("%s: LEAVE from %v FAILED! %v", s.name, parent, err)
		} else {
			resp.Body.Close()
		}
	}

	// tell children too
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(leaveReq)
	s.sendToChildren("/leave", b.Bytes(), false)
	return true
}

func (s *PeerServer) leaveHandler(w http.ResponseWriter, req *http.Request) {
	// remove peer
	req.Close = true

	leaveReq := &LeaveRequest{}
	if err := json.NewDecoder(req.Body).Decode(&leaveReq); err != nil {
		log.Printf("%s: LEAVE request from %v FAILED! BAD JSON: %v", s.name, leaveReq, req)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if leaveReq.ClusterId != s.clusterId {
		errMsg := fmt.Sprintf("%s: %s request from %v FAILED! Requested cluster %v != actual cluster %v", s.name, "LEAVE", leaveReq.Address, leaveReq.ClusterId, s.clusterId)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}
	if leaveReq.Address == s.getParent().Address {
		//uh oh, we need to rejoin
		s.handleDeadParent()
	} else {
		s.removeChild(leaveReq.Name, "leaving")
	}
}
