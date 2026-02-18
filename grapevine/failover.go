package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

/**
 * TODO: Outsource all this to raft
 *
 * @author kkandeka
 */

// Master replacement strategies - Most not implemented yet
const (
	FAILOVER_STRAT_NONE       = 0 // Wait for master to come back
	FAILOVER_STRAT_CFG        = 1 // Manually configure a failover for each node
	FAILOVER_STRAT_MASTER_CFG = 2 // Manually configure the master to assign a failover
	FAILOVER_STRAT_LAST_LEAF  = 3 // Root picks the last leaf to join, because it will certainly have no children [NOT IMPLEMENTED]
)

type LeafCheckin struct {
	Name      string `json:"name"`
	Address   string `json:"address"`
	ClusterId string `json:"clusterId"`
}

type FailoverNominate struct {
	Name        string  `json:"name"`
	Address     string  `json:"address"`
	ClusterId   string  `json:"clusterId"`
	MayBeLeaves []*Peer `json:"leaves"`
}

type FailoverResponse struct {
	Accept    bool   `json:"accept"`
	Name      string `json:"name"`
	Address   string `json:"address"`
	ClusterId string `json:"clusterId"`
}

func (s *PeerServer) getFailover() string {
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	return s.failover
}

func (s *PeerServer) assignNewFailover(failover string) bool {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	return s._assignNewFailover(failover)
}

func (s *PeerServer) _assignNewFailover(failover string) bool {
	if s._isRoot() {
		if failover != s.address {
			s.failover = failover
			s.doneFindingFailover()
			return true
		} else {
			log.Printf("%s: Failover cannot be same as root: %s", s.name, failover)
		}
	} else {
		log.Printf("%s: We are not root, cannot set failover master %s", s.name, failover)
	}
	return false
}

func (s *PeerServer) announceNewFailover(failover string) {
	//issue a command letting everyone know the new failover
	go s.Command(CMD_NEW_FAILOVER, failover)
}

func (s *PeerServer) _setMaybeFailover() {
	s.maybeFailover = true
}

func (s *PeerServer) setMaybeFailover() {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	s._setMaybeFailover()
}

func (s *PeerServer) _resetMaybeFailover() {
	s.maybeFailover = false
}

func (s *PeerServer) resetMaybeFailover() {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	s._resetMaybeFailover()
}

func (s *PeerServer) setFailover(newfailover string) {
	s.treelock.Lock()
	defer s.treelock.Unlock()
	if newfailover == s.root {
		log.Printf("%s: Failover cannot be same as root: %s", s.name, newfailover)
	} else {
		s.failover = newfailover
		s.maybeFailover = false
		log.Printf("%s: New failover '%s'", s.name, s.failover)
	}
}

func (s *PeerServer) leafCheckin(host string) {
	checkin := &LeafCheckin{
		s.name,
		s.address,
		s.clusterId,
	}
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(checkin)
	s.fireAndForget(NewPeer("", host), "/leaf", &b, "leafCheckin")
}

func (s *PeerServer) leafCheckinHandler(w http.ResponseWriter, req *http.Request) {
	req.Close = true

	leafCheckin := &LeafCheckin{}
	if err := json.NewDecoder(req.Body).Decode(&leafCheckin); err != nil {
		log.Printf("%s: LEAFCHECKIN request from %v FAILED! BAD JSON: %v", s.name, leafCheckin, req)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if leafCheckin.ClusterId != s.clusterId {
		errMsg := fmt.Sprintf("%s: %s request from %v FAILED! Requested cluster %v != actual cluster %v", s.name, "LEAFCHECKIN", leafCheckin.Address, leafCheckin.ClusterId, s.clusterId)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}
	//log.Printf("%s: LEAFCHECKIN from %v", s.name, leafCheckin)
	s.treelock.Lock()
	leaf := s._getPeer(leafCheckin.Name, leafCheckin.Address)
	s._appendLeaf(leaf)
	s.treelock.Unlock()

	s.leafMonChan <- leaf
}

/*
Failover flow:
 1. Root tracks (or queries for) leaf nodes
    a) If no leaves are available, root floods a "leafcheckin" command, forcing all leaf nodes to check in
    b) On check-in, it may proceed to step 2 if failover node is still not decided.
 2. Root nominates (preferably the most recently seen) node as failover by sending a FailoverNominate request
 3. Nominated leaf checks if it is still a leaf (i.e. no children). If so it returns a FailoverAccept message with the Accept fields set true/false accordingly
    a) In the meantime the leaf stops accepting new children for a specified time.
 4. On getting a FailoverAccept message from a leaf, the root checks if it still needs a failover (or has one been appointed in the meantime)
 5. If a failover needs to be set, the root accepts it with a 200 and assigns it as the new failover.
    a) This step involves flooding a new command down the tree instructing nodes about the new failover. This way all sub-nodes know who the failover is if the root goes down.
 6. On being accepted (i.e. 200 for positive FailoverAccept), the leaf checks that it's not already at the root, and if not, leaves its current position and joins the root.

So the basic flow is: root -- FailoverNominate --> leaf, leaf -- FailoverAccept --> root
*/
func (s *PeerServer) _nominateFailover(leaf *Peer) {
	//log.Printf("%s: NOMINATING FAILOVER %v @ %v", s.name, leaf.Name, leaf.Address)
	nominateReq := &FailoverNominate{
		s.name,
		s.address,
		s.clusterId,
		s.mayBeLeaves,
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(nominateReq)
	s.fireAndForget(leaf, "/nominate", &b, "nominateFailover")
}

func (s *PeerServer) failoverNominateHandler(w http.ResponseWriter, req *http.Request) {
	req.Close = true
	nominateReq := &FailoverNominate{}
	err := json.NewDecoder(req.Body).Decode(&nominateReq)
	if err != nil {
		errMsg := fmt.Sprintf("%s: Decode %s failed %v : %v!", s.name, "FailoverNominate", nominateReq, err)
		log.Print(errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)

	go func(nominateReq *FailoverNominate) {
		// Maybe check if nomination comes from root?
		s.treelock.Lock()
		defer s.treelock.Unlock()

		rootPeer := s._getPeer(nominateReq.Name, nominateReq.Address)

		s._setMaybeFailover() // accept no more children; gets reset when we end up being failover node
		if s._numChildren() < 1 {
			//accept
			go func() {
				time.Sleep(TIMEOUT_RESET_MAYBE_FAILOVER)
				s.resetMaybeFailover() // timeout in case we DON'T end up being failover node
			}()

			failoverResp := &FailoverResponse{
				true,
				s.name,
				s.address,
				s.clusterId,
			}

			s.mayBeLeaves = nominateReq.MayBeLeaves

			onAcceptOK := func(peer *Peer, body io.Reader) error {
				s.treelock.Lock()
				parent := ""
				if s.parent != nil {
					parent = s.parent.Address
				}
				s.failover = s.address
				s.treelock.Unlock()

				if rootPeer.Address != parent {
					log.Printf("%s: Accepted as failover, leaving %s and rejoining root %s!", s.name, parent, rootPeer)
					s.leave()
					s.join(rootPeer.Address)
				} else {
					log.Printf("%s: Accepted as failover, already connected to root %s!", s.name, rootPeer)
				}
				return nil
			}

			//log.Printf("%s: NOMINATED FOR FAILOVER by %v, ACCEPTED", s.name, rootPeer)

			var fr bytes.Buffer
			json.NewEncoder(&fr).Encode(failoverResp)
			go s.postAndProcess(rootPeer, "/accept", &fr, "failoverNominationHandler", false, onAcceptOK, nil)

		} else {
			s._resetMaybeFailover()
			//log.Printf("%s: NOMINATED FOR FAILOVER by %v, DECLINED", s.name, rootPeer)
			// don't bother responding
			/*
			   //reject
			   failoverResp := &FailoverResponse {
			       false,
			       s.name,
			       s.address,
			       s.clusterId,
			   }

			   var fr bytes.Buffer
			   json.NewEncoder(&fr).Encode(failoverResp)
			   s.fireAndForget(rootPeer, "/accept", &fr, "failoverNominationHandler")
			*/
		}
	}(nominateReq)
}

func (s *PeerServer) failoverAcceptHandler(w http.ResponseWriter, req *http.Request) {
	//log.Printf("%s: FAILOVER RESPONSE, resp = %v", s.name, req.Body)
	s.treelock.Lock()
	defer s.treelock.Unlock()

	// do we still need failover?
	if s.root != s.address || s.failover != "" {
		http.Error(w, "Sorry, don't need failover anymore. Thanks for asking though", http.StatusInternalServerError)
		return
	}

	failoverResp := &FailoverResponse{}
	if err := json.NewDecoder(req.Body).Decode(&failoverResp); err != nil {
		errMsg := fmt.Sprintf("%s: FAILOVER-ACCEPT request from %v FAILED! BAD JSON: %v, req = %v", s.name, failoverResp, err, req)
		log.Print(errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	//log.Printf("%s: FAILOVER RESPONSE, parsed resp = %v", s.name, failoverResp)
	if failoverResp.ClusterId != s.clusterId {
		errMsg := fmt.Sprintf("%s: %s request from %v FAILED! Requested cluster %v != actual cluster %v", s.name, "FAILOVER-ACCEPT", failoverResp.Address, failoverResp.ClusterId, s.clusterId)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	if failoverResp.Accept {
		log.Printf("%s: Found new failover: %s", s.name, failoverResp.Address)

		failoverPeer := s._getPeer(failoverResp.Name, failoverResp.Address)

		s._assignNewFailover(failoverResp.Address)

		if s.children[failoverPeer.Name] == nil {
			// It is not yet connected, check back in a second or so to ensure that it has now connected
			go func(failover *Peer) {
				time.Sleep(TIMEOUT_FAILOVER_CONNECT_CHECK)
				s.treelock.RLock()
				childPeer := s.children[failover.Name]
				s.treelock.RUnlock()
				if childPeer == nil {
					log.Printf("%s: Failover %s not connected, restarting process", s.name, failover.Address)
					s.setFailover("")
				}
			}(failoverPeer)
		} else {
			log.Printf("%s: Failover %v already connected", s.name, failoverPeer)
			s.announceNewFailover(failoverResp.Address)
		}
	} //else continue quest
}

// we only want one failover in progress
func (s *PeerServer) startFindingFailover() bool {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	if s.findingFailover {
		return false
	}
	s.findingFailover = true
	return true
}

func (s *PeerServer) isFindingFailover() bool {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	return s.findingFailover
}

func (s *PeerServer) doneFindingFailover() {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	s.findingFailover = false
}

func (s *PeerServer) findNewFailoverPeer() {
	if !s.isRoot() {
		log.Printf("%s: Not root, don't have authority to find new failover", s.name)
		return
	}

	if s.getFailover() != "" {
		log.Printf("%s: Already have failovr %v, no need to find new failover", s.name, s.getFailover())
		return
	}
	if !s.startFindingFailover() {
		//already finding failover
		return
	}

	log.Printf("%s: Quest for new failover", s.name)
	//pop a leaf - ask all leaves to contact us

	s.treelock.RLock()
	defer s.treelock.RUnlock()
	numNominated := 0
	for i := len(s.mayBeLeaves) - 1; i >= 0; i-- {
		log.Printf("%s: Nominating %d/%d : %v (last seen %v)", s.name, i, len(s.mayBeLeaves), s.mayBeLeaves[i], s.mayBeLeaves[i].lastSeen)
		s.leafMonChan <- s.mayBeLeaves[i]
		numNominated++
	}

	go func() {
		if numNominated > 0 {
			time.Sleep(TIMEOUT_NO_FAILOVER_PROMPT_LEAVES)
		}
		if s.getFailover() == "" {
			//Unsuccessful with ld leaves - ask all leaves to contact us
			s.Command(CMD_PROMPT_LEAVES, s.address)
			go func() {
				time.Sleep(TIMEOUT_FAILOVER)
				s.doneFindingFailover()
			}()

		}
	}()
	log.Printf("%s: Sent nominations for failover", s.name)
}

func (s *PeerServer) startFailover(failover string) {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	s.isFailover = true
	s.setRoot(failover)
}

func (s *PeerServer) isFailingOver() bool {
	s.joinlock.Lock()
	defer s.joinlock.Unlock()
	return s.isFailover
}
