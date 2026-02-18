package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

func (s *PeerServer) setLastMsg(msg *Message) {
	s.msglock.Lock()
	defer s.msglock.Unlock()
	s.lastMsgId = msg.MsgId
	s.lastMsg = msg
	s.msgStats.NumMsgs++
	s.msgStats.LastMsgId = msg.MsgId
	if s.chkDebugFlag(DEBUG_FLAG_TALLYCRC) {
		// this will not be totally consistent because messages may arrive out of order, but will be eventually be
		s.msgStats.CRCTally ^= crc32.ChecksumIEEE([]byte(msg.Data))
	}
}

func (s *PeerServer) getLastMsgId() int {
	s.msglock.Lock()
	defer s.msglock.Unlock()
	return s.lastMsgId
}

func (s *PeerServer) getNextMsgId() int {
	s.msglock.Lock()
	defer s.msglock.Unlock()
	s.lastMsgId++
	return s.lastMsgId
}

func (s *PeerServer) fireAndForget(peer *Peer, path string, b *bytes.Buffer, msgType string) bool {
	url := makeUrl(peer.Address, path)
	peer.wlock.Lock()
	defer peer.wlock.Unlock()

	resp, err := http.Post(url, "application/json", b)
	status := http.StatusOK
	if resp != nil {
		status = resp.StatusCode
		resp.Body.Close()
	}
	return (err == nil) && (status == http.StatusOK)
}

// onStatusOK is a callback on successful POST
// onError is a callback on an error. Signature is a boolean flag indicating if the error was after Post or after ReadAll, and the error itself
func (s *PeerServer) postAndProcess(peer *Peer, path string, b *bytes.Buffer, msgType string,
	logerrs bool, onStatusOK func(*Peer, io.Reader) error, onError func(bool, error)) error {
	url := makeUrl(peer.Address, path)
	peer.wlock.Lock()
	defer peer.wlock.Unlock()

	resp, err := http.Post(url, "application/json", b)
	if err == nil {
		defer resp.Body.Close()
		body, err1 := ioutil.ReadAll(resp.Body)
		if err1 != nil {
			if onError != nil {
				onError(false, err1)
			}
			if logerrs {
				log.Printf("%s: Read %s failed for %v : %v!", s.name, msgType, resp, err1)
			}
		} else {
			if resp.StatusCode == http.StatusOK {
				return onStatusOK(peer, bytes.NewBuffer(body))
			} else if err1 != nil {
				log.Printf("%s: %s non-200 status %d  : host=%v req=%v : resp=%v!", s.name, msgType, resp.StatusCode, resp.Request.URL.Host, resp.Request, resp)
			}
		}
	} else {
		if resp != nil {
			resp.Body.Close()
		}
		if onError != nil {
			onError(true, err)
		}
		log.Printf("%s: Post to %v for %s failed: %v \n", s.name, peer, msgType, err)
	}
	return err
}

// signature for onStatusOK and onError is same as for postAndProcess
func (s *PeerServer) sendToPeers(peers []*Peer, path string, data []byte, waitforit bool,
	msgType string, logerrs bool, onStatusOK func(*Peer, io.Reader) error, onError func(bool, error)) {
	if waitforit {
		// wait so that we execute command only after forwarding it
		// This is because we don't want to die before tell our children to die
		var wg sync.WaitGroup
		for _, peer := range peers {
			if peer.Address != s.address {
				wg.Add(1)
				go func(p *Peer) {
					defer wg.Done()
					s.postAndProcess(p, path, bytes.NewBuffer(data), msgType, logerrs, onStatusOK, onError)
				}(peer)
			}
		}
		wg.Wait()
	} else {
		for _, peer := range s.children {
			if peer.Address != s.address {
				s.postAndProcess(peer, path, bytes.NewBuffer(data), msgType, logerrs, onStatusOK, onError)
			}
		}
	}
}

func (s *PeerServer) writeAndPruneIfDead(peer *Peer, path string, data []byte) {
	peer.wlock.Lock()
	defer peer.wlock.Unlock()

	resp, err := http.Post(makeUrl(peer.Address, path), "application/json", bytes.NewReader(data))
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		log.Printf("%s: FORWARD msg to %v failed : %v", s.name, peer.Address, err)
		s.pruneChan <- peer
		return
	}
}

func (s *PeerServer) sendToChildren(path string, data []byte, waitforit bool) {
	/*s.sendToPeers(s.getChildren(), data, waitforit, "FORWARD", false,
	  func(body io.Reader) error {
	      return nil
	  },
	  func (isPostError bool, err error) {
	  	if isPostError {
	  		log.Printf("%s: FORWARD msg to %v failed : %v", s.name, peer.Address, err)
	  		s.pruneChan <- peer
	  	}
	  })*/
	if waitforit {
		// wait so that we execute command only after forwarding it
		// This is because we don't want to die before tell our children to die
		var wg sync.WaitGroup
		for _, peer := range s.children {
			if peer.Address != s.address {
				wg.Add(1)
				go func(p *Peer) {
					defer wg.Done()
					s.writeAndPruneIfDead(p, path, data)
				}(peer)
			}
		}
		wg.Wait()
	} else {
		for _, peer := range s.children {
			if peer.Address != s.address {
				go s.writeAndPruneIfDead(peer, path, data)
			}
		}
	}
}

func (s *PeerServer) fwdMsg(msg *Message) error {
	//make sure forwarder is our parent, else drop it
	s.treelock.RLock()
	defer s.treelock.RUnlock()
	if s.parent == nil {
		log.Printf("%s: Parent is nil, forwarded from %v (parent=%v), possible net split! dropping %v!", s.name, msg.Forwarder, s.parent, msg)
		return errors.New("Parent gone, possible net split")
	}
	if msg.Forwarder != s.parent.Address {
		log.Printf("%s: FORWARD from non parent %v (parent = %v), possible loop! dropping %v!", s.name, msg.Forwarder, s.parent, msg)
		return errors.New("Must not forward messages from non-parent")
	}

	//invoke callback if we didn't originate this msg
	if (s.onMessage != nil) && (msg.MsgType == MSG_TYPE_DAT) {
		if (msg.Source != s.address) || s.doLoopback {
			go s.onMessage(msg)
		}
	}

	// Set forwarder as us, so that downstream peers will accept and forward this msg
	msg.Forwarder = s.address
	msg.TTL -= 1

	if msg.TTL < 1 {
		log.Printf("%s: Dropping msg due to insufficient TTL: children=%v msg=%v", s.name, s.children, msg)
		return nil
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(msg)

	if s.chkDebugFlag(DEBUG_FLAG_FWD) && len(s.children) > 0 {
		log.Printf("%s: Multicasting #%05d -> %v", s.name, msg.MsgId, s.children)
	}
	if msg.MsgType == MSG_TYPE_DAT {
		//sanity checks to ensure no duplicate messages are encountered?
		s.setLastMsg(msg)
		s.sendToChildren("/fwd", b.Bytes(), false)
	} else {
		go s.onCommand(msg.Source, msg.Data)
		s.sendToChildren("/fwd", b.Bytes(), true)
	}
	return nil
}

func (s *PeerServer) multicastMsg(msg *Message) bool {
	//ask root to multicast msg for us
	expdelay := 20
	attempts := 0
	//log.Printf("%s Multicasting %v", s.name, root)

	for {
		var b bytes.Buffer
		json.NewEncoder(&b).Encode(msg)

		host := s.getRoot() // could be ourselves

		resp, err := http.Post(makeUrl(host, "/multicast"), "application/json", &b)
		if resp != nil {
			defer resp.Body.Close()
		}

		if err == nil {
			//check status code
			if resp.StatusCode == http.StatusOK {
				return true
			} else {
				log.Printf("%s: MULTICAST non-200 status %d!", s.name, resp.StatusCode)
			}
		} else {
			log.Printf("%s: Post to %s for JOIN failed: %v \n", s.name, host, err)
		}
		attempts++
		if attempts > MAX_BCAST_ATTEMPTS {
			break
		}
		log.Printf("%s: MULTICAST to %v FAILED! %v retrying # %d after %d ms", s.name, host, err, attempts, expdelay)
		time.Sleep(time.Duration(expdelay) * time.Millisecond)
		if expdelay < 30*1000 {
			expdelay = expdelay * 2
		}
	}
	//log.Fatal(fmt.Sprintf("%v: MULTICAST %v timed out", s.name, msg))
	return false
}
