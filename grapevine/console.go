package main

import (
	"bufio"
    "encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
    "net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

/**
 * @author kkandeka
 */

const (
    BASE_PORT = 4000
)

type TestConsoleApp struct {
    maxChildren int
    inproc bool
} 

func NewTestConsoleApp(c int, ip bool) *TestConsoleApp {
    return &TestConsoleApp {
        maxChildren: c,
        inproc: ip,
    }
}


type PeerServerRef interface {
    Address() string

    Name() string

    SetFailover(failover string) bool

    SetDebugFlag(flag int)
    
    GetPeerString() string

    GetMsgStats() *MessageStats
    
    GetLastMsg() *Message

    ResetDebugFlag()
    
    Topology() *TopologyResponse
    
    Die()
    
    Stop()
    
    Command(cmd string, args string) bool

    Multicast(data string) bool
}

func doGetAndReadResponse(name, host, path, method string, decode func(resp *http.Response) error) bool {
    var resp *http.Response
    var err error
    url := "http://"+host+path
    //fmt.Printf("Making GET to %s\n",url)
    resp, err = http.Get(url)

    if err != nil {
        fmt.Printf("%s: %s request failed %v!\n", name, method, err)
        return false
    }
    if resp.StatusCode != http.StatusOK {
        fmt.Printf("%s: %s unexpected status code %v!\n", name, method, resp.StatusCode)
        return false
    }
    defer resp.Body.Close()
    errd := decode(resp)
    
    if errd != nil {
        fmt.Printf("%s: %s decode failed %v : resp = %v\n", name, method, errd, resp)
    }
    return (errd != nil)
}

// Proxy for an exec'd process
type PeerServerProcess struct {
    name string
    host string
    addr string
    proc *exec.Cmd
}

func (p *PeerServer) Name() string {
    return p.name
}

func (p *PeerServer) Address() string {
    return p.address
}

func (psp *PeerServerProcess) Address() string {
    return psp.addr
}

func (psp *PeerServerProcess) SetDebugFlag(flag int)  {
    go func() {
        // sleep a sec to let server start up
        time.Sleep(50 * time.Millisecond)
        doGetAndReadResponse(psp.name, psp.host, fmt.Sprintf("/debug?flag=%d", flag), "SetDebugFlag", func(resp *http.Response) error {
            return nil
        })
    }()
}

func (psp *PeerServerProcess) SetFailover(failover string) bool {
    return doGetAndReadResponse(psp.name, psp.host, fmt.Sprintf("/failover?newfailover=%s", url.QueryEscape(failover)), "SetFailover", func(resp *http.Response) error {
        return nil
    })
}

func (psp *PeerServerProcess) ResetDebugFlag()  {
    doGetAndReadResponse(psp.name, psp.host, "/debug?flag=0", "ResetDebugFlag", func(resp *http.Response) error {
        return nil
    })
}

func (psp *PeerServerProcess) Name() string {
    return psp.name
}

func (psp *PeerServerProcess) GetPeerString() string {
    return fmt.Sprintf("[%s @ %s]", psp.name, psp.host)
}

func (psp *PeerServerProcess) GetMsgStats() *MessageStats {
    msg := &MessageStats{}
    doGetAndReadResponse(psp.name, psp.host, "/msgstats", "GetMsgStats", func(resp *http.Response) error {
        return json.NewDecoder(resp.Body).Decode(&msg)
    })
	return msg
}

func (psp *PeerServerProcess) GetLastMsg() *Message {
    msg := &Message{}
    doGetAndReadResponse(psp.name, psp.host, "/lastmsg", "GetLastMsg", func(resp *http.Response) error {
        return json.NewDecoder(resp.Body).Decode(&msg)
    })
	return msg
}

func (psp *PeerServerProcess) Topology() *TopologyResponse {
    msg := &TopologyResponse{}
    doGetAndReadResponse(psp.name, psp.host, "/topo", "Topology", func(resp *http.Response) error {
        return json.NewDecoder(resp.Body).Decode(&msg)
    })
	return msg
}

func (psp *PeerServerProcess) Die() {
    ret := doGetAndReadResponse(psp.name, psp.host, "/die", "Die", func(resp *http.Response) error {
        //do nothing
        return nil
    })
    if ret {
        // wait for it to die
        fmt.Printf("Waiting for proc for %s @ %s to die", psp.name, psp.host)
        psp.proc.Wait()
    }
}

func (psp *PeerServerProcess) Stop() {
    ret := doGetAndReadResponse(psp.name, psp.host, "/die", "Stop", func(resp *http.Response) error {
        //do nothing
        return nil
    })
    if ret {
        // wait for it to die
        fmt.Printf("Waiting for proc for %s @ %s to die", psp.name, psp.host)
        psp.proc.Wait()
    }
}

func (psp *PeerServerProcess) Command(cmd string, args string) bool {
    ret := doGetAndReadResponse(psp.name, psp.host, "/command?cmd="+url.QueryEscape(cmd)+"&args="+url.QueryEscape(args), "Command", func(resp *http.Response) error {
        //do nothing
        return nil
    })
    return ret
}

func (psp *PeerServerProcess) Multicast(data string) bool {
    ret := doGetAndReadResponse(psp.name, psp.host, "/update?dat="+url.QueryEscape(data), "Multicast", func(resp *http.Response) error {
        //do nothing
        return nil
    })
    return ret
}


func startPeer(portoffset int, root string, maxChildren int, inproc bool) PeerServerRef {
    addr := "127.0.0.1"
    name := fmt.Sprintf("h%02d", portoffset)
    port := BASE_PORT + portoffset
    listen := fmt.Sprintf("%s:%d", addr, port)
    cfg := &PeerServerCfg {
        name: name,
        host: addr,
        addr: "",
        port: port,
        cluster: "cluster",
        root: root,
        failover: "",
        maxChildren: maxChildren,
    }
    if inproc {
        s, err := NewPeerServer(cfg, func(msg *Message) {
        	  fmt.Printf("%s: callback %v\n", name, msg)
        	})
    	if err != nil {
    		log.Fatal(err)
    	}
    	s.ForceCloseConnections()
    	go func() {
        	if err := s.ListenAndServe(); err != nil {
        		log.Fatal(err)
        	}
    	}()
    	return PeerServerRef(s)
    }
    // else start separate process
    psp := &PeerServerProcess {
        name: name,
        host: listen,
        addr: listen,
    }
    maxc := fmt.Sprintf("%d", maxChildren)
    args := []string{"-n", name, "-p", strconv.Itoa(port), "-c", cfg.cluster, "-m", maxc, "-j", root}
    cmd := exec.Command("./grapevine", args...)
    fmt.Printf("Exec: ./grapevine %s\n", strings.Join(args, " "))

    //dump to console
    stdout, err1 := cmd.StdoutPipe()
    if err1 != nil {
        fmt.Println(err1)
    }
    stderr, err2 := cmd.StderrPipe()
    if err2 != nil {
        fmt.Println(err2)
    }
	err3 := cmd.Start()
	if err3 != nil {
	   log.Fatal("cmd failed: %v", err3)
	}
    go io.Copy(os.Stdout, stdout) 
    go io.Copy(os.Stderr, stderr) 
        
	psp.proc = cmd
    return  PeerServerRef(psp)
}

func (a *TestConsoleApp) Run() {
    maxChildren := a.maxChildren
    inproc := a.inproc

    bio := bufio.NewReader(os.Stdin)
    peers := make([]PeerServerRef, 0)
    var master PeerServerRef
    portoffset := 0
    root := "127.0.0.1:4000"
    debugFlag := 0
    nmsgs := 0

    parsePeerNo := func(caller string, args []string, mustberunning bool) int {
        peerno := -1
        var err error
        if len(args) < 1 {
            fmt.Printf("%s: Must specify peer number", caller)
            return -1
        }
        peerno, err = strconv.Atoi(args[0])
        if err != nil || peerno < 0 || peerno >= len(peers) {
            fmt.Printf("%s: Invalid peer number %s", caller, args[0])
            return -1
        }
        if mustberunning && peers[peerno] == nil {
            fmt.Printf("%s: Peer number %d not running", caller, peerno)
            return -1
        } 
        return peerno
    }

    forEachLivePeer := func(apply func(peer PeerServerRef)) {
        for i := 0; i < len(peers); i++ {
            if peers[i] == nil {
                fmt.Printf("peer #%d is dead, skipping\n", i)
            } else {
                apply(peers[i])
            }
        }
    }
    
    summarize := func(refMsg *Message) string {
        snippet := refMsg.Data
        trimlen := 24
        if len(snippet) > trimlen {
            snippet = fmt.Sprintf("%s... (%d b total)", snippet[:trimlen], len(snippet))
        }
        return fmt.Sprintf("ref #%05d from %s TTL=%02d data=%v", refMsg.MsgId, refMsg.Source, refMsg.TTL, snippet)
    }
    
    var exec func(line string) bool
    
    execCmd := func(cmd string, args []string, line string) bool {
        var err error
        switch cmd {
        case "s", "start":
            npeers := 1
            if len(args) > 0 {
                npeers, err = strconv.Atoi(args[0])
                if err != nil {
                    fmt.Printf("Invalid number %s", args[0])
                    break
                }
            }
            maxc := maxChildren
            if len(args) > 1 {
                maxc, err = strconv.Atoi(args[1])
                if err != nil {
                    fmt.Printf("Invalid maxc number %s", args[1])
                    break
                }
            }
            for i := 0; i < npeers; i++ {
                proot := root
                if portoffset == 0 {
                    proot = ""
                }
                s := startPeer(portoffset, proot, maxc, inproc)
                s.SetDebugFlag(debugFlag)
        		peers = append(peers, s)
                if portoffset == 0 {
                    master = s
                    fmt.Printf("Started master peer with %d max children\n", maxc)
                }
        		portoffset++
            }

        case "rs", "restart":
            peerno := parsePeerNo("restart", args, false)
            if peerno >= 0 {
                if peers[peerno] != nil {
                    fmt.Printf("Peer number %d still running", peerno)
                }  else {
                    proot := root
                    if peerno == 0 {
                        proot = ""
                    }
                    maxc := maxChildren
                    if len(args) > 1 {
                        maxc, err = strconv.Atoi(args[1])
                        if err != nil {
                            fmt.Printf("Invalid maxc number %s", args[1])
                            break
                        }
                    }
                    s := startPeer(peerno, proot, maxc, inproc)
                    s.SetDebugFlag(debugFlag)
                    peers[peerno] = s
                }
            }

        case "k", "kill":
            peerno := parsePeerNo("kill", args, true)
            if peerno >= 0 {
                peers[peerno].Stop()
                peers[peerno] = nil
            }
            
        case "m", "multicast":
            peerno := parsePeerNo("multicast", args, true)
            if peerno >= 0 && len(args) > 1 {
                peers[peerno].Multicast(args[1])
                nmsgs++
            } else {
                fmt.Printf("Must provide data to multicast")
            }

        case "f", "failover":
            peerno := parsePeerNo("failover", args, true)
            if peerno >= 0 && len(args) > 1 {
                peer2, e2 := strconv.Atoi(args[1])
                failover := args[1]
                if e2 == nil && peer2 < len(peers) {
                    failover = peers[peer2].Address()
                }
                fmt.Printf("Set new failover %s", failover)
                peers[peerno].SetFailover(failover)
            } else {
                fmt.Printf("Must provide host to failover")
            }

        case "mc", "maxchildren":
            if len(args) < 1 {
                fmt.Printf("Current max children = %d", maxChildren)
                break
            }
            maxc, err := strconv.Atoi(args[0])
            if err != nil {
                fmt.Printf("Invalid number %s", args[0])
                break
            }
            maxChildren = maxc

        case "sp", "spam":
            if len(args) < 1 {
                fmt.Printf("Must provide number of updates to multicast")
                break
            }
            
            sizecast := 0

            numcasts, err := strconv.Atoi(args[0])
            if err != nil {
                fmt.Printf("Invalid number %s", args[0])
                break
            }

            if len(args) > 1 {
                sizecast, err = strconv.Atoi(args[1])
                if err != nil {
                    fmt.Printf("Invalid number %s", args[1])
                    break
                }
            }
            extra := ""
            if sizecast > 0 {
                extra = strings.Repeat("x", sizecast)
            }
            for i := 0; i < numcasts; i++ {
                var s PeerServerRef
                for {
                    s = peers[rand.Intn(len(peers))]
                    if s != nil {
                        break
                    }
                }
                update := fmt.Sprintf("%v update %03d %s", s.Name(), nmsgs, extra)
                go s.Multicast(update)
                nmsgs++
            }

        case "l", "lastmsg":
            peerno := parsePeerNo("lastmsg", args, true)
            if peerno >= 0 {
                fmt.Printf("%d: latest %v\nstats%v\n", peerno, summarize(peers[peerno].GetLastMsg()), peers[peerno].GetMsgStats())
            }

        case "r", "run":
            if len(args) < 1 {
                fmt.Printf("No script specified")
                break
            }
            script := args[0]
            if !strings.HasSuffix(script, ".run") {
                script = script + ".run"
            }
            bytes, err := ioutil.ReadFile("./"+script)
            if err != nil {
                fmt.Printf("Error in reading file %s: %v", script, err)
                break
            }
            mlines := string(bytes)
            lines := strings.Split(mlines, "\n")
            for i := 0; i < len(lines); i++   {
                if exec(strings.TrimSpace(lines[i])) {
                    break
                }
                fmt.Printf("\n")
            }

        case "t", "topo":
            peerno := parsePeerNo("topo", args, true)
            if peerno >= 0 {
                fmt.Printf("%d topo: %v", peerno, peers[peerno].Topology())
            } else {
                forEachLivePeer(func(peer PeerServerRef) {
                    go func(peer PeerServerRef) {
                        fmt.Printf("%s: %v\n", peer.Name(), peer.Topology())
                    }(peer)
                })
            }

        case "d", "debug":
            flag := DEBUG_FLAG_FWD 
            if len(args) > 1 {
                flag, _ = strconv.Atoi(args[1])
            }
            peerno := parsePeerNo("debug", args, true)
            if peerno >= 0 {
                peers[peerno].SetDebugFlag(flag)
            } else {
                debugFlag = flag
                forEachLivePeer(func(peer PeerServerRef) {
                    peer.SetDebugFlag(flag)
                })
            }

        case "v", "verify":
            refStats := master.GetMsgStats()
            fmt.Printf("%v : %v\n", master.GetPeerString(), refStats)

            msgStatMap := make(map[string]int)

            forEachLivePeer(func(peer PeerServerRef) {
                msgStats := peer.GetMsgStats()
                sMsgStat := fmt.Sprintf("%3d/%d",msgStats.NumMsgs, msgStats.CRCTally)
                msgStatMap[sMsgStat] = msgStatMap[sMsgStat] + 1
                /*if msgStats.NumMsgs != refStats.NumMsgs || msgStats.CRCTally != refStats.CRCTally {
                    fmt.Printf("DISCREPANCY! %v has [%v]\n", peer.GetPeerString(), msgStats)
                }*/
            })
            if len(msgStatMap) == 1 {
                fmt.Printf("Verified stats\n")
            } else {
                fmt.Printf("Diverging stats:\n")
                for k, v := range msgStatMap {
                    fmt.Printf("%d peers have %s\n", v, k)
                }
            }

        case "v2", "verify2":
            refMsg := master.GetLastMsg()
            fmt.Printf("%v : %v\n", master.GetPeerString(), summarize(refMsg))

            forEachLivePeer(func(peer PeerServerRef) {
                lastMsg := peer.GetLastMsg()
                if lastMsg.Source != refMsg.Source || lastMsg.Data != refMsg.Data {
                    fmt.Printf("DISCREPANCY! %v has [%v]\n", peer.GetPeerString(), summarize(lastMsg))
                }
            })
            fmt.Printf("Verified last message\n")

        default:
            if strings.TrimSpace(line) != "" {
                fmt.Printf("Unknown command: %s", line)
            }

        case "q", "quit":
            return true
        }
        return false
    }

    exec = func(line string) bool {
        async := false
        if strings.HasSuffix(line, "&") {
            async = true
            line = strings.TrimSpace(line[:len(line)-1])
        }
        
        tokens := strings.SplitN(line, " ", 3)
        cmd := tokens[0]
        args := tokens[1:]
        if async {
            go execCmd(cmd, args, line)
            return false
        }
        return execCmd(cmd, args, line)
    }

    MainLoop: for {
        fmt.Printf("\ncmd: ")
        linebytes, _, _ := bio.ReadLine()
        mlines := string(linebytes)
        lines := strings.Split(mlines, ";")
        for i := 0; i < len(lines); i++   {
            if exec(strings.TrimSpace(lines[i])) {
                break MainLoop
            }
        }
    }
    // try to kill all procs cleanly
    if !inproc {
        forEachLivePeer(func(peer PeerServerRef) {
            peer.Die()
        })
    }
    log.Fatal("Quitting\n")
}
