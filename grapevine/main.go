package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

/**
 * @author kkandeka
 */
 
type SampleApp interface {
    Run()
}
 
func main() {
	var inproc bool
	var port int
	var maxChildren int
	//var listen string
	var cluster, host, root, failover, name, app string
	
	DEFAULT_NAME := "h00"
	DEFAULT_HOST := "127.0.0.1"
	DEFAULT_PORT := 4000
	//DEFAULT_HOSTPORT := DEFAULT_HOST+":"+DEFAULT_PORT
	
	flag.IntVar(&maxChildren, "m", 5, "Max children per node")
	flag.StringVar(&cluster, "c", "", "Cluster IDx")
	flag.StringVar(&name, "n", DEFAULT_NAME, "Node ID")
	//flag.StringVar(&listen, "l", DEFAULT_HOSTPORT, "Address/port to listen on")
	flag.StringVar(&host, "i", DEFAULT_HOST, "IP address to listen on")
	flag.IntVar(&port, "p", DEFAULT_PORT, "Port to listen on")
	flag.StringVar(&failover, "f", "", "Failover node to join if root node goes away")
	flag.StringVar(&root, "j", "", "Root node to join at.")
	flag.StringVar(&app, "app", "default", "Sample app to run.")
	
	flag.BoolVar(&inproc, "inproc", false, "Start test peers in the same process rather than via exec.Command.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s [options]`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Kill orphans
	go func() {
		for {
			time.Sleep(2 * time.Second)
			if os.Getppid() == 1 {
				log.Fatal("Parent process exited; terminating")
			}
		}
	}()

    /*if name == DEFAULT_NAME && listen != DEFAULT_HOST {
        //somebody didn't bother to provide a node name -- assign it to be the same as the listen address
        name = fmt.Sprintf("%s:%d", host, port)
    }*/
    if cluster == "" {
        cluster = root
    }

    cfg := &PeerServerCfg {
        name: name,
        host: host,
        addr: "",
        port: port,
        cluster: cluster,
        root: root,
        failover: failover,
        maxChildren: maxChildren,
    }

	// Create the appropriate app
	var sampleApp SampleApp
	switch app {
	case "chat":
	   sampleApp = SampleApp(NewChatSampleApp(cfg))

	case "default":
	   sampleApp = SampleApp(NewDefaultSampleApp(cfg))

	case "test":
	   sampleApp = SampleApp(NewTestConsoleApp(maxChildren, inproc))

    default:
        log.Fatal(fmt.Sprintf("Unknown sample app %s", app))
	}

	sampleApp.Run()

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
}
