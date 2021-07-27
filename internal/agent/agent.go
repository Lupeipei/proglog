package agent

import (
  "bytes"
  "crypto/tls"
  "fmt"
  "io"
  "net"
  "sync"
  "time"
  "go.uber.org/zap"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
  "github.com/hashicorp/raft"
  "github.com/soheilhy/cmux"
  // api "github.com/Lupeipei/proglog/api/v1"
  "github.com/Lupeipei/proglog/internal/auth"
  "github.com/Lupeipei/proglog/internal/discovery"
  "github.com/Lupeipei/proglog/internal/log"
  "github.com/Lupeipei/proglog/internal/server"
)

type Agent struct {
  Config

  mux cmux.CMux
  log *log.DistributedLog
  // log *log.Log
  server *grpc.Server
  membership *discovery.Membership
  // replicator *log.Replicator

  shutdown bool
  shutdowns chan struct{}
  shutdownLock sync.Mutex
}

type Config struct {
  ServerTLSConfig *tls.Config
  PeerTLSConfig   *tls.Config
  DataDir         string
  BindAddr        string
  RPCPort         int
  NodeName        string
  StartJoinAddrs  []string
  ACLModelFile    string
  ACLPolicyFile   string
  Bootstrap       bool // enable bootstraping the raft cluster
}

func (c Config) RPCAddr() (string, error) {
  host, _, err := net.SplitHostPort(c.BindAddr)
  if err != nil {
    return "", err
  }
  return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
  a := &Agent{
    Config: config,
    shutdowns: make(chan struct{}),
  }

  setup := []func() error {
    a.setupLogger,
    a.setupMux,
    a.setupLog,
    a.setupServer,
    a.setupMembership,
  }
  for _, fn := range setup {
    if err := fn(); err != nil {
      return nil, err
    }
  }
  go a.serve()
  return a , nil
}

func (a *Agent) setupLogger() error {
  logger, err := zap.NewDevelopment()
  if err != nil {
    return err
  }
  zap.ReplaceGlobals(logger)
  return nil
}

func (a *Agent) setupMux() error {
  rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
  ln, err := net.Listen("tcp", rpcAddr)
  if err != nil {
    return err
  }
  a.mux = cmux.New(ln)
  return nil
}

func (a *Agent) setupLog() error {
  // var err error
  // a.log, err = log.NewLog(
  //   a.Config.DataDir,
  //   log.Config{},
  // )
  // return err
  raftLn := a.mux.Match(func(reader io.Reader) bool {
    b := make([]byte, 1)
    if _, err := reader.Read(b); err != nil {
      return false
    }
    return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
  })
  logConfig := log.Config{}
  logConfig.Raft.StreamLayer = log.NewStreamLayer(
    raftLn,
    a.Config.ServerTLSConfig,
    a.Config.PeerTLSConfig,
  )
  logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
  logConfig.Raft.Bootstrap = a.Config.Bootstrap
  var err error
  a.log, err = log.NewDistributedLog(
    a.Config.DataDir,
    logConfig,
  )
  if err != nil {
    return err
  }
  if a.Config.Bootstrap {
    err = a.log.WaitForLeader(3 * time.Second) // election
  }
  return err
}

func (a *Agent) setupServer() error {
  authorizer := auth.New(
    a.Config.ACLModelFile,
    a.Config.ACLPolicyFile,
  )
  serverConfig := &server.Config{
    CommitLog: a.log,
    Authorizer: authorizer,
  }

  var opts []grpc.ServerOption
  if a.Config.ServerTLSConfig != nil {
    creds := credentials.NewTLS(a.Config.ServerTLSConfig)
    opts = append(opts, grpc.Creds(creds))
  }
  var err error
  a.server, err = server.NewGRPCServer(serverConfig, opts...)
  if err != nil {
    return err
  }

  // rpcAddr, err := a.Config.RPCAddr()

  // rpcAddr, err := a.RPCAddr()
  // if err != nil {
  //   return err
  // }
  //
  // ln, err := net.Listen("tcp", rpcAddr)
  // if err != nil {
  //   return err
  // }
  //
  // go func() {
  //   if err := a.server.Serve(ln); err != nil {
  //     _ = a.Shutdown()
  //   }
  // }()

  grpcLn := a.mux.Match(cmux.Any())
  go func() {
    if err := a.server.Serve(grpcLn); err != nil {
      _ = a.Shutdown()
    }
  }()
  return err
}

func (a *Agent) setupMembership() error {
  rpcAddr, err := a.Config.RPCAddr()
  if err != nil {
    return err
  }
  // var opts []grpc.DialOption
  // if a.Config.PeerTLSConfig != nil {
  //   opts = append(opts, grpc.WithTransportCredentials(
  //     credentials.NewTLS(a.Config.PeerTLSConfig),
  //     ),
  //   )
  // }
  // conn, err := grpc.Dial(rpcAddr, opts...)
  // if err != nil {
  //   return err
  // }
  // client := api.NewLogClient(conn)
  // a.replicator = &log.Replicator{
  //   DialOptions: opts,
  //   LocalServer: client,
  // }
  a.membership, err = discovery.New(
    a.log,
    discovery.Config{
      NodeName: a.Config.NodeName,
      BindAddr: a.Config.BindAddr,
      Tags: map[string]string{
        "rpc_addr": rpcAddr,
      },
      StartJoinAddrs: a.Config.StartJoinAddrs,
    },
  )
  return err
}

func (a *Agent) Shutdown() error {
  a.shutdownLock.Lock()
  defer a.shutdownLock.Unlock()
  if a.shutdown {
    return nil
  }
  a.shutdown = true
  close(a.shutdowns)
  shutdown :=[]func() error{
    a.membership.Leave,
    // a.replicator.Close,
    func() error {
      a.server.GracefulStop()
      return nil
    },
    a.log.Close,
  }
  for _, fn := range shutdown {
    if err := fn(); err != nil {
      return err
    }
  }
  return nil
}

func (a *Agent) serve() error {
  if err := a.mux.Serve(); err != nil {
    _ = a.Shutdown()
    return err
  }
  return nil
}
