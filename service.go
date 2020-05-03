package main

import (
	context "context"
	fmt "fmt"
	"log"
	"net"
	"regexp"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type Service struct {
	aclData    map[string][]*regexp.Regexp
	loggingMtx *sync.RWMutex
	logList    []*Event
}

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {
	service := Service{}
	logMtx := sync.RWMutex{}
	aclPaths, err := parseAclDataToRegextp(ACLData)

	if err != nil {
		return err
	}

	service.aclData = aclPaths
	service.loggingMtx = &logMtx

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalln("cant listet port", err)
	}
	server := grpc.NewServer(
		grpc.StreamInterceptor(service.authStremInterceptor),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(service.authInterceptor, service.loggingInterceptor)),
	)

	RegisterBizServer(server, &service)
	RegisterAdminServer(server, &service)
	fmt.Println("starting server at ", listenAddr)
	go func() {
		<-ctx.Done()
		server.Stop()
	}()
	go func() { server.Serve(lis) }()
	return nil
}

func (b *Service) loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	peer, _ := peer.FromContext(ctx)
	b.loggingMtx.Lock()
	b.logList = append(b.logList, &Event{
		Timestamp: time.Now().Unix(),
		Consumer:  md.Get("consumer")[0],
		Method:    info.FullMethod,
		Host:      peer.Addr.String(),
	})
	b.loggingMtx.Unlock()
	reply, err := handler(ctx, req)
	return reply, err
}

func (b *Service) authStremInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	err := performAuth(md, b.aclData, info.FullMethod)
	if err != nil {
		return err
	}
	return handler(srv, ss)
}

func (b *Service) authInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	err := performAuth(md, b.aclData, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (b *Service) Logging(in *Nothing, inStream Admin_LoggingServer) error {
	return nil
}

func (b *Service) Statistics(interval *StatInterval, inStream Admin_StatisticsServer) error {
	return nil
}

func (b *Service) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{Dummy: true}, nil
}

func (b *Service) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{Dummy: true}, nil
}

func (b *Service) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{Dummy: true}, nil
}
