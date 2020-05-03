package main

import (
	context "context"
	"encoding/json"
	fmt "fmt"
	"log"
	"net"
	"regexp"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type Service struct {
	listenAddress string
	aclData       map[string][]*regexp.Regexp
	loggingMtx    *sync.RWMutex
	logList       []*Event
}

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {
	service := Service{}
	var aclData map[string][]string
	err := json.Unmarshal([]byte(ACLData), &aclData)

	if err != nil {
		return err
	}
	aclPaths := make(map[string][]*regexp.Regexp, len(aclData))

	for key, pathList := range aclData {
		regexPathList := make([]*regexp.Regexp, len(pathList))
		for i, path := range pathList {
			regexPathList[i] = regexp.MustCompile(path)
		}
		aclPaths[key] = regexPathList
	}

	logMtx := sync.RWMutex{}

	service.listenAddress = listenAddr
	service.aclData = aclPaths
	service.loggingMtx = &logMtx

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalln("cant listet port", err)
	}
	server := grpc.NewServer(
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
	b.loggingMtx.Lock()
	b.logList = append(b.logList, &Event{
		Timestamp: time.Now().Unix(),
		Consumer:  md.Get("consumer")[0],
		Method:    info.FullMethod,
		Host:      b.listenAddress,
	})
	b.loggingMtx.Unlock()
	reply, err := handler(ctx, req)
	return reply, err
}

func (b *Service) authInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	md, _ := metadata.FromIncomingContext(ctx)
	consumer := md.Get("consumer")
	if len(consumer) < 1 {
		return nil, grpc.Errorf(codes.Unauthenticated, "no consumer info provided")
	}
	paths, ok := b.aclData[consumer[0]]
	if !ok {
		return nil, grpc.Errorf(codes.Unauthenticated, "unknown consumer")
	}

	// validate path permission
	var isDenied bool
	log.Println("Consumer is ", consumer[0])
	log.Println("Paths is ", paths)
	log.Println("Full method info is: ", info.FullMethod)
	for _, path := range paths {
		if path.MatchString(info.FullMethod) {
			isDenied = true
			break
		}
	}
	if isDenied {
		return handler(ctx, req)
	}
	return nil, grpc.Errorf(codes.Unauthenticated, "access to path denied")
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
