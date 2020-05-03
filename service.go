package main

import (
	context "context"
	"log"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type Service struct {
	ctx                   context.Context
	aclData               map[string][]*regexp.Regexp
	loggingMtx            *sync.RWMutex
	logConsumers          map[string]chan *Event
	statisticMutex        *sync.RWMutex
	methodInvokeStatistic map[string]uint64
	userStatistic         map[string]uint64
}

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) error {
	service := Service{}
	aclPaths, err := parseAclDataToRegextp(ACLData)

	if err != nil {
		return err
	}

	service.aclData = aclPaths
	service.ctx = ctx
	service.loggingMtx = &sync.RWMutex{}
	service.statisticMutex = &sync.RWMutex{}

	service.logConsumers = make(map[string]chan *Event)
	service.methodInvokeStatistic = make(map[string]uint64)
	service.userStatistic = make(map[string]uint64)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalln("cant listet port", err)
	}
	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(service.authStremInterceptor, service.loggingStreamInterceptor)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(service.authInterceptor, service.loggingInterceptor)),
	)

	RegisterBizServer(server, &service)
	RegisterAdminServer(server, &service)
	go func() {
		<-ctx.Done()
		server.Stop()
	}()
	go func() { server.Serve(lis) }()
	return nil
}

func (b *Service) loggingStreamInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	peer, _ := peer.FromContext(ctx)
	consumer := md.Get("consumer")[0]

	b.statisticMutex.Lock()
	// Collect statistic data
	b.methodInvokeStatistic[info.FullMethod]++
	b.userStatistic[consumer]++
	b.statisticMutex.Unlock()

	b.loggingMtx.RLock()
	for _, logChan := range b.logConsumers {
		logChan <- &Event{
			Timestamp: time.Now().Unix(),
			Consumer:  consumer,
			Method:    info.FullMethod,
			Host:      peer.Addr.String(),
		}
	}
	b.loggingMtx.RUnlock()

	return handler(srv, ss)
}

func (b *Service) loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	peer, _ := peer.FromContext(ctx)
	consumer := md.Get("consumer")[0]

	b.statisticMutex.Lock()
	// Collect statistic data
	b.methodInvokeStatistic[info.FullMethod]++
	b.userStatistic[consumer]++
	b.statisticMutex.Unlock()

	b.loggingMtx.RLock()
	// Collect statistic data
	for _, logChan := range b.logConsumers {
		logChan <- &Event{
			Timestamp: time.Now().Unix(),
			Consumer:  consumer,
			Method:    info.FullMethod,
			Host:      peer.Addr.String(),
		}
	}
	b.loggingMtx.RUnlock()
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
	logginChan := make(chan *Event)
	consumerID := uuid.New().String()

	b.loggingMtx.Lock()
	b.logConsumers[consumerID] = logginChan
	b.loggingMtx.Unlock()

	for {
		select {
		case logVal := <-logginChan:
			inStream.Send(logVal)
		case <-b.ctx.Done():
			b.loggingMtx.Lock()
			delete(b.logConsumers, consumerID)
			b.loggingMtx.Unlock()
			return nil
		default:
			continue
		}
	}
}

func (b *Service) Statistics(interval *StatInterval, inStream Admin_StatisticsServer) error {
	intervalTicker := time.NewTicker(time.Duration(interval.IntervalSeconds) * time.Second)
	for {
		select {
		case <-intervalTicker.C:
			b.statisticMutex.RLock()
			inStream.Send(&Stat{
				Timestamp:  time.Now().Unix(),
				ByMethod:   b.methodInvokeStatistic,
				ByConsumer: b.userStatistic,
			})
			b.statisticMutex.RUnlock()
		case <-b.ctx.Done():
			return nil
		default:
			continue
		}
	}
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
