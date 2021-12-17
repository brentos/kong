package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/durationpb"
	pb "target/targetservice"
)

const (
	port = ":15010"
)

type server struct {
	pb.UnimplementedBouncerServer
}

func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	return &pb.HelloResponse{
		Reply: fmt.Sprintf("hello %s", in.GetGreeting()),
	}, nil
}

func (s *server) BounceIt(ctx context.Context, in *pb.BallIn) (*pb.BallOut, error) {
	w := in.GetWhen().AsTime()
	now := in.GetNow().AsTime()
	ago := now.Sub(w)

	reply := fmt.Sprintf("hello %s", in.GetMessage())
	time_message := fmt.Sprintf("%s was %v ago", w.Format(time.RFC3339Nano), ago.Truncate(time.Nanosecond))

	return &pb.BallOut{
		Reply:       reply,
		TimeMessage: time_message,
		Now:         timestamppb.New(now),
		Ago:         durationpb.New(ago),
	}, nil
}

func (s *server) GrowTail(ctx context.Context, in *pb.Body) (*pb.Body, error) {
	in.Tail.Count += 1

	return in, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterBouncerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
