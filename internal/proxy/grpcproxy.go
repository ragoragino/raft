package proxy

import (
	"io"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var (
	clientStreamDescForProxying = &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
)

type Payload struct {
	payload []byte
}

type RoutingRulesHandler func(md metadata.MD) (string, error)

type Processor struct {
	routingRulesHandler RoutingRulesHandler
	server              *grpc.Server
}

func NewProcessor(routingRulesHandler RoutingRulesHandler) *Processor {
	p := &Processor{
		routingRulesHandler: routingRulesHandler,
	}

	server := grpc.NewServer(
		grpc.CustomCodec(NewProxyCodec()),
		grpc.UnknownServiceHandler(p.Handle))

	p.server = server

	return p
}

func (p *Processor) Serve(lis net.Listener) error {
	return p.server.Serve(lis)
}

func (p *Processor) GracefulStop() {
	p.server.GracefulStop()
}

func (p *Processor) Handle(srv interface{}, serverStream grpc.ServerStream) error {
	ctx := serverStream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return grpc.Errorf(codes.InvalidArgument, "could not obtain metadata from context")
	}

	address, err := p.routingRulesHandler(md)
	if err != nil {
		return grpc.Errorf(codes.Unavailable, err.Error())
	}

	conn, err := grpc.DialContext(ctx, address, grpc.WithCodec(NewProxyCodec()), grpc.WithInsecure())
	if err != nil {
		return grpc.Errorf(codes.Unavailable, err.Error())
	}

	clientCtx, clientCancel := context.WithCancel(ctx)

	fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return grpc.Errorf(codes.Internal, "method name not found")
	}

	clientStream, err := grpc.NewClientStream(clientCtx, clientStreamDescForProxying, conn, fullMethodName)
	if err != nil {
		return grpc.Errorf(codes.Internal, err.Error())
	}

	srverErrChan := p.forwardServerToClient(serverStream, clientStream)
	clientErrChan := p.forwardClientToServer(clientStream, serverStream)

	for i := 0; i < 2; i++ {
		select {
		case serverErr := <-srverErrChan:
			if serverErr == io.EOF {
				// this is the happy case where the sender has encountered io.EOF, and won't be sending anymore./
				// the clientStream>serverStream may continue pumping though.
				clientStream.CloseSend()
				break
			} else {
				// however, we may have gotten a receive error (stream disconnected, a read error etc) in which case we need
				// to cancel the clientStream to the backend, let all of its goroutines be freed up by the CancelFunc and
				// exit with an error to the stack
				clientCancel()
				return grpc.Errorf(codes.Internal, "failed proxying s2c: %v", serverErr)
			}
		case clientErr := <-clientErrChan:
			// This happens when the clientStream has nothing else to offer (io.EOF), returned a gRPC error. In those two
			// cases we may have received Trailers as part of the call. In case of other errors (stream closed) the trailers
			// will be nil.
			serverStream.SetTrailer(clientStream.Trailer())
			// c2sErr will contain RPC error from client code. If not io.EOF return the RPC error as server stream error.
			if clientErr != io.EOF {
				return clientErr
			}
			return nil
		}
	}
	return grpc.Errorf(codes.Internal, "gRPC proxying should never reach this stage.")
}

// This function is taken from: https://github.com/mwitkow/grpc-proxy
func (p *Processor) forwardClientToServer(src grpc.ClientStream, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)
	go func() {
		f := &Payload{}
		for i := 0; ; i++ {
			if err := src.RecvMsg(f); err != nil {
				ret <- err // this can be io.EOF which is happy case
				break
			}
			if i == 0 {
				// This is a bit of a hack, but client to server headers are only readable after first client msg is
				// received but must be written to server stream before the first msg is flushed.
				// This is the only place to do it nicely.
				md, err := src.Header()
				if err != nil {
					ret <- err
					break
				}
				if err := dst.SendHeader(md); err != nil {
					ret <- err
					break
				}
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				break
			}
		}
	}()
	return ret
}

// This is taken from: https://github.com/mwitkow/grpc-proxy
func (p *Processor) forwardServerToClient(src grpc.ServerStream, dst grpc.ClientStream) chan error {
	ret := make(chan error, 1)
	go func() {
		f := &Payload{}
		for i := 0; ; i++ {
			if err := src.RecvMsg(f); err != nil {
				ret <- err // this can be io.EOF which is happy case
				break
			}
			if err := dst.SendMsg(f); err != nil {
				ret <- err
				break
			}
		}
	}()
	return ret
}
