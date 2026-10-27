// Package rpc contains the example's protobuf Users service without requiring protoc at runtime.
// Production applications should use their own generated service clients and servers.
package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// getMethod identifies the unary Users method with StringValue request and response messages.
const getMethod = "/soroi.example.Users/Get"

// userServer is the server capability required by the unary gRPC service descriptor.
type userServer interface {
	// Get resolves an identifier to its public display name.
	Get(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}

// getFunc adapts the example's source callback to its unary service contract.
type getFunc func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)

// Get invokes the injected callback using the incoming RPC context.
func (f getFunc) Get(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return f(ctx, request)
}

// Register installs a Users/Get unary method using standard protobuf wrapper messages for IDs and names.
func Register(
	server *grpc.Server,
	load func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error),
) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "soroi.example.Users",
		HandlerType: (*userServer)(nil),
		Methods:     []grpc.MethodDesc{{MethodName: "Get", Handler: handleGet}},
	}, getFunc(load))
}

// Get calls the unary Users method; replacing it with a generated business client leaves soroi unchanged.
func Get(ctx context.Context, connection grpc.ClientConnInterface, id string) (string, error) {
	response := &wrapperspb.StringValue{}
	err := connection.Invoke(ctx, getMethod, wrapperspb.String(id), response)
	return response.Value, err
}

// handleGet decodes protobuf input and preserves normal gRPC interceptor behavior.
func handleGet(
	server any,
	ctx context.Context,
	decode func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	request := &wrapperspb.StringValue{}
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return server.(userServer).Get(ctx, request)
	}
	return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: server, FullMethod: getMethod},
		func(ctx context.Context, request any) (any, error) {
			return server.(userServer).Get(ctx, request.(*wrapperspb.StringValue))
		})
}
