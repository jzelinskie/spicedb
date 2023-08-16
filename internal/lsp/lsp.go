// Package lsp implements the Language Server Protocol for SpiceDB schema
// development.
package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"regexp"
	"strings"

	"github.com/jzelinskie/persistent"
	"github.com/jzelinskie/stringz"
	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/logging"
)

func InvalidParams(err error) *jsonrpc2.Error {
	return &jsonrpc2.Error{
		Code:    jsonrpc2.CodeInvalidParams,
		Message: err.Error(),
	}
}

type Server struct {
	files *persistent.Map[lsp.DocumentURI, string]
}

func NewServer() *Server {
	return &Server{
		files: persistent.NewMap[lsp.DocumentURI, string](func(x, y lsp.DocumentURI) bool {
			return string(x) < string(y)
		}),
	}
}

func (s *Server) refreshFile(uri lsp.DocumentURI) error {
	contents, err := os.ReadFile(strings.TrimPrefix(string(uri), "file://"))
	if err != nil {
		return err
	}
	s.files.Set(uri, string(contents), nil)
	return nil
}

func (s *Server) withFiles(fn func(*persistent.Map[lsp.DocumentURI, string]) error) error {
	clone := s.files.Clone()
	defer clone.Destroy()
	return fn(clone)
}

func unmarshalParams[T any](r *jsonrpc2.Request) (T, error) {
	var params T
	if r.Params == nil {
		return params, InvalidParams(errors.New("params not provided"))
	}
	if err := json.Unmarshal(*r.Params, &params); err != nil {
		return params, InvalidParams(err)
	}
	return params, nil
}

func (s *Server) textDocDidChange(ctx context.Context, r *jsonrpc2.Request) (any, error) {
	params, err := unmarshalParams[lsp.DidChangeTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	return nil, s.refreshFile(params.TextDocument.URI)
}

func (s *Server) textDocDidOpen(ctx context.Context, r *jsonrpc2.Request) (any, error) {
	params, err := unmarshalParams[lsp.DidOpenTextDocumentParams](r)
	if err != nil {
		return nil, err
	}

	return nil, s.refreshFile(params.TextDocument.URI)
}

func (s *Server) textDocCompletion(ctx context.Context, r *jsonrpc2.Request) ([]lsp.CompletionItem, error) {
	params, err := unmarshalParams[lsp.CompletionParams](r)
	if err != nil {
		return nil, err
	}

	var results []lsp.CompletionItem
	err = s.withFiles(func(files *persistent.Map[lsp.DocumentURI, string]) error {
		file, ok := files.Get(params.TextDocument.URI)
		if !ok {
			return &jsonrpc2.Error{Code: jsonrpc2.CodeInternalError, Message: "file not found"}
		}

		r := regexp.MustCompile("[^\\s]+")
		for _, id := range stringz.Dedup(r.FindAllString(file, 10)) {
			results = append(results, lsp.CompletionItem{
				Label:      id,
				InsertText: id,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Server) Handle(ctx context.Context, conn *jsonrpc2.Conn, r *jsonrpc2.Request) {
	jsonrpc2.HandlerWithError(s.handle).Handle(ctx, conn, r)
}

func (s *Server) initialize(ctx context.Context, r *jsonrpc2.Request) (any, error) {
	if r.Params == nil {
		return nil, InvalidParams(errors.New("params not provided"))
	}

	syncKind := lsp.TDSKFull
	return lsp.InitializeResult{
		Capabilities: lsp.ServerCapabilities{
			TextDocumentSync:   &lsp.TextDocumentSyncOptionsOrKind{Kind: &syncKind},
			CompletionProvider: &lsp.CompletionOptions{TriggerCharacters: []string{"."}},
		},
	}, nil
}

func (s *Server) shutdown() error {
	return nil
}

func (s *Server) handle(ctx context.Context, conn *jsonrpc2.Conn, r *jsonrpc2.Request) (any, error) {
	logging.Ctx(ctx).Trace().
		Stringer("id", r.ID).
		Str("method", r.Method).
		Msg("processing LSP request")

	switch r.Method {
	case "initialize":
		return s.initialize(ctx, r)
	case "shutdown":
		return nil, s.shutdown()
	case "exit":
		return nil, conn.Close()
	case "textDocument/completion":
		return s.textDocCompletion(ctx, r)
	case "textDocument/didOpen":
		return s.textDocDidOpen(ctx, r)
	case "textDocument/didChange":
		return s.textDocDidChange(ctx, r)
	default:
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound}
	}
}

type stdrwc struct{}

var _ io.ReadWriteCloser = (*stdrwc)(nil)

func (stdrwc) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdrwc) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdrwc) Close() error {
	if err := os.Stdin.Close(); err != nil {
		return err
	}
	return os.Stdout.Close()
}

func (s *Server) listenStdin(ctx context.Context) error {
	var connOpts []jsonrpc2.ConnOpt
	stream := jsonrpc2.NewBufferedStream(stdrwc{}, jsonrpc2.VSCodeObjectCodec{})
	conn := jsonrpc2.NewConn(ctx, stream, jsonrpc2.AsyncHandler(s), connOpts...)
	defer conn.Close()

	select {
	case <-ctx.Done():
	case <-conn.DisconnectNotify():
	}
	return nil
}

func (s *Server) listenTCP(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	var g errgroup.Group

serving:
	for {
		select {
		case <-ctx.Done():
			break serving
		default:
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			g.Go(func() error {
				stream := jsonrpc2.NewBufferedStream(conn, jsonrpc2.VSCodeObjectCodec{})
				jconn := jsonrpc2.NewConn(ctx, stream, s)
				defer jconn.Close()
				<-jconn.DisconnectNotify()
				return nil
			})
		}
	}

	return g.Wait()
}

// Run binds to the provided address and concurrently serves Language Server
// Protocol requests.
func (s *Server) Run(ctx context.Context, addr string) error {
	if addr == "-" {
		return s.listenStdin(ctx)
	}
	return s.listenTCP(ctx, addr)
}
