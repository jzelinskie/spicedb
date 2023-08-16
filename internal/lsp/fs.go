package lsp

import "io/fs"

type FS struct{}

var _ fs.FS = (*FS)(nil)

func (fs *FS) Open(uri string) (fs.File, error) {
	return nil, nil
}

type File struct{}

var _ fs.File = (*File)(nil)

func (*File) Close() error { return nil }

func (*File) Read([]byte) (int, error) {
	panic("unimplemented")
}

func (*File) Stat() (fs.FileInfo, error) {
	panic("unimplemented")
}
