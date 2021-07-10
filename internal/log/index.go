package log

import (
  "io"
  "os"
  "github.com/tysonmote/gommap"
)

var (
  offWidth uint64 = 4
  posWidth uint64 = 8
  entWidth = offWidth + posWidth
)

type index struct {
  file *os.File
  mmap gommap.MMap
  size uint64 // the size of the index, and where to write the next entry appended to the index
}

func newIndex(f *os.File, c Config)(*index, error) {
  idx := &index{ file: f }
  fi, err := os.Stat(f.Name())
  if err != nil {
    return nil, err
  }

  idx.size = uint64(fi.Size())
  if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
    return nil, err
  }

  if idx.mmap, err = gommap.Map(idx.file.fd(), gommap.PORT_READ|gommap.PORT_WRITE, gommap.MAP_SHARED); err != nil {
    return nil, err
  }
  return idx, nil
}

func (i *index) Close() error {
  if err := i.mmap.Sync(gommap.ms_sync); err != nil {
    return err
  }

  if err := i.file.Sync(); err != nil {
    return err
  }

  if err := i.file.Truncate(int64(i.size)); err != nil {
    return err
  }
  return i.file.Close()
}

// 参数 in 是 相对这个index的 segment 的 base offset 的 offset
// out 对应 offset, pos 对应 pos, 通过 mmap 映射到内存中的值
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
  if i.size == 0 {
    return 0, 0, io.EOF
  }
  if in == -1 {
    out = uint32((i.size / entWidth) - 1)
  } else {
    out = uint32(in)
  }
  pos = uint64(out) * entWidth
  if i.size < pos + entWidth {
    return 0, 0, io.EOF
  }
  out = enc.Uint32(i.mmap[pos : pos + offWidth])
  pos = enc.Uint64(i.mmap[pos + offWidth : pos + entWidth])
  return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
  if uint64(len(i.mmap)) < i.size + entWidth {
    return io.EOF
  }
  enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
  enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
  i.size += uint64(entWidth)
  return nil
}

func (i *index) Name() string {
  return i.file.Name()
}
