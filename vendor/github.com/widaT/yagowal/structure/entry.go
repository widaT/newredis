package structure

import (
	"bytes"
	"encoding/binary"
)

type Entry struct {
	Index            uint64
	Type             uint32
	Data             []byte
}

type SnapshotRecord struct {
	Index            uint64
	Data             []byte
}

func (s *Entry) Marshal() ( []byte,  error) {
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,s.Index)
	binary.Write(buf,binary.BigEndian,s.Type)
	buf.Write(s.Data)
	return buf.Bytes(),nil
}
func (s *Entry) Unmarshal(b []byte) error {
	bf := bytes.NewReader(b)

	binary.Read(bf,binary.BigEndian,&s.Index)

	binary.Read(bf,binary.BigEndian,&s.Type)
	s.Data = make([]byte,len(b[12:]))
	bf.Read(s.Data)
	return nil
}
func (s *Entry) MarshalTo(bf []byte) (int, error) {
	binary.BigEndian.PutUint64(bf[:8],s.Index)
	binary.BigEndian.PutUint32(bf[8:12],s.Type)
	i := 12
	i +=copy(bf[12:],s.Data)
	return i,nil
}

func (s *SnapshotRecord) Marshal() ( []byte,  error) {
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,s.Index)
	buf.Write(s.Data)
	return buf.Bytes(),nil
}
func (s *SnapshotRecord) Unmarshal(b []byte) error {
	bf := bytes.NewReader(b)
	binary.Read(bf,binary.BigEndian,&s.Index)
	s.Data = make([]byte,len(b[8:]))
	bf.Read(s.Data)
	return nil
}
func (s *SnapshotRecord) MarshalTo(bf []byte) (int, error) {
	binary.BigEndian.PutUint64(bf[:8],s.Index)
	i := 8
	i +=copy(bf[8:],s.Data)
	return i,nil
}