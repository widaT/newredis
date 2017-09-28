package structure

import (
	"bytes"
	"encoding/binary"
)

type Record struct {
	Type             uint64
	Data             []byte
}

func (r *Record) Reset()                    { *r = Record{} }
func (r *Record) Marshal() ( []byte,  error) {
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,r.Type)
	buf.Write(r.Data)
	return buf.Bytes(),nil
}
func (r *Record) Unmarshal(b []byte) error {
	bf := bytes.NewReader(b)
	binary.Read(bf,binary.BigEndian,&r.Type)
	r.Data = make([]byte,len(b[8:]))
	bf.Read(r.Data)
	return nil
}

func (r *Record) Size() (n int) {
	return 8 + len(r.Data)
}

type Snapshot struct {
	Index            uint64
}

func (s *Snapshot) Marshal() ( []byte,  error) {
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,s.Index)
	return buf.Bytes(),nil
}
func (s *Snapshot) Unmarshal(b []byte) error {
	bf := bytes.NewReader(b)
	binary.Read(bf,binary.BigEndian,&s.Index)
	return nil
}
func (s *Snapshot) MarshalTo(bf []byte) (int, error) {
	binary.BigEndian.PutUint64(bf[:8],s.Index)
	return 8,nil
}
