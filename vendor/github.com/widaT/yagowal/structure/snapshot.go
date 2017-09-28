package structure

import (
	"bytes"
	"encoding/binary"
)

type SnapshotFile struct {
	Crc              uint32
	Data             []byte
}

func (s *SnapshotFile) Marshal() ( []byte,  error) {
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,s.Crc)
	buf.Write(s.Data)
	return buf.Bytes(),nil
}
func (s *SnapshotFile) Unmarshal(b []byte) error {
	bf := bytes.NewReader(b)
	binary.Read(bf,binary.BigEndian,&s.Crc)
	s.Data = make([]byte,len(b[4:]))
	bf.Read(s.Data)
	return nil
}
func (s *SnapshotFile) MarshalTo(bf []byte) (int, error) {
	binary.BigEndian.PutUint32(bf[:4],s.Crc)
	i := 4
	i +=copy(bf[4:],s.Data)
	return i,nil
}