package serialize

import (
	"github.com/widaT/gowal/pkg/serialize"
	"github.com/widaT/yagowal/structure"
)


type Marshaler interface {
	Marshal() (data []byte, err error)
}

type Unmarshaler interface {
	Unmarshal(data []byte) error
}

func MustMarshal(m Marshaler) []byte {
	d, err := m.Marshal()
	if err != nil {
		//plog.Panicf("marshal should never fail (%v)", err)
	}
	return d
}

func MustUnmarshal(um Unmarshaler, data []byte) {
	if err := um.Unmarshal(data); err != nil {
		//plog.Panicf("unmarshal should never fail (%v)", err)
	}
}
func MustUnmarshalEntry(d []byte) structure.Entry {
	var e structure.Entry
	serialize.MustUnmarshal(&e, d)
	return e
}