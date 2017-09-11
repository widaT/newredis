package newredis

import (
	"sync"
	"encoding/gob"
	"bytes"
	"github.com/widaT/newredis/structure"
	"fmt"
	"strings"
	"errors"
	"strconv"
)

type (
	HashValue   map[string][]byte
	HashInt     map[string]int
	HashHash    map[string]HashValue
	HashHashInt map[string]HashInt
	HashBrStack map[string]*structure.List
	HashSkipList map[string]*structure.SkipList
	HashSet     map[string]*structure.Sset
	HashList    map[string][][]byte
)

type Memdb struct {
	Values  HashValue
	Hvalues HashHash
	dlList HashBrStack
	HSet HashSet
	HList HashList
	HSortSet HashHashInt
	skiplist HashSkipList
	rwmu sync.RWMutex
	recovebool bool
	s *Server
}

func NewMemdb(s *Server) *Memdb {
	db := &Memdb{
		Values:   make(HashValue),
		dlList:  make(HashBrStack),
		HSet    :  make(HashSet),
		HSortSet    :  make(HashHashInt),
		HList    :  make(HashList),
		Hvalues :make(HashHash),
		skiplist : make(HashSkipList),
		s:s,
	}
	return db
}

type Opt struct {
	Method string
	Key  string
	Args   [][]byte
}

func (o *Opt)String() string  {
	return  o.Method + o.Key
}

func (m *Memdb) getSnapshot()  ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	List  := m.dlList
	m.HList = make(HashList)
	for key,v:= range List {
		m.HList[key] = v.Values()
	}
	enc.Encode(*m)
	m.HList = nil
	return b.Bytes(),nil
}

func (m *Memdb)  recoverFromSnapshot(snapshot []byte) error {
	var db Memdb
	buf := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&db); err != nil {
		return err
	}
	db.dlList = make(HashBrStack)
	for key,v := range db.HList {
		if _,found := db.dlList[key];!found {
			db.dlList[key] = structure.NewList()
		}
		db.dlList[key].Add(v...)
	}
	db.HList = nil

	db.skiplist = make(HashSkipList)
	//重新初始化skiplist
	for key,val := range db.HSortSet {
		intmap := structure.NewIntMap()
		for k,v := range val {
			intmap.Set(v,k)
		}
		db.skiplist[key] = intmap
	}
	m = &db
	return nil
}

//list operation
func (m *Memdb) Rpush(values ...[]byte) (int, error) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	key := string(values[0])
	if _, exists := m.dlList[key]; !exists {
		m.dlList[key] =structure.NewList()
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method:"rpush",Args:values})
		if err != nil {
			return 0,err
		}
	}
	n := m.dlList[key].Rpush(values[1:]...)
	return n, nil
}

func (m *Memdb) Lrange(key string, start, stop int) ([][]byte, error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if _, exists := m.dlList[key]; !exists {
		m.dlList[key] = structure.NewList()
	}

	if start < 0 {
		if start = m.dlList[key].Size() + start; start < 0 {
			start = 0
		}
	}

	var ret [][]byte
	if stop < 0 {
		stop =  m.dlList[key].Size() + stop
		if stop <0 {
			return nil,nil
		}
	}
	var iter = m.dlList[key].Seek(start)
	if iter != nil {
		ret = append(ret, iter.Value())
	}
	for iter.Next(){
		if iter.Key() <= stop {
			ret = append(ret, iter.Value())
		}else {
			break
		}
	}
	iter.Close()
	return ret, nil
}

func (m *Memdb) Lindex(key string, index int) ([]byte, error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	if _, exists := m.dlList[key]; !exists {
		m.dlList[key] =  structure.NewList()
	}
	ret,_ := m.dlList[key].Get(index)
	return ret, nil
}

func (m *Memdb) Lpush(values ...[]byte) (int, error) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	key := string(values[0])
	if _, exists := m.dlList[key]; !exists {
		m.dlList[key] = structure.NewList()
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "lpush", Args: values})
		if err != nil {
			return 0, err
		}
	}
	num := m.dlList[key].Lpush(values[1:]...)
	return num, nil
}


func (m *Memdb)Lpop(key string) ([]byte,error) {

	if m.dlList == nil{
		return nil, nil
	}
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if _,found := m.dlList[key];!found{
		return nil,nil
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "lpop", Key: key})
		if err != nil {
			return nil, err
		}
	}
	return m.dlList[key].Lpop(),nil
}

func (m *Memdb)Rpop(key string) ([]byte,error) {
	if m.dlList == nil{
		return nil, nil
	}
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if _,found := m.dlList[key];!found{
		return nil,nil
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "lpop", Key: key})
		if err != nil {
			return nil, err
		}
	}
	return m.dlList[key].Rpop(),nil
}

//set operation
func (m *Memdb) Sadd (key string, values ...[]byte) (int ,error){
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if _, exists := m.HSet[key]; !exists {
		m.HSet[key] = structure.NewSset(key)
	}

	if !m.recovebool {
		bytes := make([][]byte, 0)
		for _, v := range values {
			bytes = append(bytes, []byte(v))
		}
		err := m.s.w.save(&Opt{Method: "sadd", Key: key, Args: bytes})
		if err != nil {
			return 0, err
		}
	}

	count := 0
	for _,value :=range values {
		count =count + m.HSet[key].Add(string(value))
	}
	return count,nil
}


func (m *Memdb) Scard (key string)( int,error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if _, exists := m.HSet[key]; !exists {
		return 0,nil
	}
	return m.HSet[key].Len(),nil
}

func (m *Memdb)Spop(key string)( []byte,error)  {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if _, exists := m.HSet[key]; !exists {
		return nil,nil
	}

	if m.HSet[key].Len() == 0 {
		return nil,nil
	}
	v := m.HSet[key].RandomKey()
	if !m.recovebool {
		m.s.w.save(&Opt{Method:"spop",Key:key,Args:[][]byte{[]byte(v)}})
	}
	m.HSet[key].Del(v)
	return []byte(v),nil
}

func (m * Memdb)spop(key string,k []byte)  {
	if _, exists := m.HSet[key]; !exists {
		return
	}
	m.HSet[key].Del(string(k))
}


func (m *Memdb) Smembers (key string)  ([][]byte,error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	if _, exists := m.HSet[key]; !exists {
		return nil,nil
	}

	return *m.HSet[key].Members(),nil
}



//hash set
func (m *Memdb) Hget(key, subkey string) ([]byte, error) {
	if m.Hvalues == nil {
		return nil, nil
	}
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	if v, exists := m.Hvalues[key]; exists {
		if v, exists := v[subkey]; exists {
			return v, nil
		}
	}
	return nil, nil
}

func (m *Memdb) Hset(key, subkey string, value []byte) (int, error) {
	ret := 0
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if _, exists := m.Hvalues[key]; !exists {
		m.Hvalues[key] = make(HashValue)
		ret = 1
	}

	if _, exists := m.Hvalues[key][subkey]; !exists {
		ret = 1
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method:"hset",Key:key,Args:[][]byte{[]byte(subkey),value}})
		if err != nil {
			return 0,err
		}
	}
	m.Hvalues[key][subkey] = value
	return ret, nil
}

func (m *Memdb) Hgetall(key string) (HashValue, error) {
	if  m.Hvalues == nil {
		return nil, nil
	}
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	return m.Hvalues[key], nil
}

func (m *Memdb) Get(key string) ([]byte, error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	return m.Values[key], nil
}

func (m *Memdb) Set(key string, value []byte) error {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "set", Key: key, Args: [][]byte{value}})
		if err != nil {
			return err
		}
	}
	m.Values[key] = value
	return nil
}


func (m *Memdb) Mset(values ...[]byte) error {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if len(values) % 2 != 0 {
		return errors.New("wrong number of arguments for MSET")
	}

	var bytes [][]byte
	kvmap := make(map[string][]byte)
	for i,v:= range values {
		bytes = append(bytes,[]byte(v))
		if i % 2 == 0 {
			kvmap[string(v)] = values[i+1]
		}
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "mset", Args: bytes})
		if err != nil {
			return err
		}
	}
	for k,v:= range kvmap {
		m.Values[k] = v
	}
	return nil
}


func (m *Memdb) Incr (key string) (int, error) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	v,found := m.Values[key]
	num  := 0
	var err error
	if found {
		num ,err = strconv.Atoi(string(v))
		if err != nil {
			return 0,errors.New("value is not an integer or out of range")
		}
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "incr", Key: key})
		if err != nil {
			return 0,err
		}
	}
	m.Values[key] = []byte(fmt.Sprintf("%d",num+1))
	return num ,nil
}

func (m *Memdb) Del(key string, keys ...string) (int, error) {

	keys = append([]string{key}, keys...)
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	count := 0

	bytes := make([][]byte,0)
	for _,v := range keys {
		bytes = append(bytes,[]byte(v))
	}
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "del", Args: bytes})
		if err != nil {
			return 0, err
		}
	}
	for _, k := range keys {
		if _, exists := m.Values[k]; exists {
			delete(m.Values, k)
			count++
		}
		if _, exists := m.Hvalues[key]; exists {
			delete(m.Hvalues, k)
			count++
		}

		if _, exists := m.HSet[key]; exists {
			delete(m.HSet, k)
			count++
		}
	}
	return count, nil
}

//sort set

func (m *Memdb) Zadd (key string,score int,val string) (int ,error){
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if _, exists := m.HSortSet[key]; !exists {
		m.HSortSet[key] = make(HashInt)
	}

	if _, exists := m.skiplist[key]; !exists {
		m.skiplist[key] = structure.NewIntMap()
	}

	if !m.recovebool {
		bytes := make([][]byte, 0)
		bytes = append(bytes, []byte(val))
		bytes = append(bytes, IntToBytes(score))
		err := m.s.w.save(&Opt{Method: "zadd", Key: key, Args: bytes})
		if err != nil {
			return 0, err
		}
	}
	count := 0
	_ ,found :=m.HSortSet[key][val]
	if !found {
		count = 1
	}
	m.HSortSet[key][val] = score
	m.skiplist[key].Set(score,val)
	return count,nil
}


func (m *Memdb) Zrange(key string, start, stop int,args ...string) ([][]byte, error) {
	withscores := false
	if len (args) > 0  {
		if strings.ToLower(args[0]) != "withscores"{
			return nil,errors.New("ERR syntax error")
		}else{
			withscores = true
		}
	}
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()
	if _, exists := m.skiplist[key]; !exists {
		return nil,nil
	}
	iter := m.skiplist[key].Range(start,stop)
	var ret [][]byte
	for iter.Next() {
		if withscores {
			ret = append(ret, []byte(fmt.Sprintf("%d",iter.Key().(int))))
		}
		ret = append(ret,[]byte(iter.Value().(string)))
	}
	return ret, nil
}

/*

func (m *Memdb) Ping() (*StatusReply, error) {
	return &StatusReply{code: "PONG"}, nil

}*/

func (m *Memdb) Select(key string) error {
	return nil
}
/*
func (m *Memdb) Monitor() (*MonitorReply, error) {
	return &MonitorReply{}, nil
}*/


