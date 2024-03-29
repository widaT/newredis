package newredis

import (
	"sync"
	"github.com/widaT/newredis/structure"
	"fmt"
	"strings"
	"errors"
	"strconv"
	"github.com/vmihailenco/msgpack"
)

type (
	HashValue   map[string][]byte
	HashFloat     map[string]float64
	HashHash    map[string]HashValue
	HashHashInt map[string]HashFloat
	HashBrStack map[string]*structure.List
	HashSkipList map[string]*structure.SkipList
	HashSet     map[string]*structure.Set
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
	recovebool bool   //初始化的时候不重复写wal
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
	b,err := msgpack.Marshal(m)
	if err != nil {
		return nil,err
	}
	List  := m.dlList
	m.HList = make(HashList)
	for key,v:= range List {
		m.HList[key] = v.Values()
	}
	m.HList = nil
	return b,nil
}

func (m *Memdb)  recoverFromSnapshot(snapshot []byte) error {
	var db Memdb

	if err := msgpack.Unmarshal(snapshot,&db); err != nil {
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
		intmap := structure.NewSkipList()
		for k,v := range val {
			intmap.Set(v,k)
		}
		db.skiplist[key] = intmap
	}
	db.s = m.s
	*m = db
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

func (m *Memdb) Lrange(key string, start, stop int) (*[][]byte, error) {
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
	return &ret, nil
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

func (m *Memdb) Del(keys ...[]byte) (int, error) {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	count := 0
	if !m.recovebool {
		err := m.s.w.save(&Opt{Method: "del", Args: keys})
		if err != nil {
			return 0, err
		}
	}
	for _, k := range keys {
		key := string(k)
		if _, exists := m.Values[key]; exists {
			delete(m.Values, key)
			count++
		}
		if _, exists := m.Hvalues[key]; exists {
			delete(m.Hvalues, key)
			count++
		}
		if _, exists := m.HSet[key]; exists {
			delete(m.HSet, key)
			count++
		}

		if _, exists := m.HList[key]; exists {
			delete(m.HList, key)
			count++
		}
		if _, exists := m.skiplist[key]; exists {
			delete(m.skiplist, key)
			//count++
		}
		if _, exists := m.HSortSet[key]; exists {
			delete(m.HSortSet, key)
			count++
		}
	}
	return count, nil
}

//sort set
func (m *Memdb) Zadd (key string,score float64,val string) (int ,error){
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if _, exists := m.HSortSet[key]; !exists {
		m.HSortSet[key] = make(HashFloat)
	}
	if _, exists := m.skiplist[key]; !exists {
		m.skiplist[key] = structure.NewSkipList()
	}
	if !m.recovebool {
		bytes := make([][]byte, 0)
		bytes = append(bytes, []byte(val))
		bytes = append(bytes, FloatToBytes(score))
		err := m.s.w.save(&Opt{Method: "zadd", Key: key, Args: bytes})
		if err != nil {
			return 0, err
		}
	}
	count := 0
	old ,found :=m.HSortSet[key][val]
	if !found {
		count = 1
	}else {
		//del the old key val
		m.skiplist[key].Delete(old,val)
	}
	m.HSortSet[key][val] = score
	m.skiplist[key].Set(score,val)
	return count,nil
}

func (m *Memdb) Zrange(key string, start, stop int,args ...[]byte) (*[][]byte, error) {
	if _, exists := m.HSortSet[key]; !exists {
		return nil,nil
	}
	if _, exists := m.skiplist[key]; !exists {
		return nil,nil
	}
	withscores := false

	if len (args) > 0  {
		if strings.ToLower(string(args[0])) != "withscores"{
			return nil,errors.New("ERR syntax error")
		}else{
			withscores = true
		}
	}
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()


	if start < 0 {
		if start =  m.skiplist[key].Len() + start; start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop =  m.skiplist[key].Len() + stop
		if stop <0 {
			return nil,nil
		}
	}

	if _, exists := m.skiplist[key]; !exists {
		return nil,nil
	}
	iter := m.skiplist[key].IndexRange(start,stop)
	var ret [][]byte
	for iter.Next() {
		if withscores {
			ret = append(ret, []byte(strconv.FormatFloat(iter.Key(), 'g', -1, 64)))
		}
		ret = append(ret,[]byte(iter.Value()))
	}
	iter.Close()
	return &ret, nil
}

func (m *Memdb)ZrangeByScore(key string, start, stop []byte,args ...[]byte) (*[][]byte, error) {
	if _, exists := m.HSortSet[key]; !exists {
		return nil,nil
	}
	if _, exists := m.skiplist[key]; !exists {
		return nil,nil
	}
	min,err1 := strconv.ParseFloat(string(start),64)
	max,err2 := strconv.ParseFloat(string(stop),64)
	if err1 != nil && (string(start) != "-inf" && start[0] != '(' ) {
		return nil,errors.New("ERR wrong number of arguments for zrangebyscore command")
	}
	if err2!= nil && (string(stop) != "+inf" && stop[0] != '(') {
		return nil,errors.New("ERR wrong number of arguments for zrangebyscore command")
	}

	moreTlanStart := false
	lessTlanEnd := false
	if start[0] == '(' {

		_,err:=fmt.Sscanf(string(start),"(%f",&min)
		if err !=nil {
			return nil,errors.New("ERR wrong number of arguments for zrangebyscore command")
		}
		moreTlanStart = true
	}

	if stop[0] == '(' {
		_,err:=fmt.Sscanf(string(stop),"(%f",&max)
		if err !=nil {
			return nil,errors.New("ERR wrong number of arguments for zrangebyscore command")
		}
		lessTlanEnd = true
	}


	withscores := false
	if len (args) > 0  {
		if strings.ToLower(string(args[0])) != "withscores"{
			return nil,errors.New("ERR syntax error")
		}else{
			withscores = true
		}
	}
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if string(start) == "-inf" {
		iter := m.skiplist[key].SeekToFirst()
		min = iter.Key()
		iter.Close()
	}

	if string(stop) == "+inf" {
		iter := m.skiplist[key].SeekToLast()
		max = iter.Key()
		iter.Close()
	}
	if _, exists := m.skiplist[key]; !exists {
		return nil,nil
	}
	iter := m.skiplist[key].Range(min,max)
	var ret [][]byte
	for iter.Next() {
		k := iter.Key()
		if moreTlanStart {
			if k == min {
				continue
			}
		}
		if lessTlanEnd {
			if k == max{
				break
			}
		}
		if withscores {
			ret = append(ret, []byte(strconv.FormatFloat(k, 'g', -1, 64)))
		}
		ret = append(ret,[]byte(iter.Value()))
	}
	iter.Close()
	return &ret, nil
}


