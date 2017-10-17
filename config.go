package newredis

type Config struct {
	net     string
	laddr   string
	datadir  string
	snapCount uint64
	walsavetype string
	sync      bool
}

func DefaultConfig() *Config {
	return &Config{
		net:    "tcp",
		laddr:    ":6380",
		snapCount :10000,
		datadir:"data/",
		walsavetype:"aw",
		sync : true,
	}
}

func (c *Config) Net(p string) *Config {
	c.net = p
	return c
}

func (c *Config) Laddr(h string) *Config {
	c.laddr = h
	return c
}

func (c *Config) SnapCount(n uint64) *Config {
	c.snapCount = n
	return c
}


func (c *Config) Sync(b bool) *Config {
	c.sync = b
	return c
}

func (c *Config) DataDir(w string) *Config {
	c.datadir = w
	return c
}
func (c *Config) OpenWal(s string) *Config {
	c.walsavetype = s
	return c
}


func (c *Config)Gaddr() string{
	return c.laddr
}

func (c *Config)Gwalsavetype() string{
	return c.walsavetype
}

func (c *Config)Gsync() bool{
	return c.sync
}