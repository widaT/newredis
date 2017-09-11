package newredis

import "strings"

type fn func(conn Conn, cmd Command) error

var commandMap = make(map[string]fn)

func registerCmd(cmd string,f fn)  {
	commandMap[cmd] = f
}

func DoCmd(conn Conn, cmd Command) error {
	c := strings.ToLower(string(cmd.Args[0]))
	f,found := commandMap[c]
	if !found{
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		return nil
	}
	return f(conn,cmd)
}

func set(conn Conn, cmd Command) error {


	return nil
}