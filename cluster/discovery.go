package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/name5566/leaf/conf"
	etcd "github.com/name5566/leaf/etcd"
	"github.com/name5566/leaf/log"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/mgo.v2/bson"
	"math"
	"strings"
)

//服务信息
type ServerInfo struct {
	Name           string `json:"name"`           //服务器名
	Type           string `json:"type"`           //服务器类型编号
	TCPAddr        string `json:"TCPAddr"`        //服务器地址
	WSAddr         string `json:"WSAddr"`         //服务器地址
	ListenAddr     string `json:"listenAddr"`     //rpc地址
	Weight         int    `json:"weight"`         //权重
	ClientCount    int    `json:"clientCount"`    //客户端数量
	MaxClientCount int    `json:"maxClientCount"` //最大客户端数量
	GlobalId       string `json:"globalId"`       //全局唯一id
}

var (
	ttl           int64            = 45
	LeaseID       clientv3.LeaseID = 0
	serverMap                      = make(map[string]map[string]*ServerInfo)
	serverIdMap                    = make(map[string]clientv3.LeaseID)
	ConfigPath                     = "config"
	PathSeparator                  = "/"
)

func RegisterServer() {
	err := etcd.Dial(conf.Endpoints)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// 服务注册
	serverInfo := ServerInfo{
		Name:           conf.ServerName,
		Type:           conf.ServerType,
		TCPAddr:        conf.TCPAddr,
		WSAddr:         conf.WSAddr,
		ListenAddr:     conf.ListenAddr,
		Weight:         0,
		ClientCount:    0,
		MaxClientCount: 65536,
		GlobalId:       bson.NewObjectId().Hex(), //生成全局唯一id
	}

	prefix := serverInfo.Type
	id, err := etcd.MarshalKeyTTL(
		GetEtcdClientKey(&serverInfo), serverInfo,
		ttl)

	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// 持续启动续约
	_, err = etcd.Keeplive(id)
	if err != nil {
		log.Fatal(err.Error())
	}

	LeaseID = id

	watchServer(prefix)

	for _, prefix := range conf.Watcher {
		watchServer(prefix)
	}
}

//获得etcd key
func GetEtcdClientKey(serverInfo *ServerInfo) string {
	return strings.Join([]string{serverInfo.Type, serverInfo.Name}, PathSeparator)
}

//获得配置路径
func GetConfigPath(configName string) string {
	return strings.Join([]string{ConfigPath, configName}, PathSeparator)
}

//列出配置
func ListConfig() (map[string]string, error) {
	resp, err := etcd.ReadAll(ConfigPath)
	if nil != err {
		return nil, err
	}
	retMap := make(map[string]string)
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			key := string(resp.Kvs[i].Key)
			retMap[key] = string(resp.Kvs[i].Value)
		}
	}
	return retMap, nil
}

func RemoveServer() {
	etcd.Revoke(LeaseID) // 强制过期
	etcd.Close()
}

// 监控服务器
func watchServer(prefix string) {
	_serverMap, exists := serverMap[prefix]
	if !exists {
		_serverMap = make(map[string]*ServerInfo, 0)
	}
	serverMap[prefix] = _serverMap

	resp, err := etcd.ReadAll(prefix)
	if err != nil {
		log.Fatal(err.Error())
	}
	readServiceList(resp, prefix)

	err = etcd.BindWatcher(prefix, watchHandler, 0, prefix, 0.0)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
}

func watchHandler(vtype string, key, values []byte, other01 int, other02 string, other03 float64) {
	prefix := other02

	switch vtype {
	case "PUT":
		addServerList(prefix, string(key), string(values))
	case "DELETE":
		delServerList(prefix, string(key))
	}
}

func readServiceList(resp *clientv3.GetResponse, prefix string) {
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			key := string(resp.Kvs[i].Key)
			addServerList(prefix, key, string(resp.Kvs[i].Value))
		}
	}
}

func addServerList(prefix string, key, val string) {
	var serverInfo *ServerInfo
	err := json.Unmarshal([]byte(val), &serverInfo)
	if err != nil {
		log.Error(err.Error())
		return
	}

	serverMap[prefix][key] = serverInfo

	addClusterClient(serverInfo)

	log.Release("set data key:%s val:%v", key, serverInfo)
}

func delServerList(prefix string, key string) {
	serverInfo := serverMap[prefix][key]

	delete(serverMap[prefix], key)

	if serverInfo != nil {
		removeClusterClient(serverInfo)
		if strings.Join([]string{conf.ServerType, conf.ServerName}, PathSeparator) == key { //当前服务器
			//Todo etcd服务器重启了
			log.Release("etcd服务器重启了")
		}
	}

	log.Release("del data key:%s", key)
}

func addClusterClient(serverInfo *ServerInfo) {
	if conf.ServerType == "frontServer" && serverInfo.Type != "frontServer" {
		if serverInfo.ListenAddr != "" {
			AddClient(serverInfo.Name, serverInfo.ListenAddr)
		}
	} else if conf.ServerType == "chatServer" {
		if serverInfo.Type == "frontServer" {
			if serverInfo.ListenAddr != "" {
				AddClient(serverInfo.Name, serverInfo.ListenAddr)
			}
		}
	}
}

func removeClusterClient(serverInfo *ServerInfo) {
	if conf.ServerType == "frontServer" && serverInfo.Type != "frontServer" {
		if serverInfo.ListenAddr != "" {
			RemoveClient(serverInfo.Name)
		}
	} else if conf.ServerType == "chatServer" {
		if serverInfo.Type == "frontServer" {
			RemoveClient(serverInfo.Name)
		}
	}
}

func GetServerMap(prefix string) map[string]*ServerInfo {
	return serverMap[prefix]
}

func GetBestServerInfo(args ...interface{}) ([]interface{}, error) {
	serverType := args[0].(string)
	var key string

	if args[1] != nil {

		key = args[1].(string)
		var serverInfo ServerInfo
		err := etcd.UnmarshalKey(key, &serverInfo) //读取进入的服务器信息
		if err == nil {
			body, err := etcd.Read(GetEtcdClientKey(&serverInfo)) //读取服务器是否存活
			if err != nil {
				log.Debug(err.Error())
				return nil, err
			}
			var tempServerInfo ServerInfo
			err = json.Unmarshal(body, &tempServerInfo)
			if err != nil {
				log.Debug(err.Error())
				return nil, err
			}

			if tempServerInfo.GlobalId == serverInfo.GlobalId { //相同表示服务器信息没有改变
				return []interface{}{serverInfo.Name, serverInfo.WSAddr}, nil
			}
			err = etcd.Delete(key, false) //删除进入的服务器信息
			if err != nil {
				log.Debug(err.Error())
			}
		} else {
			log.Debug(err.Error())
		}
	}

	var serverInfo *ServerInfo
	if _serverMap, ok := serverMap[serverType]; ok {
		minClientCount := math.MaxInt32
		for _, _serverInfo := range _serverMap {
			if _serverInfo.ClientCount < minClientCount && _serverInfo.ClientCount < _serverInfo.MaxClientCount {
				serverInfo = _serverInfo
			}
		}
	}

	if serverInfo == nil {
		return nil, errors.New(fmt.Sprintf("No %s server to alloc", serverType))
	} else {
		err := etcd.MarshalKey(key, *serverInfo) //设置进入的服务器信息
		return []interface{}{serverInfo.Name, serverInfo.WSAddr}, err
	}
}

func DelServerInfo(args ...interface{}) {
	keys := args[0].([]string)
	for _, key := range keys {
		err := etcd.Delete(key, false) //删除进入的服务器信息
		if err != nil {
			log.Error(err.Error())
		}
	}
	log.Debug("%v serverInfo is delete", keys)
}

func UpdateServerInfo(args ...interface{}) {
	serverType := args[0].(string)
	serverName := args[1].(string)
	clientCount := args[2].(int)
	serviceInfo, ok := serverMap[serverType][serverName]
	if ok {
		serviceInfo.ClientCount = clientCount
		err := etcd.MarshalKeyGrent(GetEtcdClientKey(serviceInfo), serviceInfo, serverIdMap[serverName])
		if err != nil {
			log.Error(err.Error())
		}
		log.Debug("%v server of client count is %v", serverName, clientCount)
	}
}
