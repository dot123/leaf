// Etcd封装库，简化各种操作
// version 1.0 beta
// by koangel
// email: jackliu100@gmail.com
// 2017/9/26

package grapeEtcd

import (
	"context"
	"errors"
	"go.etcd.io/etcd/clientv3"
	"sync"
	"time"
)

var (
	// 序列化和反序列化时的格式函数
	formatter Formatter = &JsonFormatter{}
	// 监听函数以及监听位置
	watchers sync.Map

	// etcd内部客户端
	EtcdCli *clientv3.Client = nil
	// Close
	once *sync.Once = nil

	// 验证
	IsAuth       bool   = true
	AuthUserName string = "root"
	AuthPassword string = "123456"
)

const (
	writeTimeout = 45 * time.Second
	readTimeout  = 60 * time.Second

	watchMustArgs = 3
)

type M = map[string]interface{}

func Dial(urls []string) error {
	return DialTimeout(urls, 45*time.Second)
}

func DialTimeout(urls []string, timeout time.Duration) error {
	config := clientv3.Config{
		Endpoints:          urls,
		DialTimeout:        timeout,
		MaxCallSendMsgSize: 16 * 1024 * 1024,
		MaxCallRecvMsgSize: 512 * 1024 * 1024,
	}

	// 开启验证
	if IsAuth {
		config.Username = AuthUserName
		config.Password = AuthPassword
	}

	cli, err := clientv3.New(config)
	if err != nil {
		return err
	}
	once = &sync.Once{}
	EtcdCli = cli

	return nil
}

func Close() {
	if once != nil {
		once.Do(func() {
			if EtcdCli != nil {
				EtcdCli.Close()
				EtcdCli = nil
			}
		})
	}
}

func SetFormatter(in Formatter) {
	formatter = in
}

///////////////////////////////////////////////////////////////
// 读写函数
func Read(key string) (body []byte, err error) {
	body = nil
	err = nil
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	resp, verr := EtcdCli.Get(ctx, key)
	cancel()
	if verr != nil {
		err = verr
		return
	}

	if resp.Count == 0 {
		err = errors.New("keys is empty...")
		return
	}

	body = resp.Kvs[0].Value
	return
}

func ReadAll(key string) (body *clientv3.GetResponse, err error) {
	body = nil
	err = nil
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	resp, verr := EtcdCli.Get(ctx, key, clientv3.WithPrefix())
	cancel()
	if verr != nil {
		err = verr
		return
	}

	body = resp
	return
}

func UnmarshalKey(key string, val interface{}) error {
	resp, err := Read(key)
	if err != nil {
		return err
	}

	return formatter.Unmarshal(formatter.FromString(string(resp)), val)
}

func Write(key string, val []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	_, err := EtcdCli.Put(ctx, key, string(val))
	cancel()
	if err != nil {
		return err
	}

	return nil
}

func WriteTTL(key string, val []byte, ttl int64) (Id clientv3.LeaseID, err error) {
	Id = 0
	err = nil

	gresp, gerr := Grent(ttl)
	if gerr != nil {
		err = gerr
		return
	}

	Id = gresp.ID
	err = WriteGrent(key, val, gresp.ID)
	return
}

func WriteGrent(key string, val []byte, Id clientv3.LeaseID) error {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	_, err := EtcdCli.Put(ctx, key, string(val), clientv3.WithLease(Id))
	cancel()
	if err != nil {
		return err
	}

	return nil
}

func MarshalKey(key string, val interface{}) error {
	body, err := formatter.Marshal(val)
	if err != nil {
		return err
	}

	return Write(key, []byte(formatter.ToString(body)))
}

func MarshalKeyTTL(key string, val interface{}, ttl int64) (Id clientv3.LeaseID, err error) {
	err = nil
	Id = 0

	body, ferr := formatter.Marshal(val)
	if ferr != nil {
		err = ferr
		return
	}

	Id, err = WriteTTL(key, []byte(formatter.ToString(body)), ttl)
	return
}

func MarshalKeyGrent(key string, val interface{}, Id clientv3.LeaseID) (err error) {
	err = nil
	Id = 0

	body, ferr := formatter.Marshal(val)
	if ferr != nil {
		err = ferr
		return
	}

	WriteGrent(key, []byte(formatter.ToString(body)), Id)
	return
}

// 创建一个租约
func Grent(ttl int64) (resp *clientv3.LeaseGrantResponse, err error) {
	resp, err = EtcdCli.Grant(context.Background(), ttl)
	return
}

func TimeToLive(Id clientv3.LeaseID) (resp *clientv3.LeaseTimeToLiveResponse, err error) {
	resp, err = EtcdCli.TimeToLive(context.Background(), Id)
	return
}

// 持续续约 会开启一个持续的携程去续约这个租约
func Keeplive(Id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return EtcdCli.KeepAlive(context.Background(), Id)
}

// 续约一次 本次只续约一次，过后依旧会删除
func KeepliveOnce(Id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return EtcdCli.KeepAliveOnce(context.Background(), Id)
}

// 强制取消租约
func Revoke(Id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return EtcdCli.Revoke(context.Background(), Id)
}

// 删除Key
func Delete(key string, prefix bool) error {
	if prefix {
		_, err := EtcdCli.Delete(context.Background(), key, clientv3.WithPrefix())
		return err
	}
	_, err := EtcdCli.Delete(context.Background(), key)
	return err
}
