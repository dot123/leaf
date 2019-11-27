// Etcd封装库，简化各种操作
// version 1.0 beta
// by koangel
// email: jackliu100@gmail.com
// 2017/9/26

package grapeEtcd

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"go.etcd.io/etcd/clientv3"
)

type EtcdHandler interface{}

type EtcdWatcher struct {
	key     string
	caller  reflect.Value
	args    []reflect.Value
	mux     sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	wchan   clientv3.WatchChan
	isEvent bool
}

func (w *EtcdWatcher) init(opts ...clientv3.OpOption) {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wchan = EtcdCli.Watch(w.ctx, w.key, opts...) // 建立监听行为

	go w.watcher() //开始听着
}

// 调用一开始绑定函数，处理这个事
func (w *EtcdWatcher) call(vtype string, key, value []byte) {
	var in = make([]reflect.Value, watchMustArgs)
	in[0] = reflect.ValueOf(vtype)
	in[1] = reflect.ValueOf(key)
	in[2] = reflect.ValueOf(value)

	for _, v := range w.args {
		in = append(in, v)
	}

	w.caller.Call(in)
}

func (w *EtcdWatcher) callEvent(ev *clientv3.Event) {
	var in = make([]reflect.Value, 1)

	in[0] = reflect.ValueOf(ev)
	for _, v := range w.args {
		in = append(in, v)
	}

	w.caller.Call(in)
}

// 跑着处理所有相关的watcher事宜
func (w *EtcdWatcher) watcher() {

	for wch := range w.wchan {
		for _, ev := range wch.Events {
			if w.isEvent {
				w.callEvent(ev)
			} else {
				// 处理他
				w.call(ev.Type.String(), ev.Kv.Key, ev.Kv.Value)
			}
		}
	}
}

func (w *EtcdWatcher) Close() {
	w.cancel() // 关闭他
}

func BindWatcherPrefix(key string, isPrefix bool, wFunc EtcdHandler, args ...interface{}) error {
	_, ok := watchers.Load(key)
	if ok {
		return errors.New("Key already exists")
	}

	t := reflect.TypeOf(wFunc) // 先获得是否是一个函数
	if t.Kind() != reflect.Func {
		return errors.New("Handler must be a function")
	}

	// 顺道获取下有多少个参数并把函数的部分参数绑定进去
	argArr := args // 先把参数都转成ARRAY
	var mustArgs = watchMustArgs
	if t.NumIn() == 1 {

		chkType := t.In(0)
		if chkType.Kind() != reflect.Ptr {
			return errors.New("Handler 1st argument must be a ptr")
		}

		chkType = chkType.Elem()
		event := reflect.TypeOf(clientv3.Event{})
		if event.NumField() != chkType.NumField() && chkType.Kind() != reflect.Struct {
			return errors.New("Handler 1st argument must be a clientv3.Event")
		}

		mustArgs = 1
	} else {
		if (len(argArr) + watchMustArgs) != t.NumIn() {
			return errors.New("Not enough arguments")
		}

		chkType := t.In(0)
		if chkType.Kind() != reflect.String {
			return errors.New("Handler 1st argument must be a string")
		}

		chkType = t.In(1)
		if chkType.Kind() != reflect.Slice {
			return errors.New("Handler 2nd argument must be a []byte")
		}

		chkType = t.In(2)
		if chkType.Kind() != reflect.Slice {
			return errors.New("Handler 3rd argument must be a []byte")
		}
	}
	// 解析全部参数
	var in = make([]reflect.Value, (t.NumIn() - mustArgs)) //MAKE要保存的参数
	for i := 0; i < (t.NumIn() - mustArgs); i++ {          // 跳过第一个type string参数
		argType := t.In(i + mustArgs)
		if argType != reflect.TypeOf(argArr[i]) {
			return errors.New(fmt.Sprintf("Value not found for type %v", argType))
		}
		in[i] = reflect.ValueOf(argArr[i]) // 参数保存下来
	}
	wc := &EtcdWatcher{
		key:     key,
		caller:  reflect.ValueOf(wFunc),
		args:    in,
		isEvent: mustArgs == 1,
	}

	if isPrefix == false {
		wc.init()
	} else {
		wc.init(clientv3.WithPrefix())
	}

	watchers.Store(key, wc)
	return nil
}

///////////////////////////////////////////////////////////////
// watcher
// watcher函数第一个参数必须是string,会自动传入type,否则无法绑定
// watcher函数第二个参数必须是[]byte,会自动传入key,否则无法绑定
// watcher函数第三个个参数必须是[]byte,会自动传入value,否则无法绑定
// watcher example:func TestCallback(type string,value []byte,testInt int,testFloat float32)
func BindWatcher(key string, wFunc EtcdHandler, args ...interface{}) error {
	return BindWatcherPrefix(key, true, wFunc, args...)
}

func StopWatcher(key string) error {

	w, ok := watchers.Load(key)
	if !ok {
		return errors.New(fmt.Sprint("unknow Watcher:", key))
	}

	w.(*EtcdWatcher).Close() // 先销毁他
	watchers.Delete(key)
	return nil
}
