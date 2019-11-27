// Etcd封装库，格式化对象专用
// version 1.0 beta
// by koangel
// email: jackliu100@gmail.com
// 2017/9/26

package grapeEtcd

import (
	"encoding/base64"
	"encoding/json"

	"gopkg.in/mgo.v2/bson"
)

// 格式化CallBack
type Formatter interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
	ToString(convert []byte) string
	FromString(convert string) []byte
}

// 内建支持json和base64等
//////////////////////////////////////////////////
// json格式化
type JsonFormatter struct {
}

func (f *JsonFormatter) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (f *JsonFormatter) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (f *JsonFormatter) ToString(convert []byte) string {
	return string(convert)
}

func (f *JsonFormatter) FromString(convert string) []byte {
	return []byte(convert)
}

//////////////////////////////////////////////////
// bson的格式化
type BsonFormatter struct {
}

func (f *BsonFormatter) Marshal(v interface{}) ([]byte, error) {
	return bson.Marshal(v)
}

func (f *BsonFormatter) Unmarshal(data []byte, v interface{}) error {
	return bson.Unmarshal(data, v)
}

func (f *BsonFormatter) ToString(convert []byte) string {
	return base64.StdEncoding.EncodeToString(convert)
}

func (f *BsonFormatter) FromString(convert string) []byte {
	b, _ := base64.StdEncoding.DecodeString(convert)
	return b
}
