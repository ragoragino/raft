package proxy

import (
	"fmt"
)

type ProxyCodec struct{}

func NewProxyCodec() ProxyCodec {
	return ProxyCodec{}
}

func (c ProxyCodec) Marshal(v interface{}) ([]byte, error) {
	out, ok := v.(*Payload)
	if !ok {
		return nil, fmt.Errorf("unable to marshal to bytes")
	}

	return out.payload, nil
}

func (c ProxyCodec) Unmarshal(data []byte, v interface{}) error {
	dst, ok := v.(*Payload)
	if !ok {
		return fmt.Errorf("unable to unmarshal to Payload")
	}

	dst.payload = data
	return nil
}

func (c ProxyCodec) String() string {
	return "proxy-codec"
}
