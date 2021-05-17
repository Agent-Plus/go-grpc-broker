package client

import (
	"strconv"
)

type Header map[string][]string

func (h Header) AddInt64(k string, v int64) {
	str := strconv.FormatInt(v, 10)
	h[k] = append(h[k], str)
}

func (h Header) AddString(k, v string) {
	h[k] = append(h[k], v)
}

func (h Header) Delete(k string) {
	delete(h, k)
}

func (h Header) GetInt64(k string) (int64, error) {
	v := h.GetString(k)

	if len(v) > 0 {
		return strconv.ParseInt(v, 10, 64)
	}
	return 0, nil
}

func (h Header) GetString(k string) string {
	if h == nil {
		return ""
	}
	v := h[k]

	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (h Header) SetInt64(k string, v int64) {
	h[k] = []string{strconv.FormatInt(v, 10)}
}

func (h Header) SetString(k, v string) {
	h[k] = []string{v}
}

func (h Header) ValuesInt64(k string) ([]int64, error) {
	v := h[k]
	if len(v) == 0 {
		return nil, nil
	}
	ret := make([]int64, 0, len(v))
	for _, str := range v {
		i, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, err
		}
		ret = append(ret, i)
	}

	return ret, nil
}

func (h Header) ValuesString(k string) []string {
	return h[k]
}
