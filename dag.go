package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

const (
	k          = 1 << 10
	Block_Size = 256 * k
)

// Add函数用于向Merkle Dag中添加节点
// 参数store是用于存储数据的KVStore实例
// 参数node是要添加的节点
// 参数h是哈希函数实例
func Add(store KVStore, node Node, h hash.Hash) []byte {
	switch node.(type) {
	case File:
		file := node.(File)
		addFile(store, file, h)
	case Dir:
		dir := node.(Dir)
		addDir(store, dir, h)
	}
	return h.Sum(nil)
}

// addFile函数用于将文件节点添加到Merkle Dag中
func addFile(store KVStore, file File, h hash.Hash) *Object {
	data := file.Bytes()
	if len(data) <= Block_Size {
		// 如果文件大小不超过256KB，则直接将文件内容写入KVStore，并返回哈希值
		blob := Object{
			Links: nil,
			Data:  data,
		}
		jsonMarshal, _ := json.Marshal(blob)
		h.Write(jsonMarshal)
		store.Put(h.Sum(nil), data)
		return &blob
	} else {
		res := dealFile_(file, store, h)
		return res
	}
}

// addDir函数用于将文件夹节点添加到Merkle Dag中
func addDir(store KVStore, dir Dir, h hash.Hash) *Object {
	it := dir.It()
	treesObject := &Object{}

	// 遍历文件夹中的文件和文件夹
	for it.Next() {
		node := it.Node()

		switch node.(type) {
		case File:
			// 如果是文件节点，则递归调用addFile函数，并保存返回的哈希值和文件名
			file := node.(File)
			tmp := addFile(store, file, h)
			jsonMarshal, _ := json.Marshal(tmp)
			h.Write(jsonMarshal)
			treesObject.Links = append(treesObject.Links, Link{
				Hash: h.Sum(nil),
				Size: int(file.Size()),
				Name: file.Name(),
			})
			typeName := "link"
			if tmp.Links == nil {
				typeName = "blob"
			}
			treesObject.Data = append(treesObject.Data, []byte(typeName)...)

		case Dir:
			// 如果是文件夹节点，则递归调用addDir函数，并保存返回的哈希值和文件夹名
			dir := node.(Dir)
			tmp := addDir(store, dir, h)
			jsonMarshal, _ := json.Marshal(tmp)
			h.Write(jsonMarshal)
			treesObject.Links = append(treesObject.Links, Link{
				Hash: h.Sum(nil),
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			typeName := "tree"
			treesObject.Data = append(treesObject.Data, []byte(typeName)...)
		}
	}
	jsonMarshal, _ := json.Marshal(treesObject)
	h.Write(jsonMarshal)
	store.Put(h.Sum(nil), jsonMarshal)
	return treesObject
}

func dealFile_(node File, store KVStore, h hash.Hash) *Object {
	links := &Object{}
	for i := 0; i < int(node.Size()/256)+1; i += 256 { //有几个256 分多一块
		//分片  需要link
		//end 每次 + 256  最后一片不满256  end = node.Size()
		//data 取 [i:end]
		end := i + 256
		if int(node.Size()) < end {
			end = int(node.Size())
		}
		data := node.Bytes()[i:end]
		blob := Object{
			Links: nil,
			Data:  data,
		}
		jsonMarshal, _ := json.Marshal(blob)
		h.Write(jsonMarshal)
		store.Put(h.Sum(nil), data)

		//分片写入links 每个ipfs就代指一片
		links.Links = append(links.Links, Link{
			Name: node.Name(),
			Hash: h.Sum(nil),
			Size: len(data),
		})
		links.Data = append(links.Data, []byte("blob")...)
	}
	//links写入KVStore
	jsonMarshal, _ := json.Marshal(links)
	h.Write(jsonMarshal)
	store.Put(h.Sum(nil), jsonMarshal)
	return links
}
