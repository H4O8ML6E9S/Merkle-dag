package merkledag

import (
	"encoding/json"
	"hash"
)

type Tree struct {
	Hash []byte
	Data []byte
}

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
	switch n := node.(type) {
	case File:
		return addFile(store, n, h)
	case Dir:
		return addDir(store, n, h)
	default:
		return nil
	}
}

// addFile函数用于将文件节点添加到Merkle Dag中
func addFile(store KVStore, file File, h hash.Hash) []byte {
	data := file.Bytes()
	if len(data) <= Block_Size {
		// 如果文件大小不超过256KB，则直接将文件内容写入KVStore，并返回哈希值
		h.Write(data)
		hashValue := h.Sum(nil)
		err := store.Put(hashValue, data)
		if err != nil {
			return nil
		}
		return hashValue
	}

	// 如果文件大小超过256KB，则进行分片存储
	chunks := splitDataIntoChunks(data, Block_Size)
	var chunkHashes [][]byte

	// 遍历每个分片，计算哈希值，并将哈希值和分片数据保存在KVStore中
	for _, chunk := range chunks {
		h.Write(chunk)
		hashValue := h.Sum(nil)
		err := store.Put(hashValue, chunk)
		if err != nil {
			return nil
		}
		chunkHashes = append(chunkHashes, hashValue)
		h.Reset()
	}

	// 将所有分片的哈希值拼接成一个连续的字节切片
	concatenated := concatenateHashes(chunkHashes)
	h.Write(concatenated)

	// 计算并返回Merkle Root的哈希值
	merkleRoot := h.Sum(nil)
	return merkleRoot
}

// addDir函数用于将文件夹节点添加到Merkle Dag中
func addDir(store KVStore, dir Dir, h hash.Hash) []byte {
	it := dir.It()
	var links []Link

	// 遍历文件夹中的文件和文件夹
	for it.Next() {
		node := it.Node()

		switch n := node.(type) {
		case File:
			// 如果是文件节点，则递归调用addFile函数，并保存返回的哈希值和文件名
			hashValue := addFile(store, n, h)
			link := Link{
				Name: n.Name(),
				Hash: hashValue,
				Size: int(n.Size()),
			}
			links = append(links, link)
		case Dir:
			// 如果是文件夹节点，则递归调用addDir函数，并保存返回的哈希值和文件夹名
			hashValue := addDir(store, n, h)
			link := Link{
				Name: n.Name(),
				Hash: hashValue,
				Size: int(n.Size()),
			}
			links = append(links, link)
		}
	}

	// 将所有子节点的哈希值和名称组成一个Object
	obj := Object{
		Links: links,
		Data:  nil,
	}

	// 将Object序列化为JSON
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	// 计算JSON的哈希值作为Merkle Root，并将JSON数据保存在KVStore中
	h.Write(jsonData)
	merkleRoot := h.Sum(nil)
	err = store.Put(merkleRoot, jsonData)
	if err != nil {
		return nil
	}

	return merkleRoot
}

// splitDataIntoChunks函数用于将数据分成指定大小的块
func splitDataIntoChunks(data []byte, chunkSize int) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]
		chunks = append(chunks, chunk)
	}
	return chunks
}

// concatenateHashes函数用于将哈希值拼接成一个连续的字节切片。
func concatenateHashes(hashes [][]byte) []byte {
	var concatenated []byte
	for _, hashValue := range hashes {
		concatenated = append(concatenated, hashValue...)
	}
	return concatenated
}
