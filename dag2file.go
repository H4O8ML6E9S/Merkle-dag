package merkledag

import (
	"encoding/json"
	"path/filepath"
	"strings"
)

// Hash to file
func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte { //hp哈希池的作用？？
	// 根据hash和path，返回对应的文件，hash对应的类型是tree

	// 从kvstore中根据哈希找对应值   Get(key []byte) ([]byte, error)
	treeData, err := store.Get(hash)
	if err != nil {
		return nil
	}

	// 解析tree对象
	var treeObject Object
	json.Unmarshal(treeData, &treeObject)

	// 根据path获取对应的文件内容
	fileContent := getFileContent(store, treeObject, path, hp)

	return fileContent
}

func getFileContent(store KVStore, treeObject Object, path string, hp HashPool) []byte {
	if path == "" {
		// 返回tree对象的内容作为文件内容
		return treeObject.Data
	}

	// 将路径按斜杠分隔为多个级别
	levels := strings.Split(path, "/")

	// 遍历Links
	for _, link := range treeObject.Links {
		if link.Name == levels[0] {
			if len(link.Hash) == 0 {
				//IPFSLink包括 Hash  Size  Name 三个属性
				//没有哈希值与之关联 link的类型为blob，直接返回
				fileData, err := store.Get(link.Hash)
				if err != nil {
					return nil
				}
				return fileData
			} else {
				// link的类型为tree，递归
				subTreeData, err := store.Get(link.Hash)
				if err != nil {
					return nil
				}
				var subTreeObject Object
				json.Unmarshal(subTreeData, &subTreeObject)

				return getFileContent(store, subTreeObject, filepath.Join(levels[1:]...), hp)
			}
		}
	}

	return nil
}
