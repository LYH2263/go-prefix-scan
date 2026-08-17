# go-prefix-scan

嵌入式 LSM-Tree 键值库（Go library），提供 WAL + MemTable(SkipList) + SSTable + Manifest + Compaction。

## 模块

`github.com/LYH2263/go-prefix-scan`（Go 1.22）

## 包结构

| 包 | 作用 |
|---|---|
| `lsmkv` | 对外 API：Open/Close、Put/Get/Delete、WriteBatch、Sync、Compact、Iterator、Options |
| `internal/wal` | CRC 分帧 WAL，截断尾部自动丢弃 |
| `internal/memtable` | SkipList 内存表 |
| `internal/sstable` | 分块 SSTable（restart + bloom） |
| `internal/manifest` | JSON Manifest，原子替换 |
| `internal/compact` | 多路归并，按 seq / tombstone 裁决 |
| `internal/bloomx` | Bloom Filter |
| `internal/byteutil` / `crc32x` / `varintx` / `filelock` | 基础工具 |

## 快速使用

```go
db, err := lsmkv.Open("/data/kv", &lsmkv.Options{
    MemtableBytes: 4 << 20,
    SyncPolicy:    lsmkv.SyncFull,
})
if err != nil { /* ... */ }
defer db.Close()

_ = db.Put([]byte("k"), []byte("v"))
v, err := db.Get([]byte("k"))
_ = db.Delete([]byte("k"))
_ = db.Compact()
```

## 测试

```bash
export GOTOOLCHAIN=local
go test ./... -count=1
```

## 说明

本仓库是 **library/component**，不是 HTTP 服务、CLI、游戏或业务 CRUD。
