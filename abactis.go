package main

import (
  "fmt"
  "log"
  "os"
  "reflect"
  consul "github.com/hashicorp/consul/api"
)

var logFlags = log.LstdFlags
var logger   = log.New(os.Stderr, "", logFlags)

func main() {
  client, err := consul.NewClient(consul.DefaultConfig())
  MaybeFatal(err)
  kv := client.KV()

  var oldIndex uint64 = 0
  oldKvPairs := make(consul.KVPairs, 0)
  oldKvMap := make(map[string]string)

  for {
    newKvPairs, meta, err := kv.List("", nil)

    MaybePanic(err) // TODO: Don't panic in RL, probably log & exponential backoff

    newIndex := meta.LastIndex

    // If the index is unchanged do nothing
    if (newIndex == oldIndex) {
      continue
    }

    // Update the index, look for change
    _oldIndex := oldIndex
    oldIndex = newIndex
    if _oldIndex != 0 && reflect.DeepEqual(oldKvPairs, newKvPairs) {
      continue
    }

    // Handle the updated result

    newKvMap := make(map[string]string, len(newKvPairs))
    allKeysMap := make(map[string]string, len(oldKvMap) + len(newKvPairs))

    for k, v := range oldKvMap {
      allKeysMap[k] = v
    }

    for _, kvPair := range newKvPairs {
      allKeysMap[kvPair.Key] = string(kvPair.Value)
      newKvMap[kvPair.Key] = string(kvPair.Value)
    }

    allKeys := make([]string, len(allKeysMap))
    for k, _ := range allKeysMap {
      allKeys = append(allKeys, k)
    }

    type ValDiff struct {
      Old, New string
    }

    var modKeyPairs = make(map[string]ValDiff)
    var addKeyPairs = make(map[string]string)
    var remKeyPairs = make(map[string]string)

    for _, key := range allKeys {
      oldV, oldOk := oldKvMap[key]
      newV, newOk := newKvMap[key]
      // oldOk false  newOk false  => impossible
      // oldOk false  newOk true   => added
      // oldOk true   newOk false  => removed
      // oldOk true   newOk true   => possibly modified
      if oldOk && newOk && oldV != newV { // changed
        modKeyPairs[key] = ValDiff{oldV, newV}
      } else if oldOk && !newOk { // removed
        remKeyPairs[key] = oldV
      } else if !oldOk && newOk { // added
        addKeyPairs[key] = newV
      }
    }

    logger.Println("ADD KV pairs:")
    for k, v := range addKeyPairs {
      logger.Printf("ADD %v : %v\n", k, v)
    }
    logger.Println()

    logger.Println("MOD KV pairs:")
    for k, v := range modKeyPairs {
      logger.Printf("MOD %v : old: %v new: %v\n", k, v.Old, v.New)
    }
    logger.Println()

    logger.Println("REM KV pairs:")
    for k, v := range remKeyPairs {
      logger.Printf("REM %v : %v\n", k, v)
    }
    logger.Println()
    logger.Println()

    oldKvPairs = newKvPairs
    oldKvMap = newKvMap
  }
}

func IsNilError(e error) bool {
  return e == nil || reflect.ValueOf(e).IsNil()
}
func IsError(e error) bool {
  return !IsNilError(e)
}

func TypedString(x interface{}) string {
  return fmt.Sprintf("%v %T", x, x)
}

func MaybeFatal(e error) { if IsError(e) { logger.Fatal(e) } }
func MaybePanic(e error) { if IsError(e) { logger.Panic(e) } }
func MaybeLog(e error)   { if IsError(e) { logger.Println(TypedString(e)) } }
