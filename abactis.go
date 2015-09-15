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
  client, _ := consul.NewClient(consul.DefaultConfig())
  kv := client.KV()

  var oldIndex uint64 = 0
  var oldKvPairs consul.KVPairs = nil

  for {
    newKvPairs, meta, err := kv.List("", nil)

    MaybePanic(err) // TODO: Don't panic in RL, probably log & exponential backoff

    newIndex := meta.LastIndex

    if (newIndex == oldIndex) {
      continue
    }

    _oldIndex := oldIndex
    oldIndex = newIndex
    if _oldIndex != 0 && reflect.DeepEqual(oldKvPairs, newKvPairs) {
      continue
    }

    logger.Println("Old KV pairs:")
    for _, kvPair := range oldKvPairs {
      logger.Printf("%v : %v\n", kvPair.Key, string(kvPair.Value))
    }

    logger.Println("New KV pairs:")
    for _, kvPair := range newKvPairs {
      logger.Printf("%v : %v\n", kvPair.Key, string(kvPair.Value))
    }
    logger.Println()

    oldKvPairs = newKvPairs
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
