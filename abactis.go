package main

import (
  "fmt"
  "log"
  "os"
  "reflect"
  consul "github.com/hashicorp/consul/api"
  consulWatch "github.com/hashicorp/consul/watch"
)

var logFlags  = log.LstdFlags
var logOutput = os.Stderr
var logPrefix = ""
var logger    = log.New(logOutput, logPrefix, logFlags)

func main() {
  logger.Println("Started.")

  conf := consul.DefaultConfig()

  watchParams := make(map[string]interface{})
  watchParams["type"] = "keyprefix"
  watchParams["prefix"] = "/"

  watch, err := consulWatch.Parse(watchParams)
  MaybeFatal(err)

  watch.Datacenter = conf.Datacenter
  watch.Token      = conf.Token
  watch.LogOutput  = logOutput
  watch.Handler    = makeKvPairsHandler(conf)

  MaybeFatal(watch.Run(conf.Address))
}

func makeKvPairsHandler(conf *consul.Config) consulWatch.HandlerFunc {
  client, err := consul.NewClient(conf)
  MaybeFatal(err)
  kv := client.KV()

  oldKvPairs, _, err := kv.List("/", nil)
  MaybeFatal(err)
  oldKvMap := makeKvMap(oldKvPairs)

  return func(index uint64, result interface{}) {
    newKvPairs := result.(consul.KVPairs)
    newKvMap := makeKvMap(newKvPairs)

    allKvPairs := append(oldKvPairs, newKvPairs...)
    allKeys := stringStringMapKeys(makeKvMap(allKvPairs))

    type StringDiff struct {
      Old, New string
    }

    var modKeyPairs = make(map[string]StringDiff)
    var addKeyPairs = make(map[string]string)
    var remKeyPairs = make(map[string]string)

    for _, key := range allKeys {
      oldV, oldOk := oldKvMap[key]
      newV, newOk := newKvMap[key]
      if oldOk && newOk && oldV != newV {
        modKeyPairs[key] = StringDiff{oldV, newV}
      } else if oldOk && !newOk {
        remKeyPairs[key] = oldV
      } else if !oldOk && newOk {
        addKeyPairs[key] = newV
      }
    }

    for k, v := range addKeyPairs {
      logger.Printf("ADD key = %v value = [%v]\n", k, v)
    }
    for k, v := range modKeyPairs {
      logger.Printf("MOD key = %v old value = [%v] new value = [%v]\n", k, v.Old, v.New)
    }
    for k, v := range remKeyPairs {
      logger.Printf("REM key = %v old value = [%v]\n", k, v)
    }

    oldKvPairs = newKvPairs
    oldKvMap = newKvMap
  }
}

func makeKvMap(kvPairs consul.KVPairs) map[string]string {
  kvMap := make(map[string]string, len(kvPairs))
  for _, kvPair := range kvPairs {
    kvMap[kvPair.Key] = string(kvPair.Value)
  }
  return kvMap
}

func stringStringMapKeys(m map[string]string) []string {
  keys := make([]string, len(m))
  for k, _ := range m {
    keys = append(keys, k)
  }
  return keys
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
