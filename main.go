package main

import (
  "flag"
  "fmt"
  "io/ioutil"
  "log"
  "os"
  "strings"
  "text/template"

  "github.com/hashicorp/consul/api"
)

var prefix = flag.String("prefix", "", "Prefix for Consul keys")

func main () {
  flag.Parse()
  if flag.NArg() != 1 {
    log.Fatalf("Got %d arguments, expected 1", flag.NArg())
  }

  template_path := flag.Arg(0)
  template, err := slurp(template_path)
  if err != nil {
    log.Fatal(err)
  }

  err = process_template(template, prefix)
  if err != nil {
    log.Fatalf("Error processing template %s: %v", template_path, err)
  }
}

func slurp(path string) (*string, error) {
  file, err := os.Open(path)
  if err != nil {
    return nil, fmt.Errorf("Open %s: %v", path, err)
  }
  defer file.Close()
  data, err := ioutil.ReadAll(file)
  if err != nil {
    return nil, fmt.Errorf("Read %s: %v", path, err)
  }
  result := string(data)
  return &result, nil
}

func process_template(template_content *string, prefix *string) error {
  client, err := api.NewClient(api.DefaultConfig())
  if err != nil {
    return fmt.Errorf("Constructing Consul client with default config: %v", err)
  }
  kv_client := client.KV()

  funcMap := template.FuncMap{
    "join": join,
    "quote": enquote,
    "env": env,
    "cv": cv(kv_client, *prefix),
    "qcv": qcv(kv_client, *prefix),
    "cvl": cvl(kv_client, *prefix),
    "qcvl": qcvl(kv_client, *prefix),
  }
  tmpl, err := template.New("template").
    Option("missingkey=error").
    Funcs(funcMap).
    Parse(*template_content)
  if err != nil {
    return fmt.Errorf("Error parsing template: %v", err)
  }
  err = tmpl.Execute(os.Stdout, make(map[string]string))
  return err
}

// Helper functinos for use in templates

func env(varname string) (string, error) {
  v := os.Getenv(varname)
  if len(v) == 0 {
    return "", fmt.Errorf("Environment variable %s not set", varname)
  }
  return v, nil
}

func join(sep string, xs []string) string {
  return strings.Join(xs, sep)
}

func enquote(s string) string {
  return fmt.Sprintf("\"%s\"", s)
}

func cv (client *api.KV, prefix string) func(string) (string, error) {
  return func(k string) (string, error) {
    return get_consul_value(client, prefix+k)
  }
}

func qcv (client *api.KV, prefix string) func(string) (string, error) {
  return func(k string) (string, error) {
    v, err := get_consul_value(client, prefix+k)
    if err != nil {
      return v, err
    }
    return enquote(v), nil
  }
}

func cvl(client *api.KV, prefix string) func(string) ([]string, error) {
  return func(k string) ([]string, error) {
    return get_consul_values(client, prefix+k)
  }
}

func qcvl(client *api.KV, prefix string) func(string) ([]string, error) {
  return func(k string) ([]string, error) {
    vals, err := get_consul_values(client, prefix+k)
    if err != nil {
      return nil, err
    }
    result := make([]string, len(vals), len(vals))
    for i, v := range vals {
      result[i] = enquote(v)
    }
    return result, nil
  }
}

func get_consul_value(client *api.KV, key string) (string, error) {
  kv, _, err := client.Get(key, nil)
  if err != nil {
    return "", err
  }
  if kv == nil {
    return "", fmt.Errorf("Key %s not found in Consul", key)
  }
  return string(kv.Value), nil
}

func get_consul_values(client *api.KV, prefix string) ([]string, error) {
  kvs, _, err := client.List(prefix, nil)
  if err != nil {
    return nil, err
  }
  if len(kvs) == 0 {
    return nil, fmt.Errorf("Prefix %s not found in Consul", prefix)
  }
  result := make([]string, len(kvs), len(kvs))
  for i, kv := range kvs {
    result[i] = string(kv.Value)
  }
  return result, nil
}
