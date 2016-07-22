<!--
Copyright (C) Satoshi Konno 2016

This is licensed under BSD-style license, see file COPYING.
-->

# Fractal Binding

```
mvn -pl com.yahoo.ycsb:fractal-binding -am clean package
```

## Fractal Configuration Parameters

- `host` (**required**)
  - No default.

* `port`
  * Default is `38400`.

- `methods`
  - method names for regstry.
  * Default is `set_registry,get_registry,remove_registry`.
