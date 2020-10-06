# ton-indexer

###
Usage:

```
-h|--help               prints help
-c|--config<arg>        global config
-d|--db<arg>            postgres db url
-t|--threads<arg>       worker thread count (default 4)
-m|--masterchain<arg>   masterchain worker count (default 10)
-w|--workchain<arg>     workchain worker count (default 10)
```

###
How to build:

```
cmake -DCMAKE_BULD_TYPE=Release ..
cmake --build . --target ton-indexer -- -j8
```
