```golang
package main

import (
        "flag"
        "fmt"
        "gossip"
        "log"
        "time"
)

func main() {
        addr := flag.String("bind", "", "address to bind")
        region := flag.String("region", "default", "region")
        seed := flag.String("seed", "", "where to seed from")

        flag.Parse()

        g, err := gossip.NewGossiper(gossip.GossiperConfig{Address: *addr,
                Region:   *region,
                Seed:     *seed,
                Fanout:   50,
                Interval: 2 * time.Second})
        if err != nil {
                log.Fatal(err)
        }

        g.Start()

        for {
                fmt.Println(g.Status())
                time.Sleep(time.Second)
        }
}
```
