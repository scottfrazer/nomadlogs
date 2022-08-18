# nomadlogs

Install with `go install`

Then, run `nomadlogs ls` to list instances

Run `nomadlogs tail <task>` to tail all logs from all instances of `<task>`

By default it uses `localhost:4646`.  To override this, set `NOMAD_ADDR=hostname:port`
