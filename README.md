# Mock iot devices for iot-device-manager

Connects to mochi mqtt server and publishes data in `json` in the form for 30 seconds:

Publishes to the topic: `assets/:id` where `id` is a uint from 1 to the max specified devices

```
{
    "telemetry": json object # any data
    "status": string #status string, just "online"
}
```

### Usage

- clone the repo
- create `.env` file according to [example](.env.example) (All variables required)
- run `go run .` in repo's root directory
- end with `ctrl+c`
