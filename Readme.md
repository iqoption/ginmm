# GIN Monitoring Middlewares 

It is middleware for send metrics about API to Telegraf

## Message format

    raw-api-requests-updates,request_name=GET-/api/v1/info,response_name=GET-/api/v1/info,error_code=0,status=2XX timing=3 1498226123268
    raw-api-requests-updates,request_name=GET-/api/v1/info,response_name=GET-/api/v1/info,error_code=0,status=2XX timing=4 1498226123648

## Usage 

```go
    mm := ginmm.NewMetricMiddleware(MetricParams{
        Service:         "updates",
        UdpAddres:       "localhost:12345",
        FlushBufferSize: DEFAULT_BUFFER_SIZE,
        FlushTimeout:    DEFAULT_FLUSH_TIMEOUT,
    })
    
    // Creates a router without any middleware by default
    r := gin.New()
    
    // Setup global middleware
    r.Use(mm.Middleware())
    // ...
```