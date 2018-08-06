package ginmm

import (
	"bytes"
	"github.com/gin-gonic/gin"
	"log"
	"math"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	RESPONSE_CHANEL_BUFFER = 1024
	DEFAULT_BUFFER_SIZE    = 400
	RECONNECT_TIME_OUT     = time.Second * 10
	DEFAULT_FLUSH_TIMEOUT  = time.Second
	RAW_API_REQUEST        = "raw-api-requests-"
	REQUEST_NAME           = ",request_name="
	RESPONSE_NAME          = ",response_name="
	STATUS                 = ",error_code="
	TIMING                 = " timing="
	SLA                    = "sla-"
	CUSTOM_SLA_METRIC      = ",metric=alive"
	CUSTOM_SLA_VALUE       = " value=1.0 "
	ALIVE_PERIOD           = time.Second * 10
)

type responseInfo struct {
	status   int64
	duration time.Duration
	method   string
	url      *url.URL
}

type MetricParams struct {
	Service         string
	UdpAddres       string
	FlushTimeout    time.Duration
	FlushBufferSize int
}

type MetricMiddleware struct {
	metricChanel chan responseInfo
	params       MetricParams
}

func (m *MetricMiddleware) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {

		t := time.Now()
		c.Next()

		r := responseInfo{
			status:   int64(c.Writer.Status()),
			duration: time.Since(t),
			method:   c.Request.Method,
			url:      c.Request.URL,
		}

		m.metricChanel <- r
	}
}

func NewMetricMiddleware(params MetricParams) *MetricMiddleware {
	mch := make(chan responseInfo, RESPONSE_CHANEL_BUFFER)

	var wg sync.WaitGroup
	wg.Add(1)
	go metricLoop(params, mch, &wg)
	wg.Wait()

	return &MetricMiddleware{
		metricChanel: mch,
		params:       params,
	}
}

func metricLoop(params MetricParams, dataChanel <-chan responseInfo, wg *sync.WaitGroup) {

	sender := NewSender(params)
	wg.Done()

	sender.Connect()
	tick := time.NewTicker(params.FlushTimeout)
	alive := time.NewTicker(ALIVE_PERIOD)

	for {
		select {
		case data, ok := <-dataChanel:
			if !ok {
				//Chanel is closed. Exit from gorutine
				return
			}

			err := sender.Send(data)
			if err != nil {
				sender.Connect()
			}

		case <-tick.C:
			err := sender.Flush()
			if err != nil {
				sender.Connect()
			}

		case <-alive.C:
			err := sender.Alive()
			if err != nil {
				sender.Connect()
			}
		}
	}
}

type MetricSender struct {
	conn      *net.UDPConn
	buffer    bytes.Buffer
	tmpBuffer bytes.Buffer
	params    MetricParams
	isError   bool
}

func NewSender(params MetricParams) *MetricSender {
	return &MetricSender{
		params: params,
	}
}

func (m *MetricSender) Send(data responseInfo) error {
	defer m.tmpBuffer.Reset()
	b := &m.tmpBuffer

	// "raw-api-requests-%s,request_name=%s,response_name=%s,error_code=%d timing=%.0f %d\n"
	_, err := b.WriteString(RAW_API_REQUEST)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(m.params.Service)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(REQUEST_NAME)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}
	{
		//write request name
		b.WriteString(data.method)
		b.WriteRune('-')
		b.WriteString(data.url.Path)
	}

	_, err = b.WriteString(RESPONSE_NAME)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}
	{
		//write respone name
		b.WriteString(data.method)
		b.WriteRune('-')
		b.WriteString(data.url.Path)
	}

	_, err = b.WriteString(STATUS)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(strconv.FormatInt(data.status, 10))

	_, err = b.WriteString(TIMING)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	timing := math.Ceil(float64(data.duration) / float64(time.Millisecond))
	_, err = b.WriteString(strconv.FormatFloat(timing, 'g', 3, 64))
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	b.WriteRune(' ')

	_, err = b.WriteString(strconv.FormatInt(makeTimestamp(), 10))
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteRune('\n')
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	m.buffer.Write(b.Bytes())

	if m.buffer.Len() >= m.params.FlushBufferSize {
		return m.Flush()
	}

	return nil
}

func (m *MetricSender) Quit() error {

	err := m.Flush()
	m.close()
	return err
}

func (m *MetricSender) Flush() error {

	var err error = nil
	if m.buffer.Len() != 0 {
		if m.conn != nil {
			_, err = m.buffer.WriteTo(m.conn)
		} else {
			log.Print("Error UDP connection is not exist")
			m.buffer.Reset()
		}
	}

	return err
}

func (m *MetricSender) close() {
	if m.conn != nil {
		m.conn.Close()
	}
}

func makeTimestamp() int64 {
	return time.Now().UnixNano()
}

func (m *MetricSender) Alive() error {
	defer m.tmpBuffer.Reset()
	b := &m.tmpBuffer

	//sla-%s,metric=alive value=1.0 1481537925000000000
	_, err := b.WriteString(SLA)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(m.params.Service)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(CUSTOM_SLA_METRIC)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(CUSTOM_SLA_VALUE)
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteString(strconv.FormatInt(makeTimestamp(), 10))
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	_, err = b.WriteRune('\n')
	if err != nil {
		log.Printf("Can't write in buffer: %s", err.Error())
		return err
	}

	m.buffer.Write(b.Bytes())
	m.Flush()
	return nil
}

func (m *MetricSender) Connect() error {
	log.Println("Connecting to telegraf")
	if m.isError {
		m.close()
	}

	m.isError = false
	dest, err := net.ResolveUDPAddr("udp", m.params.UdpAddres)

	if err != nil {
		log.Printf("Warning: can't resolve address %s: %s", m.params.UdpAddres, err.Error())
		m.isError = true
		return err
	}

	conn, err := net.DialUDP("udp", nil, dest)
	if err != nil {
		log.Printf("Warning: can't create udp dial for %s: %s", m.params.UdpAddres, err.Error())
		m.isError = true
		return err
	}
	log.Println("Connected to telegraf")

	m.conn = conn
	return nil
}
