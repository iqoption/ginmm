package ginmm

import (
	"log"
	"net"
	"sync"
	"time"
	"math"
	"bytes"
	"net/url"
	"github.com/gin-gonic/gin"
	"github.com/go-errors/errors"
	"strconv"
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
	metricChanel  chan responseInfo
	errChanel     chan error
	isError       bool
	params        MetricParams
	lastReConnect time.Time
}

func (m *MetricMiddleware) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		m.check()

		if m.isError {
			c.Next()
			return
		}

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
	errch := make(chan error)

	var wg sync.WaitGroup
	wg.Add(1)
	go metricLoop(params, mch, errch, &wg)
	wg.Wait()

	return &MetricMiddleware{
		metricChanel: mch,
		errChanel:    errch,
		params:       params,
	}
}

func (m *MetricMiddleware) check() {
	if m.isError {
		m.reConnect()
		return
	}

	select {
	case err := <-m.errChanel:
		log.Print(err)
		m.isError = true

		close(m.errChanel)
		close(m.metricChanel)

		m.lastReConnect = time.Now()
	default:
	}
}

func (m *MetricMiddleware) reConnect() {
	if time.Since(m.lastReConnect) >= RECONNECT_TIME_OUT {
		m.lastReConnect = time.Now()

		log.Println("I am trying to connect...")
		m.metricChanel = make(chan responseInfo, RESPONSE_CHANEL_BUFFER)
		m.errChanel = make(chan error)
		var wg sync.WaitGroup
		wg.Add(1)
		go metricLoop(m.params, m.metricChanel, m.errChanel, &wg)
		wg.Wait()
		m.isError = false
	}
}

func metricLoop(params MetricParams, dataChanel <-chan responseInfo, errc chan<- error, wg *sync.WaitGroup) {

	sender, err := NewSender(params)
	wg.Done()
	if err != nil {
		errc <- err
		return
	}

	tick := time.NewTicker(params.FlushTimeout)

	for {
		select {
		case data, ok := <-dataChanel:
			if !ok {
				err = sender.Quit()
				return
			}

			err = sender.Send(data)
			if err != nil {
				errc <- err
				return
			}

		case <-tick.C:
			err = sender.Flush()
			if err != nil {
				errc <- err
				return
			}
		}
	}
}

type MetricSender struct {
	conn   *net.UDPConn
	buffer bytes.Buffer
	params MetricParams
}

func NewSender(params MetricParams) (*MetricSender, error) {
	dest, err := net.ResolveUDPAddr("udp", params.UdpAddres)
	if err != nil {
		return nil, errors.New(errors.Errorf("Warning: can't resolve address %s: %s", params.UdpAddres, err.Error()))
	}

	conn, err := net.DialUDP("udp", nil, dest)
	if err != nil {
		return nil, errors.New(errors.Errorf("Warning: can't create udp dial for %s: %s", params.UdpAddres, err.Error()))
	}

	return &MetricSender{
		conn:   conn,
		params: params,
	}, nil
}

func (m *MetricSender) Send(data responseInfo) error {

	var b bytes.Buffer
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
		b.WriteString(data.url.String())
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
		b.WriteString(data.url.String())
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
