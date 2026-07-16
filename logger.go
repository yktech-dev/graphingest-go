package graphingest

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// StreamingLogger buffers log entries and flushes them to the control plane in batches.
type StreamingLogger struct {
	flowRunID  string
	taskRunID  string
	client     *GraphIngestClient
	bufferSize int
	flushEvery time.Duration

	mu     sync.Mutex
	buffer []LogEntry
	stop   chan struct{}
	done   chan struct{}
}

// StreamingLoggerOpts configures the streaming logger.
type StreamingLoggerOpts struct {
	FlowRunID  string
	TaskRunID  string
	Client     *GraphIngestClient
	BufferSize int
	FlushEvery time.Duration
}

// NewStreamingLogger creates a new streaming logger that sends log batches to the control plane.
func NewStreamingLogger(opts StreamingLoggerOpts) *StreamingLogger {
	if opts.Client == nil {
		opts.Client = GetClient()
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 20
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 2 * time.Second
	}

	sl := &StreamingLogger{
		flowRunID:  opts.FlowRunID,
		taskRunID:  opts.TaskRunID,
		client:     opts.Client,
		bufferSize: opts.BufferSize,
		flushEvery: opts.FlushEvery,
		buffer:     make([]LogEntry, 0, opts.BufferSize),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}

	go sl.flushLoop()
	return sl
}

// Log adds a log entry at the specified level.
func (sl *StreamingLogger) Log(level, message string) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	entry := LogEntry{
		FlowRunID: sl.flowRunID,
		TaskRunID: sl.taskRunID,
		Level:     level,
		Message:   message,
		WorkerID:  os.Getenv("WORKER_ID"),
	}
	sl.buffer = append(sl.buffer, entry)

	if len(sl.buffer) >= sl.bufferSize {
		sl.doFlush()
	}
}

// Info logs at INFO level.
func (sl *StreamingLogger) Info(msg string) { sl.Log("INFO", msg) }

// Infof logs at INFO level with formatting.
func (sl *StreamingLogger) Infof(format string, args ...any) { sl.Log("INFO", fmt.Sprintf(format, args...)) }

// Warn logs at WARNING level.
func (sl *StreamingLogger) Warn(msg string) { sl.Log("WARNING", msg) }

// Warnf logs at WARNING level with formatting.
func (sl *StreamingLogger) Warnf(format string, args ...any) { sl.Log("WARNING", fmt.Sprintf(format, args...)) }

// Error logs at ERROR level.
func (sl *StreamingLogger) Error(msg string) { sl.Log("ERROR", msg) }

// Errorf logs at ERROR level with formatting.
func (sl *StreamingLogger) Errorf(format string, args ...any) { sl.Log("ERROR", fmt.Sprintf(format, args...)) }

// Debug logs at DEBUG level.
func (sl *StreamingLogger) Debug(msg string) { sl.Log("DEBUG", msg) }

// Debugf logs at DEBUG level with formatting.
func (sl *StreamingLogger) Debugf(format string, args ...any) { sl.Log("DEBUG", fmt.Sprintf(format, args...)) }

// Flush sends all buffered logs to the control plane.
func (sl *StreamingLogger) Flush() {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.doFlush()
}

// Close stops the background flusher and flushes remaining logs.
func (sl *StreamingLogger) Close() {
	close(sl.stop)
	<-sl.done
	sl.Flush()
}

func (sl *StreamingLogger) flushLoop() {
	defer close(sl.done)
	ticker := time.NewTicker(sl.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sl.Flush()
		case <-sl.stop:
			return
		}
	}
}

// doFlush sends buffered logs. Must be called under sl.mu lock.
func (sl *StreamingLogger) doFlush() {
	if len(sl.buffer) == 0 {
		return
	}
	batch := make([]LogEntry, len(sl.buffer))
	copy(batch, sl.buffer)
	sl.buffer = sl.buffer[:0]

	if err := sl.client.SendLogs(sl.flowRunID, batch); err != nil {
		log.Printf("[graphingest] failed to flush logs: %v", err)
	}
}
