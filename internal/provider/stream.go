package provider

// StreamReader is returned by Provider.ChatCompletionStream and yields
// the incremental chunks of a streaming chat completion.
type StreamReader interface {
	// Recv blocks until the next StreamChunk is available. It returns
	// io.EOF once the stream has been fully consumed, and any other
	// error if the stream fails before completing. After an error
	// (including io.EOF), subsequent calls to Recv must continue to
	// return that error.
	Recv() (*StreamChunk, error)

	// Close releases any resources held by the stream (e.g. the
	// underlying HTTP response body). It is safe to call Close before
	// the stream is fully consumed, and safe to call it more than once.
	Close() error
}

// StreamChunk is one incremental piece of a streaming chat completion.
type StreamChunk struct {
	// Delta is the incremental content produced since the previous
	// chunk. It is empty on chunks that carry no new content (e.g. a
	// final chunk that only reports FinishReason or Usage).
	Delta string
	// FinishReason indicates why generation stopped (e.g. "stop",
	// "length"), as normalized/reported by the provider. It is empty
	// on every chunk except the final one.
	FinishReason string
	// Usage reports token accounting for the request. It is nil on
	// every chunk except, for providers that report it, the final one.
	Usage *Usage
}
