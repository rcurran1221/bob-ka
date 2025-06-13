project name:
boblog or bob-ka or bob-queue

project goals:

v1
Http endpoint that enables a caller to poll for realtime data updates
	use kafka consumer pattern as a "pattern" for the apis
	consume, ack, subscribe(?)
	server keeps track of consumer state
	messages live in memory, when server process ends, message queue is cleared
		eg: compliance is restarted, there is no obligation to rebuild message queue
	messages are "produced" from existing process, eg. notification, event from other part of system
		assume process has message in event form in memory, and it is added to queue by an independent thread
		allow for test "producer", that produces X events every N seconds
	retention of in memory queue is configurable, time based trim on write - similar to eventstream pattern

future
back queue with disk persistence
	if process serving messages fails, it will rebuild queue from disk
process exposes a "producer" api, modeling after kafka producer
