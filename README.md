# System broadcast
This package implements internal broadcasting for distributed systems using Redis.

Internally Redis `publish`/`subscribe` commands are used. This library offers an easy
interface for publishing and receiving messages and provides a mechanism to check whether
some messages were missed while the broadcast channel was not subscribed.

## Basic usage

Implementing a publisher is very easy:

	$broadcast = new SystemBroadcast(['host' => '127.0.0.1']);
	$broadcast->publish('my-message');
	
	
The corresponding listener would look like this:

	$broadcast = new SystemBroadcast(['host' => '127.0.0.1']);
	$broadcast->handler('my-message', function() {
		// do something
	})
	$broadcast->listen();
	
	
### Passing data together with messages
You may pass any JSON serializable data together with messages:

	$broadcast->publish('my-message', $data);

The handler will receive the data as first parameter:
	
	$broadcast->handler('my-message', function($data) {
		// do something
	})
	
	
## Missed messages
While listening for broadcasts, listeners receive all messages. However if your listener
process is not running for whatever reason, all messages published meanwhile are
irrevocably lost for this client.

Even if you cannot recover missed messages, you can check if you missed any published
messages. Your handler can receive a sequence value and save it to a file:

	$broadcast->handler('my-message', function($data, $broadcast) {
		$seq = $broadcast->sequence();
		
		/* ... save it to file */
	})
	
Before you start listening, you can register an `onListen`-callback, which receives the
current sequence value right after listening is established. If this value does not equal
the last value stored by the handler, you  missed some messages:

	$broadcast->onListen(function($sequence) {
	
		$expected = /* ... load last sequence value from file */
		
		if ($sequence != $expected) {
			/* handle inconsitency */
		}
	
	});


## Signing messages
Since messages can be very critical, you may sign messages. To do so, use the `setSignKey`
method to set the same secret for publishers and listeners.

See class documentation for additional signing parameters.


