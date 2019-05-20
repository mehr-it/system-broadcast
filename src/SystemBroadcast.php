<?php


	namespace MehrIt\SystemBroadcast;


	use InvalidArgumentException;
	use MehrIt\SystemBroadcast\Exception\StopException;
	use MehrIt\SystemBroadcast\Exception\SystemBroadcastUnhandledException;
	use Predis\Client;
	use Predis\CommunicationException;
	use RuntimeException;


	class SystemBroadcast
	{
		/**
		 * @var Client
		 */
		protected $client;


		/**
		 * @var array
		 */
		protected $clientConfig;

		/**
		 * @var string
		 */
		protected $channel;


		/**
		 * @var callable[]
		 */
		protected $handlers = [];

		/**
		 * @var callable
		 */
		protected $onListenHandlers = [];


		/**
		 * @var string|null
		 */
		protected $signKey = null;

		/**
		 * @var string|null
		 */
		protected $signKeyName = null;

		/**
		 * @var string
		 */
		protected $signAlgorithm = 'sha256';

		/**
		 * @var string[]
		 */
		protected $verificationKeys = [];

		/**
		 * @var array
		 */
		protected $verificationAlgorithms = [];

		/**
		 * @var int
		 */
		protected $timeTolerance =  3600;


		/**
		 * @var bool
		 */
		protected $stopped = false;

		/**
		 * @var string
		 */
		protected $sequencePrefix = 'system_broadcast_sequence_';

		/**
		 * Creates a new instance
		 * @param array $clientConfig The redis client configuration
		 * @param string $channel The redis channel used for broadcasting
		 */
		public function __construct(array $clientConfig, string $channel) {
			$this->clientConfig = $clientConfig;
			$this->channel      = $channel;
		}/** @noinspection PhpDocMissingThrowsInspection */


		/**
		 * Publishes a new message
		 * @param string $message The message
		 * @param mixed|null $data Additional message data. Will be JSON encoded for transfer and therefore must be a valid value for json_encode()
		 * @return SystemBroadcast This instance
		 */
		public function publish(string $message, $data = null) : SystemBroadcast {

			if (strpos($message, ':') !== false)
				throw new InvalidArgumentException('Message must not contain colons');

			// encode data
			$dataEncoded = '';
			if ($data !== null) {
				$dataEncoded = @json_encode($data);
				if (json_last_error())
					throw new RuntimeException('JSON encode failed: ' . json_last_error_msg());
			}

			// sign
			$signature = '';
			if ($this->signKey) {
				$keyName   = $this->signKeyName();
				$timestamp = time();
				$signature = "{$this->signAlgorithm}|{$keyName}|{$timestamp}|" . hash_hmac($this->signAlgorithm, $this->stringToSign($message, $dataEncoded, $timestamp), $this->signKey);
			}

			// publish message and increment sequence
			/** @noinspection PhpUnhandledExceptionInspection */
			$this->getClient()->transaction()
				->publish($this->channel, "{$message}:{$signature}:{$dataEncoded}")
				->incr($this->sequenceName())
				->execute();

			return $this;
		}

		/**
		 * Gets the current broadcast sequence id
		 * @return string The sequence id
		 */
		public function sequence() {
			return $this->getClient()->get($this->sequenceName());
		}/** @noinspection PhpDocMissingThrowsInspection */

		/**
		 * Starts the listening for new messages. Listens until handler throws an exception or timeout exceeds
		 * @param int $timeout The timeout. 0 means endless
		 * @return SystemBroadcast This instance
		 */
		public function listen(int $timeout = 0) : SystemBroadcast {

			if ($timeout < 0)
				throw new InvalidArgumentException('Timeout must not be negative');

			$this->stopped = false;

			$ts = time();

			$client = $this->createClient([
				'read_write_timeout' => ($timeout ? $timeout : -1),
				'timeout'            => 0,
			]);


			$pubSub = null;
			try {


				$pubSub = $client->pubSubLoop();
				$pubSub->subscribe($this->channel);

				foreach ($pubSub as $message) {
					/** @var object $message */
					switch ($message->kind) {
						case 'subscribe':

							// notify with sequence number we started listening at
							$seq = $this->sequence();
							foreach($this->onListenHandlers as $currCallback) {
								call_user_func($currCallback, $seq);
							}

							break;

						case 'message':
							$this->onReceive($message->payload);
							break;
					}

					// check if stopped
					if ($this->stopped)
						$pubSub->unsubscribe();
				}
			}
			catch (StopException $ex) {
			}
			/** @noinspection PhpRedundantCatchClauseInspection */
			catch(CommunicationException $commEx) {

				// only throw exception if exception not caused by end of timeout
				if ($timeout == 0 || time() - $ts < $timeout - 1) {
					/** @noinspection PhpUnhandledExceptionInspection */
					throw $commEx;
				}
			}
			finally {
				unset($pubSub);
			}

			return $this;
		}


		/**
		 * Adds a new message handler
		 * @param string $message
		 * @param callable $handler
		 * @return SystemBroadcast This instance
		 */
		public function handler(string $message, callable $handler) : SystemBroadcast {

			$this->handlers[$message] = $handler;

			return $this;
		}

		/**
		 * Adds a callback which is called right after listening for broadcast messages was init. The broadcast sequence id at it's state right after connect is passed as parameter
		 * so the user may check if messages were missed meanwhile
		 * @param callable $callback The callback
		 * @return SystemBroadcast This instance
		 */
		public function onListen(callable $callback) : SystemBroadcast {

			$this->onListenHandlers[] = $callback;

			return $this;
		}

		/**
		 * Returns if broadcasting client uses message signatures
		 * @return bool True if signatures are used. Else false.
		 */
		public function usesSignatures() {
			return $this->signKey !== null;
		}

		/**
		 * Stops listening
		 * @return $this
		 */
		public function stop() {
			$this->stopped = true;

			return $this;
		}

		/**
		 * @return string
		 */
		public function getSequencePrefix(): string {
			return $this->sequencePrefix;
		}

		/**
		 * @param string $sequencePrefix
		 * @return SystemBroadcast
		 */
		public function setSequencePrefix(string $sequencePrefix): SystemBroadcast {
			$this->sequencePrefix = $sequencePrefix;

			return $this;
		}



		/**
		 * Gets the sign key
		 * @return string|null The sign key
		 */
		public function getSignKey(): ?string {
			return $this->signKey;
		}

		/**
		 * Sets the sign key
		 * @param string|null $signKey The sign key Null if not to use signing
		 * @return SystemBroadcast This instance
		 */
		public function setSignKey(?string $signKey): SystemBroadcast {
			$this->signKey = $signKey;

			return $this;
		}

		/**
		 * Gets the sign key name
		 * @return string|null The sign key name
		 */
		public function getSignKeyName() : ?string {
			return $this->signKeyName;
		}

		/**
		 * Sets the sign key name
		 * @param string null $signKeyName The sign key name. Null to use 'default'
		 * @return SystemBroadcast This instance
		 */
		public function setSignKeyName(?string $signKeyName) : SystemBroadcast {
			$this->signKeyName = $signKeyName;

			return $this;
		}

		/**
		 * Gets the algorithm used for signing
		 * @return string The algorithm name
		 */
		public function getSignAlgorithm(): string {
			return $this->signAlgorithm;
		}

		/**
		 * Sets the algorithm used for signing
		 * @param string $signAlgorithm The algorithm name. Supported values may be retrieved using hash_hmac_algos()
		 * @return SystemBroadcast This instance
		 */
		public function setSignAlgorithm(string $signAlgorithm): SystemBroadcast {
			$this->signAlgorithm = $signAlgorithm;

			return $this;
		}

		/**
		 * Gets the maximum time tolerance for signed messages
		 * @return int The time tolerance in seconds
		 */
		public function getTimeTolerance(): int {
			return $this->timeTolerance;
		}

		/**
		 * Sets the maximum time tolerance for signed messages
		 * @param int $timeTolerance The maximum time tolerance for signed messages
		 * @return SystemBroadcast This instance
		 */
		public function setTimeTolerance(int $timeTolerance): SystemBroadcast {
			if ($timeTolerance <= 0)
				throw new InvalidArgumentException('Time tolerance must be greater than 0');

			$this->timeTolerance = $timeTolerance;

			return $this;
		}

		/**
		 * Gets the redis client configuration
		 * @return array The redis client configuration
		 */
		public function getClientConfig(): array {
			return $this->clientConfig;
		}

		/**
		 * Gets the redis channel used
		 * @return string The redis channel used
		 */
		public function getChannel(): string {
			return $this->channel;
		}

		/**
		 * Gets the additional keys allowed for signature verification
		 * @return string[] The additional keys allowed for signature verification
		 */
		public function getVerificationKeys(): array {
			return $this->verificationKeys;
		}

		/**
		 * Sets the additional keys allowed for signature verification
		 * @param string[] $verificationKeys The additional keys allowed for signature verification
		 * @return SystemBroadcast This instance
		 */
		public function setVerificationKeys(array $verificationKeys): SystemBroadcast {
			$this->verificationKeys = $verificationKeys;

			return $this;
		}

		/**
		 * Gets the additional hash algorithms allowed for signature verification
		 * @return string[] The additional hash algorithms allowed for signature verification
		 */
		public function getVerificationAlgorithms(): array {
			return $this->verificationAlgorithms;
		}

		/**
		 * Sets the additional hash algorithms allowed for signature verification
		 * @param string[] $verificationAlgorithms The additional hash algorithms allowed for signature verification
		 * @return SystemBroadcast This instance
		 */
		public function setVerificationAlgorithms(array $verificationAlgorithms): SystemBroadcast {
			$this->verificationAlgorithms = $verificationAlgorithms;

			return $this;
		}

		/**
		 * Handles a message receive
		 * @param string $payload The message
		 */
		protected function onReceive(string $payload) {

			$sp = explode(':', $payload, 3);

			// extract handler
			$message = $sp[0];
			$handler = $this->handlers[$message] ?? null;
			if (!$handler)
				throw new SystemBroadcastUnhandledException("No handler for system broadcast message \"$message\" registered");

			// extract data
			$dataEncoded = $sp[2] ?? '';
			$data        = null;
			if ($dataEncoded != '') {
				$data = @json_decode($dataEncoded, true);
				if (json_last_error())
					throw new RuntimeException('JSON decode failed: ' . json_last_error_msg());
			}

			// check signature
			$signatureData = $sp[1] ?? null;
			if ($signatureData) {
				$spSig = explode('|', $signatureData, 4);

				// check algorithm
				$algorithm = $spSig[0] ?? null;
				if (!in_array($algorithm, $this->verificationAlgorithms()))
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" with invalid signature algorithm \"$algorithm\"");

				// get secret
				$keyName = $spSig[1] ?? null;
				if (!in_array($algorithm, $this->verificationAlgorithms()))
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" without signature key name");
				$secret = $this->verificationKeys()[$keyName] ?? null;
				if (!$secret)
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" with unknown signature key \"$keyName\"");

				// get timestamp
				$timestamp = $spSig[2] ?? 0;
				if (!$timestamp || !is_numeric($timestamp))
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" with invalid timestamp \"$timestamp\"");
				$now      = time();
				$timeDiff = abs($now - $timestamp);
				if ($timeDiff > $this->timeTolerance)
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" with timestamp \"$timestamp\" exceeding the maximum tolerance of \"{$this->timeTolerance}\". Current system time is $now.");


				// verify signature
				$signature = $spSig[3];
				if (!$signature)
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" with missing signature");
				$expectedSign = hash_hmac($algorithm, $this->stringToSign($message, $dataEncoded, $timestamp), $secret);
				if (!hash_equals($expectedSign, $signature))
					throw new SystemBroadcastUnhandledException("Refused handling system broadcast message \"$message\" with invalid signature \"$signature\". Valid signature would be \"$expectedSign\"");

			}
			else if ($this->usesSignatures()) {
				throw new SystemBroadcastUnhandledException("Refused handling unauthenticated system broadcast message \"$message\"");
			}


			call_user_func($handler, $data, $this, $message);
		}

		protected function createClient(array $overrides = []) {
			$config   = array_merge($this->clientConfig, array_filter($overrides, function($v) {
				return $v !== null;
			}));

			if (empty($config['scheme']))
				$config['scheme'] = 'tcp';
			if (empty($config['port']))
				$config['port'] = 6379;

			if (empty($config['host']))
				throw new RuntimeException('Redis host not configured');

			return new Client($config);
		}

		/**
		 * Gets the Redis client instance to use
		 * @return Client
		 */
		protected function getClient() {

			if (!$this->client) {

				$client = $this->createClient();

				return $this->client = $client;
			}

			return $this->client;

		}


		/**
		 * Gets the string to sing
		 * @param string $message The message
		 * @param string $encodedData The encoded data
		 * @param int $timestamp The timestamp
		 * @return string The string to sign
		 */
		protected function stringToSign(string $message, string $encodedData, int $timestamp) {
			return "{$message}:{$timestamp}:{$encodedData}";
		}

		/**
		 * Gets the sign key name using default value
		 * @return string The sing key name
		 */
		protected function signKeyName() {
			return $this->signKeyName ?: 'default';
		}


		/**
		 * Gets all verification keys
		 * @return string[] The verification keys
		 */
		protected function verificationKeys() : array {

			$ret = $this->verificationKeys;

			if ($this->usesSignatures())
				$ret[$this->signKeyName()] = $this->signKey;

			return $ret;
		}

		/**
		 * Gets the allowed algorithms for signature verification
		 * @return string[] The algorithms
		 */
		protected function verificationAlgorithms() : array {

			$ret = $this->verificationAlgorithms;

			$ret[] = $this->signAlgorithm;

			return array_unique($ret);
		}

		protected function sequenceName() : string {
			return "{$this->sequencePrefix}{$this->channel}";
		}

	}