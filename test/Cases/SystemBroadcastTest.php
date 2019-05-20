<?php


	namespace MehrItSystemBroadcastTest\Cases;



	use MehrIt\SystemBroadcast\Exception\StopException;
	use MehrIt\SystemBroadcast\Exception\SystemBroadcastUnhandledException;
	use MehrIt\SystemBroadcast\SystemBroadcast;
	use PHPUnit\Framework\TestCase;
	use RuntimeException;
	use Spatie\Async\Pool;


	class SystemBroadcastTest extends TestCase
	{


		protected function createClient() {

			$configFile = __DIR__ . '/../../test.ini';

			$config = parse_ini_file($configFile);
			if (!$config)
				throw new RuntimeException('Error parsing config file "' . $configFile . '"');

			$redisConfig = [
				'host' => $config['REDIS_HOST'] ?? null,
				'port' => $config['REDIS_PORT'] ?? null,
			];

			return new SystemBroadcast($redisConfig, $config['REDIS_CHANNEL']);
		}

		public function testOnListen() {

			$client = $this->createClient();

			$sequenceBefore = $client->sequence();
			$recSequence    = null;


			$this->assertSame($client, $client->onListen(function ($sequence) use (&$recSequence) {
				$recSequence = $sequence;
			}));

			$client->listen(1);

			$this->assertSame($sequenceBefore, $recSequence);

		}

		public function testOnListen_multipleCallbacks() {

			$client = $this->createClient();

			$sequenceBefore = $client->sequence();
			$recSequence1   = null;
			$recSequence2   = null;


			$client
				->onListen(function ($sequence) use (&$recSequence1) {
					$recSequence1 = $sequence;
				})
				->onListen(function ($sequence) use (&$recSequence2) {
					$recSequence2 = $sequence;
				});

			$client->listen(1);

			$this->assertSame($sequenceBefore, $recSequence1);
			$this->assertSame($sequenceBefore, $recSequence2);

		}

		public function testPublish() {

			$client = $this->createClient();

			$sequenceBefore = $client->sequence();

			$this->assertSame($client, $client->publish('msg1'));
			$this->assertEquals($sequenceBefore + 1, $client->sequence());


			$this->assertSame($client, $client->publish('msg2'));
			$this->assertEquals($sequenceBefore + 2, $client->sequence());
		}


		public function testPublishListen() {


			$pool = Pool::create();


			$pool
				->add(function () {
					$client = $this->createClient();

					$received      = [];
					$sequenceAfter = null;
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received, &$sequenceAfter, $client) {
						$received[$message][] = $data;
						$sequenceAfter        = $client->sequence();
					});
					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return [
						$received,
						$sequenceAfter,
					];
				})
				->catch(function ($ex) {

					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);

					$sequenceBefore = $this->createClient()->sequence();

					$this->createClient()
						->publish('test-msg')
						->publish('exit');

					return $sequenceBefore;
				})
				->catch(function ($ex) {
					throw($ex);
				});

			$res = $pool->wait();

			$results = array_values(array_filter($res, 'is_array'));
			$this->assertSame(['test-msg'], array_keys($results[0][0]));

			$seq = array_values(array_filter($res, 'is_string'));

			$this->assertEquals($seq[0] + 1, $results[0][1]);

		}

		public function testPublishListen_withPayload() {


			$pool = Pool::create();


			$pool
				->add(function () {
					$client = $this->createClient();

					$received      = [];
					$sequenceAfter = null;
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received, &$sequenceAfter, $client) {
						$received[$message][] = $data;
						$sequenceAfter        = $client->sequence();
					});
					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return [
						$received,
						$sequenceAfter,
					];
				})
				->catch(function ($ex) {

					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);

					$sequenceBefore = $this->createClient()->sequence();

					$this->createClient()
						->publish('test-msg', ['a' => 17, 'b' => true])
						->publish('exit');

					return $sequenceBefore;
				})
				->catch(function ($ex) {
					throw($ex);
				});

			$res = $pool->wait();

			$results = array_values(array_filter($res, 'is_array'));
			$this->assertSame(['test-msg' => [['a' => 17, 'b' => true]]], $results[0][0]);

			$seq = array_values(array_filter($res, 'is_string'));

			$this->assertEquals($seq[0] + 1, $results[0][1]);

		}

		public function testPublishListen_multipleListeners() {


			$pool = Pool::create();

			$pool
				->add(function () {
					$client = $this->createClient();

					$received = [];
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;
					});
					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {
					throw($ex);
				});

			$pool
				->add(function () {
					$received = [];
					$client   = $this->createClient();

					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;
					});

					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {
					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);

					$this->createClient()
						->publish('test-msg')
						->publish('exit');
				})
				->catch(function ($ex) {
					throw($ex);
				});

			$results = array_values(array_filter($pool->wait(), 'is_array'));
			$this->assertSame(['test-msg'], array_keys($results[0]));
			$this->assertSame(['test-msg'], array_keys($results[1]));

		}

		public function testPublishListen_multiplePublishers() {

			$pool = Pool::create();

			$pool
				->add(function () {
					$client = $this->createClient();

					$received = [];
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;

						if (count($received) == 2)
							throw new StopException();

					});
					$client->handler('test-msg2', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;

						if (count($received) == 2)
							throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {
					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);

					$this->createClient()->publish('test-msg');
				})
				->catch(function ($ex) {
					throw($ex);
				});

			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);

					$this->createClient()->publish('test-msg2');
				})
				->catch(function ($ex) {
					throw($ex);
				});


			$results = array_values(array_filter($pool->wait(), 'is_array'));

			$messages = array_keys($results[0]);
			sort($messages);
			$this->assertSame(['test-msg', 'test-msg2'], $messages);

		}


		public function testPublishListen_signed() {


			$pool = Pool::create();

			$pool
				->add(function () {
					$client = $this->createClient();
					$client->setSignKey('my-secret');

					$received = [];
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;
					});
					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {

					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);


					$this->createClient()
						->setSignKey('my-secret')
						->publish('test-msg')
						->publish('exit');

				})
				->catch(function ($ex) {
					throw($ex);
				});

			$res = $pool->wait();

			$results = array_values(array_filter($res, 'is_array'));
			$this->assertSame(['test-msg'], array_keys($results[0]));


		}

		public function testPublishListen_signed_multipleKeys() {


			$pool = Pool::create();

			$pool
				->add(function () {
					$client = $this->createClient();
					$client->setSignKey('my-secret');
					$client->setVerificationKeys(['other-key' => 'other-secret']);

					$received = [];
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;

						if (count($received) == 2)
							throw new StopException();
					});
					$client->handler('test-msg2', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;

						if (count($received) == 2)
							throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {

					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);


					$this->createClient()
						->setSignKey('my-secret')
						->publish('test-msg');

				})
				->catch(function ($ex) {
					throw($ex);
				});

			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);


					$this->createClient()
						->setSignKeyName('other-key')
						->setSignKey('other-secret')
						->publish('test-msg2');

				})
				->catch(function ($ex) {
					throw($ex);
				});

			$res = $pool->wait();

			$results = array_values(array_filter($res, 'is_array'));
			$keys = array_keys($results[0]);
			sort($keys);
			$this->assertSame(['test-msg', 'test-msg2'], $keys);


		}


		public function testPublishListen_signed_invalidSignature() {


			$pool = Pool::create();

			$pool
				->add(function () {
					$client = $this->createClient();
					$client->setSignKey('other-secret');

					$received = [];
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;
					});
					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {

					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);


					$this->createClient()
						->setSignKey('my-secret')
						->publish('test-msg')
						->publish('exit');

				})
				->catch(function ($ex) {
					throw($ex);
				});

			$this->expectException(SystemBroadcastUnhandledException::class);

			$pool->wait();


		}

		public function testPublishListen_signed_noSignature() {


			$pool = Pool::create();

			$pool
				->add(function () {
					$client = $this->createClient();
					$client->setSignKey('my-secret');

					$received = [];
					$client->handler('test-msg', function ($data, $bc, $message) use (&$received) {
						$received[$message][] = $data;
					});
					$client->handler('exit', function () use (&$received) {
						throw new StopException();
					});

					$client->listen(2);

					return $received;
				})
				->catch(function ($ex) {

					throw($ex);
				});


			$pool
				->add(function () {
					// wait for listeners to become ready
					sleep(1);


					$this->createClient()
						->publish('test-msg')
						->publish('exit');

				})
				->catch(function ($ex) {
					throw($ex);
				});

			$this->expectException(SystemBroadcastUnhandledException::class);

			$pool->wait();


		}
	}

