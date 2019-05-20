<?php


	namespace MehrIt\SystemBroadcast;


//	class Daemon
//	{
//
//		public function start() {
//
//			// we don't care for our children, so we tell the system not to create zombies
//			pcntl_signal(SIGCHLD, SIG_IGN);
//
//			// react to end signals
//			pcntl_signal(SIGTERM, [$this, 'onSignal']);
//			pcntl_signal(SIGHUP, [$this, 'onSignal']);
//			pcntl_signal(SIGINT, [$this, 'onSignal']);
//
//			$this->client->subscribe([$this->channel], [$this, 'receive']);
//
//		}
//
//		public function receive($redis, $channel, $message) {
//
//			$sp      = explode(':', $message, 2);
//			$message = $sp[0];
//			$data    = $sp[1] ?? null;
//
//
//			$cmd = "{$this->handlersDir}/$message";
//
//
//			$pid = pcntl_fork();
//			if ($pid == -1) {
//				die('Could not fork');
//			}
//			else if ($pid) {
//				// nothing to do for the parent
//			}
//			else {
//				// Wir sind das Kind
//
//				$descriptorspec = array(
//					0 => array("pipe", "r"),  // stdin is a pipe that the child will read from
//					1 => array("pipe", "w"),  // stdout is a pipe that the child will write to
//					2 => null,
//				);
//
//				$process = proc_open($cmd, $descriptorspec, $pipes);
//
//				if (is_resource($process)) {
//					// $pipes now looks like this:
//					// 0 => writeable handle connected to child stdin
//					// 1 => readable handle connected to child stdout
//
//					fwrite($pipes[0], stream_get_contents(STDIN)); // file_get_contents('php://stdin')
//					fclose($pipes[0]);
//
//					$pdf_content = stream_get_contents($pipes[1]);
//					fclose($pipes[1]);
//
//					// It is important that you close any pipes before calling
//					// proc_close in order to avoid a deadlock
//					$return_value = proc_close($process);
//
//
//					header('Content-type: application/pdf');
//					header('Content-Disposition: attachment; filename="output.pdf"');
//					echo $pdf_content;
//				}
//			}
//
//
//		}
//
//		public function onSignal($sig) {
//
//			$sigName = '';
//			switch ($sig) {
//				case SIGINT:
//					$sigName = 'SIGINT';
//					break;
//				case SIGHUP:
//					$sigName = 'SIGHUP';
//					break;
//				case SIGTERM:
//					$sigName = 'SIGTERM';
//					break;
//			}
//
//			echo "Received signal $sig" . ($sigName ? "($sigName)" : '') . ', exiting';
//
//			// simply exit the process
//			exit();
//		}
//
//	}