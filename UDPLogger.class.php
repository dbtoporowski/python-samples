/**
 * Author: DavidT 
 * Description: Script to submit flash page view data via a UDP socket to a python data listening daemon.
 * The class is auto-loaded with the rest of the API
 */

class UDPLogger {
	var $host;
	var $port;
	var $socket;

	// php 4 constructor
	function UDPLogger($host, $port, $persist = FALSE) {
		//destructor
		register_shutdown_function(array(&$this, '__destruct'));

		//constructor
		$argcv = func_get_args();
		call_user_func_array(array(&$this, '__construct'), $argcv);
	}

	function __construct($host, $port, $persist = FALSE) {
		$this->host = $host;
		$this->port = $port;
		if (strlen($host) && $port > 0) {
			$this->socket = @socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
		}
	}
	
	function __destruct() {
		if (isset($this->socket)) {
			@socket_close($this->socket);
		}
	}
	
	function escape($field) {
		return
			str_replace("\t", "\\t",
				str_replace("\n", "\\n",
					str_replace("\r", "\\r",
						str_replace("\\", "\\\\", $field);
	}
	
	function send($data) {
		if (isset($this->socket) && isset($data)) {
			if (is_array($data)) {
				$out = "";
				$delimiter = "";
				foreach($data as $field) {
					$out .= $delimiter . $this->escape($field);
					$delimiter = "\t";
				}
			} else {
				$out = $this->escape($data);
			}
			if (strlen($out) {
				@socket_write($this->socket, $out);
			}
		}
	}
}
