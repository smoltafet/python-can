#!/usr/bin/env python

import os
import socket
import tempfile
import threading
import unittest

import can
from can.interfaces.socketcand import SocketCanDaemonBus, socketcand


class TestConvertAsciiMessageToCanMessage(unittest.TestCase):
    def test_valid_frame_message(self):
        # Example: < frame 123 1680000000.0 01020304 >
        ascii_msg = "< frame 123 1680000000.0 01020304 >"
        msg = socketcand.convert_ascii_message_to_can_message(ascii_msg)
        self.assertIsInstance(msg, can.Message)
        self.assertEqual(msg.arbitration_id, 0x123)
        self.assertEqual(msg.timestamp, 1680000000.0)
        self.assertEqual(msg.data, bytearray([1, 2, 3, 4]))
        self.assertEqual(msg.dlc, 4)
        self.assertFalse(msg.is_extended_id)
        self.assertTrue(msg.is_rx)

    def test_valid_error_message(self):
        # Example: < error 1ABCDEF0 1680000001.0 >
        ascii_msg = "< error 1ABCDEF0 1680000001.0 >"
        msg = socketcand.convert_ascii_message_to_can_message(ascii_msg)
        self.assertIsInstance(msg, can.Message)
        self.assertEqual(msg.arbitration_id, 0x1ABCDEF0)
        self.assertEqual(msg.timestamp, 1680000001.0)
        self.assertEqual(msg.data, bytearray([0]))
        self.assertEqual(msg.dlc, 1)
        self.assertTrue(msg.is_extended_id)
        self.assertTrue(msg.is_error_frame)
        self.assertTrue(msg.is_rx)

    def test_invalid_message(self):
        ascii_msg = "< unknown 123 0.0 >"
        msg = socketcand.convert_ascii_message_to_can_message(ascii_msg)
        self.assertIsNone(msg)

    def test_missing_ending_character(self):
        ascii_msg = "< frame 123 1680000000.0 01020304"
        msg = socketcand.convert_ascii_message_to_can_message(ascii_msg)
        self.assertIsNone(msg)


class FakeSocketcandServer(threading.Thread):
    """A minimal socketcand server speaking just enough of the protocol for
    the bus handshake, one outgoing frame and one incoming frame."""

    def __init__(self, server_socket):
        super().__init__(daemon=True)
        self._server_socket = server_socket
        self.received = []
        self.frame_to_send = "< frame 123 1680000000.0 01020304 >"
        self.error = None

    @staticmethod
    def _recv_message(conn, buffer):
        while ">" not in buffer:
            chunk = conn.recv(1024).decode("ascii")
            if not chunk:
                raise ConnectionError("client closed the connection")
            buffer += chunk
        message, _, buffer = buffer.partition(">")
        return message + ">", buffer

    def run(self):
        try:
            conn, _ = self._server_socket.accept()
            with conn:
                buffer = ""
                conn.sendall(b"< hi >")
                message, buffer = self._recv_message(conn, buffer)
                self.received.append(message)
                conn.sendall(b"< ok >")
                message, buffer = self._recv_message(conn, buffer)
                self.received.append(message)
                conn.sendall(b"< ok >")
                # record the frame the bus sends us
                message, buffer = self._recv_message(conn, buffer)
                self.received.append(message)
                # then push one frame back to the bus
                conn.sendall(self.frame_to_send.encode("ascii"))
                # keep the connection open until the client goes away
                while conn.recv(1024):
                    pass
        except (ConnectionError, OSError):
            pass
        except Exception as exc:
            self.error = exc
        finally:
            self._server_socket.close()


class TestSocketcandBusConnections(unittest.TestCase):
    def _run_bus_roundtrip(self, bus, expected_channel_info_part):
        try:
            self.assertIn(expected_channel_info_part, bus.channel_info)
            bus.send(
                can.Message(
                    arbitration_id=0x123,
                    data=[1, 2, 3, 4],
                    is_extended_id=False,
                    check=True,
                )
            )
            msg = bus.recv(timeout=5)
            self.assertIsNotNone(msg)
            self.assertEqual(msg.arbitration_id, 0x123)
            self.assertEqual(msg.data, bytearray([1, 2, 3, 4]))
            self.assertEqual(msg.channel, "can0")
        finally:
            bus.shutdown()

    def test_unix_socket_connection(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            socket_path = os.path.join(tmp_dir, "socketcand.sock")
            server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_socket.bind(socket_path)
            server_socket.listen(1)
            server = FakeSocketcandServer(server_socket)
            server.start()

            bus = SocketCanDaemonBus(channel="can0", unix_socket_path=socket_path)
            self._run_bus_roundtrip(bus, socket_path)
            server.join(timeout=5)
            self.assertIsNone(server.error)
            self.assertEqual(server.received[0], "< open can0 >")
            self.assertEqual(server.received[1], "< rawmode >")
            self.assertEqual(server.received[2], "< send 123 4 1 2 3 4 >")

    def test_tcp_connection_still_works(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", 0))
        port = server_socket.getsockname()[1]
        server_socket.listen(1)
        server = FakeSocketcandServer(server_socket)
        server.start()

        bus = SocketCanDaemonBus(channel="can0", host="127.0.0.1", port=port)
        self._run_bus_roundtrip(bus, f"127.0.0.1:{port}")
        server.join(timeout=5)
        self.assertIsNone(server.error)
        self.assertEqual(server.received[2], "< send 123 4 1 2 3 4 >")

    def test_unix_socket_and_tcp_address_are_mutually_exclusive(self):
        with self.assertRaises(can.CanInitializationError):
            SocketCanDaemonBus(
                channel="can0",
                host="127.0.0.1",
                port=29536,
                unix_socket_path="/run/socketcand.sock",
            )

    def test_missing_address_raises(self):
        with self.assertRaises(can.CanInitializationError):
            SocketCanDaemonBus(channel="can0")
        with self.assertRaises(can.CanInitializationError):
            SocketCanDaemonBus(channel="can0", host="127.0.0.1")


if __name__ == "__main__":
    unittest.main()
