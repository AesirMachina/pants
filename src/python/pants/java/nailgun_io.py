# coding=utf-8
# Copyright 2015 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import errno
import io
import os
import select
import socket
import threading
from contextlib import contextmanager

from pants.java.nailgun_protocol import ChunkType, NailgunProtocol


class NailgunStreamStdinReader(threading.Thread):
  """Reads Nailgun 'stdin' chunks on a socket and writes them to an output file-like.

  Because a Nailgun server only ever receives STDIN and STDIN_EOF ChunkTypes after initial
  setup, this thread executes all reading from a server socket.

  Runs until the socket is closed.
  """

  def __init__(self, sock):
    """
    :param socket sock: the socket to read nailgun protocol chunks from.
    """
    super(NailgunStreamStdinReader, self).__init__()
    self.daemon = True
    self._write_handle = None
    self._socket = sock

  @contextmanager
  def running(self):
    r_fd, w_fd = os.pipe()
    self._write_handle = os.fdopen(w_fd, 'w')
    self.start()
    try:
      yield os.fdopen(r_fd, 'r')
    finally:
      self._try_close()

  def _try_close(self):
    try:
      self._socket.close()
    except:
      pass

  def run(self):
    for chunk_type, payload in NailgunProtocol.iter_chunks(self._socket, return_bytes=True):
      # TODO: Read chunks. Expecting only STDIN, STDIN_EOF, and maybe EXIT.
      if chunk_type == ChunkType.STDIN:
        self._write_handle.write(payload)
      elif chunk_type == ChunkType.STDIN_EOF:
        self._write_handle.close()
      elif chunk_type == ChunkType.EXIT:
        break
      else:
        self._try_close()
        # TODO: Will kill the thread, but may not be handled in a useful way
        raise Exception('received unexpected chunk {} -> {}: closing.'.format(chunk_type, payload))


class NailgunStreamStdinWriter(threading.Thread):
  """Reads input from stdin and writes Nailgun 'stdin' chunks on a socket."""

  SELECT_TIMEOUT = 1

  def __init__(self, in_fd, sock, buf_size=io.DEFAULT_BUFFER_SIZE, select_timeout=SELECT_TIMEOUT):
    """
    :param file in_fd: the input file descriptor (e.g. sys.stdin) to read from.
    :param socket sock: the socket to emit nailgun protocol chunks over.
    :param int buf_size: the buffer size for reads from the file descriptor.
    :param int select_timeout: the timeout (in seconds) for select.select() calls against the fd.
    """
    super(NailgunStreamStdinWriter, self).__init__()
    self.daemon = True
    self._stdin = in_fd
    self._socket = sock
    self._buf_size = buf_size
    self._select_timeout = select_timeout
    # N.B. This Event is used as nothing more than a convenient atomic flag - nothing waits on it.
    self._stopped = threading.Event()

  @property
  def is_stopped(self):
    """Indicates whether or not the instance is stopped."""
    return self._stopped.is_set()

  def stop(self):
    """Stops the instance."""
    self._stopped.set()

  @contextmanager
  def running(self):
    self.start()
    yield
    self.stop()

  def run(self):
    while not self.is_stopped:
      readable, _, errored = select.select([self._stdin], [], [self._stdin], self._select_timeout)

      if self._stdin in errored:
        self.stop()
        return

      if not self.is_stopped and self._stdin in readable:
        data = os.read(self._stdin.fileno(), self._buf_size)

        if not self.is_stopped:
          if data:
            NailgunProtocol.write_chunk(self._socket, ChunkType.STDIN, data)
          else:
            NailgunProtocol.write_chunk(self._socket, ChunkType.STDIN_EOF)
            try:
              self._socket.shutdown(socket.SHUT_WR)  # Shutdown socket sends.
            except socket.error:  # Can happen if response is quick.
              pass
            finally:
              self.stop()


class NailgunStreamsWriter(object):
  """A sys.{stdout,stderr} replacement that writes output to a socket using the nailgun protocol."""

  def __init__(self, sock, chunk_type):
    """
    :param socket sock: A connected socket capable of speaking the nailgun protocol.
    :param str chunk_type: A ChunkType constant representing the nailgun protocol chunk type.
    """
    self._socket = sock
    self._chunk_type = chunk_type

  @contextmanager
  def running(self):
    r_fd, w_fd = os.pipe()
    self._read_handle = os.fdopen(r_fd, 'r')
    self.start()
    try:
      yield os.fdopen(w_fd, 'w')
    finally:
      self._try_close()

  def _try_close(self):
    try:
      self._socket.close()
    except:
      pass

  def run(self):
    while True:
      readable, _, errored = select.select([self._stdin], [], [self._stdin], self._select_timeout)

      if self._stdin in errored:
        self.stop()
        return

      if not self.is_stopped and self._stdin in readable:
        data = os.read(self._stdin.fileno(), self._buf_size)

        if not self.is_stopped:
          if data:
            NailgunProtocol.write_chunk(self._socket, ChunkType.STDIN, data)
          else:
            NailgunProtocol.write_chunk(self._socket, ChunkType.STDIN_EOF)
            try:
              self._socket.shutdown(socket.SHUT_WR)  # Shutdown socket sends.
            except socket.error:  # Can happen if response is quick.
              pass
            finally:
              self.stop()
