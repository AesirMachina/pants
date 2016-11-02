# coding=utf-8
# Copyright 2015 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os
import unittest
from contextlib import closing, contextmanager

from pants.build_graph.address import Address
from pants.engine.engine import LocalSerialEngine
from pants.engine.nodes import Return
from pants.engine.subsystem.native import Native
from pants_test.engine.examples.planners import Classpath, setup_json_scheduler
from pants_test.subsystem.subsystem_util import subsystem_instance


class EngineTest(unittest.TestCase):
  def setUp(self):
    build_root = os.path.join(os.path.dirname(__file__), 'examples', 'scheduler_inputs')
    with subsystem_instance(Native.Factory) as native_factory:
      self.scheduler = setup_json_scheduler(build_root, native_factory.create())

    self.java = Address.parse('src/java/simple')

  def request(self, goals, *addresses):
    return self.scheduler.build_request(goals=goals,
                                        subjects=addresses)

  def assert_engine(self, engine):
    result = engine.execute(self.request(['compile'], self.java))
    self.assertEqual([Return(Classpath(creator='javac'))], result.root_products.values())
    self.assertIsNone(result.error)

  @contextmanager
  def serial_engine(self):
    with closing(LocalSerialEngine(self.scheduler)) as e:
      yield e

  def test_serial_engine_simple(self):
    with self.serial_engine() as engine:
      self.assert_engine(engine)

  def test_product_request_return(self):
    with self.serial_engine() as engine:
      count = 0
      for computed_product in engine.product_request(Classpath, [self.java]):
        self.assertIsInstance(computed_product, Classpath)
        count += 1
      self.assertGreater(count, 0)
